import asyncio
import json
import sqlite3
import uuid
from collections.abc import AsyncIterator, Mapping
from io import StringIO
from pathlib import Path
from typing import cast

import pytest
from qoder_agent_sdk import (
    PermissionResultAllow,
    PermissionResultDeny,
    QoderAgentOptions,
    ToolPermissionContext,
)

from qworker.cli import run
from qworker.control import (
    ControlCallbacks,
    ElicitationDecision,
    ElicitationRequest,
    PermissionDecision,
    PermissionRequest,
    SteeringPriority,
)
from qworker.domain import AuditContract
from qworker.events import AdapterEvent, ResultEvent
from qworker.model_policy import AvailableModel
from qworker.qoder_sdk import QoderSDKTransport, build_auditor_options
from qworker.rpc import RPCServer
from qworker.store import EventRecord, JsonValue, WorkerStore
from qworker.supervisor import Supervisor, SupervisorError
from tests.fakes import SUCCESSFUL_AUDIT_REPORT, FakeQoderTransport


class ControlledFakeQoderTransport(FakeQoderTransport):
    """Live deterministic transport exposing top-level control operations."""

    def __init__(self) -> None:
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(
                ResultEvent(
                    session_id="session-control",
                    is_error=False,
                    result=SUCCESSFUL_AUDIT_REPORT,
                    model_usage=("Qwen3.8-Max",),
                ),
            ),
        )
        self.finish = asyncio.Event()
        self.callbacks: ControlCallbacks | None = None
        self.steering: list[tuple[str, SteeringPriority, str]] = []
        self.cancellation_results: list[bool] = []

    def bind_control(self, callbacks: ControlCallbacks) -> None:
        self.callbacks = callbacks

    async def steer(
        self,
        message: str,
        *,
        priority: SteeringPriority,
        message_id: str,
    ) -> None:
        self.steering.append((message, priority, message_id))

    async def cancel_message(self, message_id: str) -> bool:
        del message_id
        return self.cancellation_results.pop(0)

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        await self.finish.wait()
        async for event in super().messages():
            yield event


class PausingApprovalStore(WorkerStore):
    """Pause one approval append before its real SQLite transaction."""

    def __init__(self, state_dir: Path) -> None:
        super().__init__(state_dir)
        self.approval_append_started = asyncio.Event()
        self.release_approval_append = asyncio.Event()

    async def append_event(
        self,
        worker_id: str,
        event_type: str,
        payload: Mapping[str, JsonValue],
    ) -> EventRecord:
        if event_type == "approval.requested":
            self.approval_append_started.set()
            await self.release_approval_append.wait()
        return await super().append_event(worker_id, event_type, payload)

    async def record_approval_request(
        self,
        worker_id: str,
        *,
        expected_attempt: int,
        payload: Mapping[str, JsonValue],
    ) -> tuple[EventRecord, ...]:
        self.approval_append_started.set()
        await self.release_approval_append.wait()
        return await super().record_approval_request(
            worker_id,
            expected_attempt=expected_attempt,
            payload=payload,
        )


async def _spawn_running(
    tmp_path: Path,
    *,
    store: WorkerStore | None = None,
) -> tuple[Supervisor, ControlledFakeQoderTransport, str, WorkerStore]:
    transport = ControlledFakeQoderTransport()
    worker_store = store or WorkerStore(tmp_path / "state")
    supervisor = Supervisor(
        worker_store, lambda _cwd: transport, sdk_version="1.0.13"
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="audit live controls", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "running":
            return supervisor, transport, worker_id, worker_store
        await asyncio.sleep(0)
    raise AssertionError("worker did not initialize")


async def _request_permission(
    transport: ControlledFakeQoderTransport,
) -> asyncio.Task[PermissionDecision]:
    callbacks = transport.callbacks
    assert callbacks is not None
    return asyncio.create_task(
        callbacks.request_permission(
            PermissionRequest(tool_name="Read", agent_id=None, display_message="safe")
        )
    )


async def test_completion_during_approval_creation_leaves_no_orphan(
    tmp_path: Path,
) -> None:
    store = PausingApprovalStore(tmp_path / "state")
    supervisor, transport, worker_id, _store = await _spawn_running(
        tmp_path, store=store
    )
    permission = await _request_permission(transport)
    await store.approval_append_started.wait()

    transport.finish.set()
    for _ in range(20):
        if transport.disconnected:
            break
        await asyncio.sleep(0)
    store.release_approval_append.set()

    assert (await permission).action == "deny"
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "completed":
            break
        await asyncio.sleep(0)
    assert (await supervisor.status(worker_id))["state"] == "completed"
    approval_events = [
        event
        for event in await store.events_since(worker_id)
        if event.type.startswith("approval.")
    ]
    assert approval_events == [] or [
        (event.type, event.payload.get("status")) for event in approval_events
    ] == [
        ("approval.requested", None),
        ("approval.resolved", "expired"),
    ]
    assert (await supervisor.status(worker_id))["pending_approvals"] == []
    await supervisor.close()


@pytest.mark.parametrize("resume", (False, True), ids=("loss", "loss-resume"))
async def test_loss_or_resume_during_approval_creation_persists_no_request(
    tmp_path: Path,
    resume: bool,
) -> None:
    store = PausingApprovalStore(tmp_path / "state")
    supervisor, transport, worker_id, _store = await _spawn_running(
        tmp_path, store=store
    )
    permission = await _request_permission(transport)
    await store.approval_append_started.wait()
    await store.append_event(
        worker_id,
        "worker.state_changed",
        {"schema_version": 1, "state": "lost"},
    )
    if resume:
        assert (await store.start_attempt(worker_id)).attempt == 2

    store.release_approval_append.set()

    assert (await permission).action == "deny"
    assert not any(
        event.type.startswith("approval.")
        for event in await store.events_since(worker_id)
    )
    assert (await supervisor.status(worker_id))["pending_approvals"] == []
    await supervisor.close()


@pytest.mark.parametrize("cancelled", (True, False))
async def test_each_priority_uses_stable_uuid_and_preserves_cancellation_result(
    tmp_path: Path,
    cancelled: bool,
) -> None:
    supervisor, transport, worker_id, store = await _spawn_running(tmp_path)
    secret_messages = {
        "now": "urgent prompt with password=opaque-now",
        "next": "boundary prompt with password=opaque-next",
        "later": "idle prompt with password=opaque-later",
    }

    responses = []
    for priority, message in secret_messages.items():
        responses.append(
            await supervisor.steer(
                worker_id,
                message,
                priority=cast(SteeringPriority, priority),
            )
        )

    message_ids = [str(response["message_id"]) for response in responses]
    assert [str(uuid.UUID(message_id)) for message_id in message_ids] == message_ids
    assert responses == [
        {"message_id": message_ids[0], "priority": "now", "accepted": True},
        {"message_id": message_ids[1], "priority": "next", "accepted": True},
        {"message_id": message_ids[2], "priority": "later", "accepted": True},
    ]
    assert transport.steering == [
        (secret_messages["now"], "now", message_ids[0]),
        (secret_messages["next"], "next", message_ids[1]),
        (secret_messages["later"], "later", message_ids[2]),
    ]

    transport.cancellation_results.append(cancelled)
    cancellation = await supervisor.cancel_message(worker_id, message_ids[2])
    assert cancellation == {"message_id": message_ids[2], "cancelled": cancelled}

    events = await store.events_since(worker_id)
    assert [event.type for event in events[4:]] == [
        "steer.queued",
        "steer.delivered",
        "steer.queued",
        "steer.delivered",
        "steer.queued",
        "steer.delivered",
        "steer.cancelled",
    ]
    assert [events[index].payload["message_id"] for index in (4, 6, 8)] == (
        message_ids
    )
    assert [events[index].payload["priority"] for index in (4, 6, 8)] == [
        "now",
        "next",
        "later",
    ]
    assert events[10].payload["cancelled"] is cancelled
    with sqlite3.connect(store.database_path) as connection:
        durable = json.dumps(connection.execute("SELECT * FROM events").fetchall())
    assert all(message not in durable for message in secret_messages.values())

    transport.finish.set()
    await supervisor.close()


async def _latest_request_id(store: WorkerStore, worker_id: str) -> str:
    requests = [
        event
        for event in await store.events_since(worker_id)
        if event.type == "approval.requested"
    ]
    request_id = requests[-1].payload["request_id"]
    assert isinstance(request_id, str)
    return request_id


async def test_permission_and_elicitation_callbacks_pause_and_resume_worker(
    tmp_path: Path,
) -> None:
    supervisor, transport, worker_id, store = await _spawn_running(tmp_path)
    callbacks = transport.callbacks
    assert callbacks is not None
    persisted_secret = "password=approval-secret"

    permission = asyncio.create_task(
        callbacks.request_permission(
            PermissionRequest(
                tool_name="Read",
                agent_id="nested-1",
                display_message=persisted_secret,
            )
        )
    )
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("permission callback did not pause worker")
    permission_request_id = await _latest_request_id(store, worker_id)

    with pytest.raises(SupervisorError) as invalid_response:
        await supervisor.respond(
            worker_id,
            permission_request_id,
            {"action": "accept"},
        )
    assert invalid_response.value.code == "invalid_request"
    assert permission.done() is False
    assert (await supervisor.status(worker_id))["state"] == "requires_action"

    assert await supervisor.respond(
        worker_id,
        permission_request_id,
        {"action": "allow"},
    ) == {"request_id": permission_request_id, "status": "allowed"}
    assert (await permission).action == "allow"
    assert (await supervisor.status(worker_id))["state"] == "running"

    elicitation = asyncio.create_task(
        callbacks.request_elicitation(
            ElicitationRequest(
                server_name="safe-server",
                mode="form",
                display_message=f"form asks for {persisted_secret}",
            )
        )
    )
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("elicitation callback did not pause worker")
    elicitation_request_id = await _latest_request_id(store, worker_id)
    response_secret = "opaque-response-secret"

    assert await supervisor.respond(
        worker_id,
        elicitation_request_id,
        {"action": "accept", "content": {"answer": response_secret}},
    ) == {"request_id": elicitation_request_id, "status": "answered"}
    elicitation_decision = await elicitation
    assert elicitation_decision.action == "accept"
    assert elicitation_decision.content == {"answer": response_secret}
    assert (await supervisor.status(worker_id))["state"] == "running"

    events = await store.events_since(worker_id)
    control_events = [event for event in events if event.type.startswith("approval.")]
    assert [(event.type, event.payload["kind"] if event.type.endswith("requested") else event.payload["status"]) for event in control_events] == [
        ("approval.requested", "tool_permission"),
        ("approval.resolved", "allowed"),
        ("approval.requested", "elicitation"),
        ("approval.resolved", "answered"),
    ]
    with sqlite3.connect(store.database_path) as connection:
        durable = json.dumps(connection.execute("SELECT * FROM events").fetchall())
    assert persisted_secret not in durable
    assert response_secret not in durable

    transport.finish.set()
    await supervisor.close()


async def test_pending_status_and_events_expose_only_safe_approval_display(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment_secret = "opaque-environment-display-secret"
    keyed_secret = "opaque-keyed-display-secret"
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", environment_secret)
    supervisor, transport, worker_id, store = await _spawn_running(tmp_path)
    callbacks = transport.callbacks
    assert callbacks is not None

    permission = asyncio.create_task(
        callbacks.request_permission(
            PermissionRequest(
                tool_name="Read",
                agent_id="nested-safe",
                display_message=f"raw {environment_secret} password={keyed_secret}",
            )
        )
    )
    for _ in range(20):
        status = await supervisor.status(worker_id)
        if status["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    pending = status["pending_approvals"]
    assert isinstance(pending, list)
    assert len(pending) == 1
    permission_display = pending[0]
    assert isinstance(permission_display, dict)
    permission_request_id = permission_display["request_id"]
    assert permission_display == {
        "request_id": permission_request_id,
        "attempt": 1,
        "kind": "tool_permission",
        "agent_id": "nested-safe",
        "tool_name": "Read",
        "prompt": "Allow tool Read for this turn?",
        "choices": ["allow", "deny"],
    }
    requested = [
        event
        for event in await store.events_since(worker_id)
        if event.type == "approval.requested"
    ][-1]
    assert requested.payload == {
        "schema_version": 1,
        **{
            key: value
            for key, value in permission_display.items()
            if key != "attempt"
        },
    }
    assert isinstance(permission_request_id, str)
    await supervisor.respond(
        worker_id,
        permission_request_id,
        {"action": "deny"},
    )
    assert (await permission).action == "deny"

    elicitation = asyncio.create_task(
        callbacks.request_elicitation(
            ElicitationRequest(
                server_name="safe-server",
                mode="form",
                display_message=(
                    f"Choose region; token={keyed_secret}; {environment_secret}; "
                    + "x" * 600
                ),
                requested_schema={
                    "type": "object",
                    "properties": {
                        "region": {"type": "string"},
                        "count": {"type": "integer"},
                    },
                    "required": ["region"],
                    "additionalProperties": False,
                },
            )
        )
    )
    for _ in range(20):
        status = await supervisor.status(worker_id)
        if status["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    pending = status["pending_approvals"]
    assert isinstance(pending, list)
    assert len(pending) == 1
    elicitation_display = pending[0]
    assert isinstance(elicitation_display, dict)
    elicitation_request_id = elicitation_display["request_id"]
    assert elicitation_display["kind"] == "elicitation"
    assert elicitation_display["server_name"] == "safe-server"
    assert elicitation_display["mode"] == "form"
    assert elicitation_display["choices"] == ["accept", "decline", "cancel"]
    assert elicitation_display["fields"] == ["count:integer", "region:string"]
    assert elicitation_display["required_fields"] == ["region"]
    prompt = elicitation_display["prompt"]
    assert isinstance(prompt, str)
    assert 0 < len(prompt) <= 256
    assert "[REDACTED]" in prompt
    assert environment_secret not in prompt
    assert keyed_secret not in prompt
    elicitation_event = [
        event
        for event in await store.events_since(worker_id)
        if event.type == "approval.requested"
    ][-1]
    assert elicitation_event.payload == {
        "schema_version": 1,
        **{
            key: value
            for key, value in elicitation_display.items()
            if key != "attempt"
        },
    }

    with sqlite3.connect(store.database_path) as connection:
        durable = json.dumps(connection.execute("SELECT * FROM events").fetchall())
    assert environment_secret not in durable
    assert keyed_secret not in durable
    assert "additionalProperties" not in durable

    assert isinstance(elicitation_request_id, str)
    await supervisor.respond(
        worker_id,
        elicitation_request_id,
        {"action": "decline"},
    )
    assert (await elicitation).action == "decline"
    transport.finish.set()
    await supervisor.close()


async def test_schema_display_never_persists_sensitive_property_names(
    tmp_path: Path,
) -> None:
    supervisor, transport, worker_id, store = await _spawn_running(tmp_path)
    callbacks = transport.callbacks
    assert callbacks is not None
    aws_shaped_name = "AKIA1234567890ABCDEF"
    jwt_shaped_name = "abcd.efgh.ijkl"
    marker_name = "password"
    token_name = "token"
    refresh_token_name = "refresh_token"
    elicitation = asyncio.create_task(
        callbacks.request_elicitation(
            ElicitationRequest(
                server_name="safe-server",
                mode="form",
                display_message="Choose a region.",
                requested_schema={
                    "type": "object",
                    "properties": {
                        "region": {"type": "string"},
                        aws_shaped_name: {"type": "string"},
                        jwt_shaped_name: {"type": "string"},
                        marker_name: {"type": "string"},
                        token_name: {"type": "string"},
                        refresh_token_name: {"type": "string"},
                    },
                },
            )
        )
    )
    for _ in range(20):
        status = await supervisor.status(worker_id)
        if status["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    pending = status["pending_approvals"]
    assert isinstance(pending, list)
    assert len(pending) == 1
    assert isinstance(pending[0], dict)
    assert pending[0]["fields"] == ["region:string"]
    request_id = pending[0]["request_id"]
    assert isinstance(request_id, str)

    requested = [
        event
        for event in await store.events_since(worker_id)
        if event.type == "approval.requested"
    ][-1]
    assert requested.payload["fields"] == ["region:string"]
    with sqlite3.connect(store.database_path) as connection:
        durable = json.dumps(connection.execute("SELECT * FROM events").fetchall())
    for sensitive_name in (
        aws_shaped_name,
        jwt_shaped_name,
        marker_name,
        token_name,
        refresh_token_name,
    ):
        assert sensitive_name not in durable

    await supervisor.respond(worker_id, request_id, {"action": "cancel"})
    assert (await elicitation).action == "cancel"
    transport.finish.set()
    await supervisor.close()


async def test_elicitation_response_must_match_live_schema_and_mode(
    tmp_path: Path,
) -> None:
    supervisor, transport, worker_id, store = await _spawn_running(tmp_path)
    callbacks = transport.callbacks
    assert callbacks is not None
    form = asyncio.create_task(
        callbacks.request_elicitation(
            ElicitationRequest(
                server_name="safe-server",
                mode="form",
                display_message="Choose a region and retry count.",
                requested_schema={
                    "type": "object",
                    "properties": {
                        "region": {"type": "string", "minLength": 2},
                        "count": {"type": "integer", "minimum": 1},
                    },
                    "required": ["region"],
                    "additionalProperties": False,
                },
            )
        )
    )
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    form_request_id = await _latest_request_id(store, worker_id)

    invalid_responses: tuple[dict[str, JsonValue], ...] = (
        {"action": "accept"},
        {"action": "accept", "content": {"count": 2}},
        {"action": "accept", "content": {"region": "cn", "count": "two"}},
        {
            "action": "accept",
            "content": {"region": "cn", "count": 2, "secret": "not allowed"},
        },
    )
    for response in invalid_responses:
        with pytest.raises(SupervisorError) as invalid:
            await supervisor.respond(worker_id, form_request_id, response)
        assert invalid.value.code == "invalid_request"
        assert form.done() is False
        assert (await supervisor.status(worker_id))["state"] == "requires_action"

    assert await supervisor.respond(
        worker_id,
        form_request_id,
        {"action": "accept", "content": {"region": "cn", "count": 2}},
    ) == {"request_id": form_request_id, "status": "answered"}
    assert (await form).content == {"region": "cn", "count": 2}

    url = asyncio.create_task(
        callbacks.request_elicitation(
            ElicitationRequest(
                server_name="safe-server",
                mode="url",
                display_message="Authorize access in the browser.",
            )
        )
    )
    for _ in range(20):
        url_status = await supervisor.status(worker_id)
        if url_status["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    url_pending = url_status["pending_approvals"]
    assert isinstance(url_pending, list)
    assert len(url_pending) == 1
    assert isinstance(url_pending[0], dict)
    assert url_pending[0]["mode"] == "url"
    assert url_pending[0]["choices"] == ["accept", "decline", "cancel"]
    assert "fields" not in url_pending[0]
    assert "required_fields" not in url_pending[0]
    url_request_id = await _latest_request_id(store, worker_id)

    with pytest.raises(SupervisorError) as invalid_url:
        await supervisor.respond(
            worker_id,
            url_request_id,
            {"action": "accept", "content": {}},
        )
    assert invalid_url.value.code == "invalid_request"
    assert url.done() is False
    assert await supervisor.respond(
        worker_id,
        url_request_id,
        {"action": "accept"},
    ) == {"request_id": url_request_id, "status": "answered"}
    assert (await url) == ElicitationDecision("accept")

    resolved = [
        event
        for event in await store.events_since(worker_id)
        if event.type == "approval.resolved"
    ]
    assert [event.payload["status"] for event in resolved] == [
        "answered",
        "answered",
    ]
    with sqlite3.connect(store.database_path) as connection:
        durable = json.dumps(connection.execute("SELECT * FROM events").fetchall())
    assert "additionalProperties" not in durable
    assert "minLength" not in durable
    assert "minimum" not in durable

    transport.finish.set()
    await supervisor.close()


async def test_elicitation_validation_uses_ingress_schema_snapshot(
    tmp_path: Path,
) -> None:
    supervisor, transport, worker_id, store = await _spawn_running(tmp_path)
    callbacks = transport.callbacks
    assert callbacks is not None
    source_schema: dict[str, object] = {
        "type": "object",
        "properties": {"region": {"type": "string"}},
        "required": ["region"],
        "additionalProperties": False,
    }
    elicitation = asyncio.create_task(
        callbacks.request_elicitation(
            ElicitationRequest(
                server_name="safe-server",
                mode="form",
                display_message="Choose a region.",
                requested_schema=source_schema,
            )
        )
    )
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    request_id = await _latest_request_id(store, worker_id)

    source_schema.clear()
    with pytest.raises(SupervisorError) as invalid:
        await supervisor.respond(
            worker_id,
            request_id,
            {"action": "accept", "content": {"unexpected": 42}},
        )
    assert invalid.value.code == "invalid_request"
    assert elicitation.done() is False
    assert (await supervisor.status(worker_id))["state"] == "requires_action"

    await supervisor.respond(
        worker_id,
        request_id,
        {"action": "accept", "content": {"region": "cn"}},
    )
    assert (await elicitation).content == {"region": "cn"}
    with sqlite3.connect(store.database_path) as connection:
        durable = json.dumps(connection.execute("SELECT * FROM events").fetchall())
    assert "additionalProperties" not in durable
    assert "unexpected" not in durable

    transport.finish.set()
    await supervisor.close()


async def test_response_and_callbacks_reject_a_lost_attempt(
    tmp_path: Path,
) -> None:
    supervisor, transport, worker_id, store = await _spawn_running(tmp_path)
    callbacks = transport.callbacks
    assert callbacks is not None
    pending = asyncio.create_task(
        callbacks.request_permission(
            PermissionRequest(tool_name="Read", agent_id=None, display_message="safe")
        )
    )
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    request_id = await _latest_request_id(store, worker_id)
    requested = [
        event
        for event in await store.events_since(worker_id)
        if event.type == "approval.requested"
    ][-1]
    assert requested.attempt == 1
    await store.append_event(
        worker_id,
        "worker.state_changed",
        {"schema_version": 1, "state": "lost"},
    )
    resumed = await store.start_attempt(worker_id)
    assert resumed.attempt == 2

    with pytest.raises(SupervisorError) as captured:
        await supervisor.respond(worker_id, request_id, {"action": "allow"})
    assert captured.value.code == "worker_not_live"
    assert pending.done() is False

    late = await callbacks.request_permission(
        PermissionRequest(tool_name="Read", agent_id=None, display_message="late")
    )
    assert late.action == "deny"
    assert len(
        [
            event
            for event in await store.events_since(worker_id)
            if event.type == "approval.requested"
        ]
    ) == 1

    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await supervisor.close()


class ControlFakeSDKClient:
    def __init__(self, options: QoderAgentOptions) -> None:
        self.options = options
        self.steering: list[tuple[str, SteeringPriority, str | None]] = []
        self.cancelled: list[str] = []

    async def query(
        self,
        prompt: str,
        session_id: str = "default",
        *,
        priority: SteeringPriority = "next",
        message_uuid: str | None = None,
        should_query: bool = True,
    ) -> None:
        del session_id, should_query
        self.steering.append((prompt, priority, message_uuid))

    async def cancel_async_message(self, message_uuid: str) -> bool:
        self.cancelled.append(message_uuid)
        return False


async def test_sdk_adapter_bridges_callbacks_without_weakening_auditor_policy(
    tmp_path: Path,
) -> None:
    options = build_auditor_options(tmp_path)
    client = ControlFakeSDKClient(options)
    transport = QoderSDKTransport(client)
    permission_requests: list[PermissionRequest] = []
    elicitation_requests: list[ElicitationRequest] = []

    async def request_permission(
        request: PermissionRequest,
    ) -> PermissionDecision:
        permission_requests.append(request)
        return PermissionDecision("allow")

    async def request_elicitation(
        request: ElicitationRequest,
    ) -> ElicitationDecision:
        elicitation_requests.append(request)
        return ElicitationDecision("accept", {"answer": "live-only"})

    transport.bind_control(
        ControlCallbacks(
            request_permission=request_permission,
            request_elicitation=request_elicitation,
        )
    )
    assert options.can_use_tool is not None
    denied = await options.can_use_tool(
        "Write",
        {"file_path": "secret-tool-argument"},
        ToolPermissionContext(agent_id="nested-unsafe"),
    )
    allowed_input = {"file_path": "safe-read-target"}
    allowed = await options.can_use_tool(
        "Read",
        allowed_input,
        ToolPermissionContext(agent_id="nested-safe", title="Inspect file"),
    )
    assert options.on_elicitation is not None
    elicited = await options.on_elicitation(
        {
            "serverName": "safe-server",
            "message": "secret live prompt",
            "mode": "form",
            "requestedSchema": {"type": "object"},
        }
    )

    assert isinstance(denied, PermissionResultDeny)
    assert isinstance(allowed, PermissionResultAllow)
    assert allowed.updated_input is allowed_input
    assert [request.tool_name for request in permission_requests] == ["Read"]
    assert permission_requests[0].agent_id == "nested-safe"
    assert elicited == {"action": "accept", "content": {"answer": "live-only"}}
    assert elicitation_requests[0].display_message == "secret live prompt"
    assert elicitation_requests[0].mode == "form"
    assert elicitation_requests[0].requested_schema == {"type": "object"}

    message_id = str(uuid.uuid4())
    await transport.steer("live steering prompt", priority="now", message_id=message_id)
    assert await transport.cancel_message(message_id) is False
    assert client.steering == [("live steering prompt", "now", message_id)]
    assert client.cancelled == [message_id]


async def test_cli_controls_live_worker_without_putting_bodies_in_argv(
    tmp_path: Path,
) -> None:
    supervisor, transport, worker_id, store = await _spawn_running(tmp_path)
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    steering_secret = "steer from stdin with password=opaque-cli"
    stdout = StringIO()

    exit_code = await run(
        [
            "--socket",
            str(socket_path),
            "steer",
            worker_id,
            "--priority",
            "later",
            "--json",
        ],
        stdin=StringIO(steering_secret),
        stdout=stdout,
    )

    steered = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert steered["priority"] == "later"
    assert steered["accepted"] is True
    assert transport.steering == [
        (steering_secret, "later", steered["message_id"])
    ]

    transport.cancellation_results.append(False)
    stdout = StringIO()
    exit_code = await run(
        [
            "--socket",
            str(socket_path),
            "cancel-message",
            worker_id,
            steered["message_id"],
            "--json",
        ],
        stdin=StringIO(),
        stdout=stdout,
    )
    assert exit_code == 0
    assert json.loads(stdout.getvalue()) == {
        "message_id": steered["message_id"],
        "cancelled": False,
    }

    callbacks = transport.callbacks
    assert callbacks is not None
    pending = asyncio.create_task(
        callbacks.request_permission(
            PermissionRequest(tool_name="Read", agent_id=None, display_message="safe")
        )
    )
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)
    request_id = await _latest_request_id(store, worker_id)
    stdout = StringIO()
    exit_code = await run(
        [
            "--socket",
            str(socket_path),
            "respond",
            worker_id,
            request_id,
            "--json",
        ],
        stdin=StringIO('{"action":"deny"}'),
        stdout=stdout,
    )
    assert exit_code == 0
    assert json.loads(stdout.getvalue()) == {
        "request_id": request_id,
        "status": "denied",
    }
    assert (await pending).action == "deny"

    stdout = StringIO()
    exit_code = await run(
        [
            "--socket",
            str(socket_path),
            "steer",
            worker_id,
            "--agent-id",
            "nested-unsupported",
            "--json",
        ],
        stdin=StringIO("do not deliver"),
        stdout=stdout,
    )
    assert exit_code == 1
    assert json.loads(stdout.getvalue())["error"]["code"] == "unsupported_operation"
    assert len(transport.steering) == 1

    with sqlite3.connect(store.database_path) as connection:
        durable = json.dumps(connection.execute("SELECT * FROM events").fetchall())
    assert steering_secret not in durable

    transport.finish.set()
    await server.close()
    await supervisor.close()
