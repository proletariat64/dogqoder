import asyncio
import json
import sqlite3
import uuid
from collections.abc import AsyncIterator
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
from qworker.store import WorkerStore
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


async def _spawn_running(
    tmp_path: Path,
) -> tuple[Supervisor, ControlledFakeQoderTransport, str, WorkerStore]:
    transport = ControlledFakeQoderTransport()
    store = WorkerStore(tmp_path / "state")
    supervisor = Supervisor(store, lambda _cwd: transport, sdk_version="1.0.13")
    accepted = await supervisor.spawn(
        AuditContract(objective="audit live controls", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])
    for _ in range(20):
        if (await supervisor.status(worker_id))["state"] == "running":
            return supervisor, transport, worker_id, store
        await asyncio.sleep(0)
    raise AssertionError("worker did not initialize")


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
