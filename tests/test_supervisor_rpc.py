import asyncio
import json
import os
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import cast

import pytest

from qworker.domain import AuditContract
from qworker.events import AdapterEvent, ResultEvent
from qworker.model_policy import AvailableModel
from qworker.qoder_sdk import AdapterDiagnostic
from qworker.rpc import RPCClientError, RPCServer, call
from qworker.store import JsonValue, WorkerStore
from qworker.supervisor import Supervisor
from tests.fakes import SUCCESSFUL_AUDIT_REPORT, FakeQoderTransport


class GatedFakeQoderTransport(FakeQoderTransport):
    """Fake transport whose result remains pending until a test releases it."""

    def __init__(self, gate: asyncio.Event, *, session_id: str) -> None:
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(
                ResultEvent(
                    session_id=session_id,
                    is_error=False,
                    result=SUCCESSFUL_AUDIT_REPORT,
                    model_usage=("Qwen3.8-Max",),
                ),
            ),
        )
        self._gate = gate

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        await self._gate.wait()
        async for event in super().messages():
            yield event


async def test_spawn_returns_stable_id_before_durable_completion(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    gate = asyncio.Event()
    transport = GatedFakeQoderTransport(gate, session_id="session-1")
    supervisor = Supervisor(
        WorkerStore(state_dir),
        lambda _: transport,
        sdk_version="1.0.13",
    )

    accepted = await supervisor.spawn(
        AuditContract(objective="audit task six", cwd=tmp_path)
    )
    worker_id = accepted["worker_id"]
    assert isinstance(worker_id, str)

    assert accepted == {
        "worker_id": worker_id,
        "state": "starting",
        "role": "auditor",
        "cwd": str(tmp_path),
        "event_cursor": 1,
    }
    assert (await supervisor.status(worker_id))["state"] == "starting"
    assert await supervisor.result(worker_id) is None

    gate.set()
    terminal = [
        event async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]
    result = await supervisor.result(worker_id)

    assert [event["sequence"] for event in terminal] == [1, 2, 3, 4, 5, 6, 7]
    assert [event["type"] for event in terminal] == [
        "worker.created",
        "model.resolved",
        "worker.state_changed",
        "worker.health_changed",
        "result.received",
        "worker.health_changed",
        "worker.state_changed",
    ]
    assert result is not None
    assert result["outcome"] == "completed"
    assert result["summary"] == "safe"
    assert result["session_id"] == "session-1"
    assert result["nested_state"] == "settled"
    assert (await supervisor.status(worker_id))["health"] == "exited"

    await supervisor.close()
    reopened = Supervisor(
        WorkerStore(state_dir),
        lambda _: transport,
        sdk_version="1.0.13",
    )
    assert await reopened.result(worker_id) == result
    assert (await reopened.status(worker_id))["state"] == "completed"
    await reopened.close()


async def test_rpc_constructs_coder_contract_and_rejects_missing_cwd(
    tmp_path: Path,
) -> None:
    gate = asyncio.Event()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: GatedFakeQoderTransport(
            asyncio.Event(), session_id="unexpected-auditor"
        ),
        coder_transport_factory=lambda _: GatedFakeQoderTransport(
            gate, session_id="coder-rpc"
        ),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    reader, writer = await asyncio.open_unix_connection(socket_path)

    await _write_request(
        writer,
        {
            "request_id": "spawn-coder",
            "method": "spawn",
            "params": {
                "role": "coder",
                "cwd": str(tmp_path),
                "objective": "implement through rpc",
            },
        },
    )
    response = await _read_response(reader)
    assert response["ok"] is True
    accepted = response["result"]
    assert isinstance(accepted, dict)
    assert accepted["role"] == "coder"
    status = await supervisor.status(str(accepted["worker_id"]))
    assert status["requested_model"] == "qwen-coder"
    assert status["write_capability"] == "shared_workspace"

    await _write_request(
        writer,
        {
            "request_id": "spawn-coder-missing-cwd",
            "method": "spawn",
            "params": {
                "role": "coder",
                "cwd": str(tmp_path / "missing"),
                "objective": "must reject",
            },
        },
    )
    rejected = await _read_response(reader)
    assert rejected["ok"] is False
    error = rejected["error"]
    assert isinstance(error, dict)
    assert error["code"] == "invalid_request"

    writer.close()
    await writer.wait_closed()
    await server.close()
    await supervisor.close()


async def _write_request(
    writer: asyncio.StreamWriter, request: dict[str, object]
) -> None:
    writer.write(json.dumps(request).encode() + b"\n")
    await writer.drain()


async def _read_response(reader: asyncio.StreamReader) -> dict[str, object]:
    line = await asyncio.wait_for(reader.readline(), timeout=1)
    assert line
    response = json.loads(line)
    assert isinstance(response, dict)
    return response


async def test_rpc_correlates_finite_requests_and_streams_ordered_watch(
    tmp_path: Path,
) -> None:
    gate = asyncio.Event()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: GatedFakeQoderTransport(gate, session_id="session-rpc"),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()

    reader, writer = await asyncio.open_unix_connection(socket_path)
    await _write_request(
        writer,
        {
            "request_id": "request-spawn",
            "method": "spawn",
            "params": {
                "role": "auditor",
                "cwd": str(tmp_path),
                "model": "qwen-auditor",
                "objective": "audit rpc",
            },
        },
    )
    spawn_response = await _read_response(reader)
    assert spawn_response["request_id"] == "request-spawn"
    assert spawn_response["ok"] is True
    accepted = spawn_response["result"]
    assert isinstance(accepted, dict)
    worker_id = accepted["worker_id"]
    assert isinstance(worker_id, str)

    await _write_request(
        writer,
        {
            "request_id": "request-status",
            "method": "status",
            "params": {"worker_id": worker_id},
        },
    )
    status_response = await _read_response(reader)
    assert status_response["request_id"] == "request-status"
    assert status_response["ok"] is True
    writer.close()
    await writer.wait_closed()

    await asyncio.sleep(0)
    watch_reader, watch_writer = await asyncio.open_unix_connection(socket_path)
    await _write_request(
        watch_writer,
        {
            "request_id": "request-watch",
            "method": "watch",
            "params": {"worker_id": worker_id, "since": 3, "follow": True},
        },
    )
    initial = await _read_response(watch_reader)
    assert initial == {
        "request_id": "request-watch",
        "ok": True,
        "result": {"worker_id": worker_id, "since": 3, "follow": True},
    }

    gate.set()
    streamed = [await _read_response(watch_reader) for _ in range(5)]
    assert [response["request_id"] for response in streamed] == [
        "request-watch",
        "request-watch",
        "request-watch",
        "request-watch",
        "request-watch",
    ]
    assert [response["result"]["event"]["sequence"] for response in streamed[:4]] == [  # type: ignore[index]
        4,
        5,
        6,
        7,
    ]
    assert streamed[4]["result"] == {
        "end": {"reason": "terminal", "cursor": 7, "state": "completed"}
    }
    assert await asyncio.wait_for(watch_reader.readline(), timeout=1) == b""
    watch_writer.close()
    await watch_writer.wait_closed()

    if os.name == "posix":
        assert socket_path.parent.stat().st_mode & 0o777 == 0o700
        assert socket_path.stat().st_mode & 0o777 == 0o600

    await server.close()
    await supervisor.close()


async def test_rpc_refuses_to_replace_an_active_supervisor_socket(
    tmp_path: Path,
) -> None:
    gate = asyncio.Event()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: GatedFakeQoderTransport(gate, session_id="session-owner"),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    owner = RPCServer(supervisor, socket_path)
    contender = RPCServer(supervisor, socket_path)
    await owner.start()

    with pytest.raises(RuntimeError, match="already active"):
        await contender.start()

    reader, writer = await asyncio.open_unix_connection(socket_path)
    await _write_request(
        writer,
        {
            "request_id": "owner-check",
            "method": "status",
            "params": {"worker_id": "missing-worker"},
        },
    )
    response = await _read_response(reader)
    assert response["request_id"] == "owner-check"
    error = response["error"]
    assert isinstance(error, dict)
    assert error["code"] == "worker_not_found"
    writer.close()
    await writer.wait_closed()

    await owner.close()
    await supervisor.close()


async def test_two_workers_follow_independent_reconnecting_cursors(
    tmp_path: Path,
) -> None:
    first_gate = asyncio.Event()
    second_gate = asyncio.Event()
    transports = iter(
        (
            GatedFakeQoderTransport(first_gate, session_id="session-1"),
            GatedFakeQoderTransport(second_gate, session_id="session-2"),
        )
    )
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: next(transports),
        sdk_version="1.0.13",
    )
    first = await supervisor.spawn(
        AuditContract(objective="audit first worker", cwd=tmp_path)
    )
    second = await supervisor.spawn(
        AuditContract(objective="audit second worker", cwd=tmp_path)
    )
    await asyncio.sleep(0)

    first_replay = [
        event
        async for event in supervisor.watch(
            str(first["worker_id"]), since=0, follow=False
        )
    ]
    second_replay = [
        event
        async for event in supervisor.watch(
            str(second["worker_id"]), since=0, follow=False
        )
    ]
    assert [event["sequence"] for event in first_replay] == [1, 2, 3, 4]
    assert [event["sequence"] for event in second_replay] == [1, 2, 3, 4]

    async def reconnect(worker_id: str) -> list[dict[str, JsonValue]]:
        return [
            event async for event in supervisor.watch(worker_id, since=4, follow=True)
        ]

    first_watch = asyncio.create_task(reconnect(str(first["worker_id"])))
    second_watch = asyncio.create_task(reconnect(str(second["worker_id"])))
    await asyncio.sleep(0)
    first_gate.set()
    second_gate.set()
    first_tail, second_tail = await asyncio.gather(first_watch, second_watch)

    assert [event["sequence"] for event in first_tail] == [5, 6, 7]
    assert [event["sequence"] for event in second_tail] == [5, 6, 7]
    assert {event["worker_id"] for event in first_tail} == {first["worker_id"]}
    assert {event["worker_id"] for event in second_tail} == {second["worker_id"]}
    first_result = await supervisor.result(str(first["worker_id"]))
    second_result = await supervisor.result(str(second["worker_id"]))
    assert first_result is not None
    assert second_result is not None
    assert first_result["session_id"] == "session-1"
    assert second_result["session_id"] == "session-2"

    await supervisor.close()


async def test_preinitialization_failure_stays_starting_until_failed(
    tmp_path: Path,
) -> None:
    def failing_factory(_cwd: Path) -> FakeQoderTransport:
        raise AdapterDiagnostic("auth_required", "Authentication is required.")

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        failing_factory,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="audit initialization", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])

    events = [
        event
        async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]
    status = await supervisor.status(worker_id)

    state_changes: list[JsonValue] = []
    for event in events:
        if event["type"] == "worker.state_changed":
            payload = event["payload"]
            assert isinstance(payload, dict)
            state_changes.append(payload["state"])
    assert state_changes == ["failed"]
    assert not any(event["type"] == "worker.health_changed" for event in events)
    assert status["state"] == "failed"
    assert status["health"] == "unknown"
    await supervisor.close()


async def test_large_rpc_contract_and_response_exceed_default_stream_limit(
    tmp_path: Path,
) -> None:
    gate = asyncio.Event()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: GatedFakeQoderTransport(gate, session_id="session-large"),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()

    accepted = await call(
        socket_path,
        "spawn",
        {
            "role": "auditor",
            "cwd": str(tmp_path),
            "model": "qwen-auditor",
            "objective": "x" * 70_000,
        },
        request_id="large-request",
    )
    assert isinstance(accepted, dict)
    assert accepted["state"] == "starting"

    response_socket = tmp_path / "large-response.sock"

    async def respond_large(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        request = json.loads(await reader.readline())
        response = {
            "request_id": request["request_id"],
            "ok": True,
            "result": {"text": "y" * 70_000},
        }
        writer.write(json.dumps(response).encode() + b"\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    response_server = await asyncio.start_unix_server(
        respond_large, path=response_socket
    )
    large_result = await call(
        response_socket,
        "status",
        {"worker_id": "worker-large"},
        request_id="large-response",
    )
    assert isinstance(large_result, dict)
    assert large_result["text"] == "y" * 70_000

    response_server.close()
    await response_server.wait_closed()
    await server.close()
    await supervisor.close()


async def test_rpc_rejects_over_limit_request_with_correlated_error(
    tmp_path: Path,
) -> None:
    gate = asyncio.Event()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: GatedFakeQoderTransport(gate, session_id="session-over-limit"),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    reader, writer = await asyncio.open_unix_connection(socket_path)
    writer.write(
        b'{"method":"status","params":{"padding":"'
        + b"z" * (4 * 1024 * 1024 - 256)
        + b'"},"request_id":"over-limit","overflow":"'
        + b"z" * 512
        + b'"}\n'
    )
    await writer.drain()

    response = await _read_response(reader)
    assert response == {
        "request_id": "over-limit",
        "ok": False,
        "error": {
            "code": "frame_too_large",
            "message": "RPC frame exceeds the 4194304-byte limit.",
        },
    }
    writer.close()
    await writer.wait_closed()
    await server.close()
    await supervisor.close()


async def test_rpc_over_limit_request_with_near_limit_id_stays_structured(
    tmp_path: Path,
) -> None:
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: FakeQoderTransport(models=(), events=()),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    reader, writer = await asyncio.open_unix_connection(socket_path)
    near_limit_id = b"r" * (4 * 1024 * 1024 - 32)
    writer.write(
        b'{"request_id":"'
        + near_limit_id
        + b'","method":"status","params":{}}\n'
    )
    await writer.drain()

    response = await _read_response(reader)
    assert response == {
        "request_id": None,
        "ok": False,
        "error": {
            "code": "frame_too_large",
            "message": "RPC frame exceeds the 4194304-byte limit.",
        },
    }
    writer.close()
    await writer.wait_closed()
    await server.close()
    await supervisor.close()


async def test_rpc_client_rejects_overlong_request_id_before_connecting(
    tmp_path: Path,
) -> None:
    with pytest.raises(RPCClientError) as captured:
        await call(
            tmp_path / "missing.sock",
            "status",
            {"worker_id": "worker-safe"},
            request_id="r" * 129,
        )

    assert captured.value.code == "invalid_request"
    assert captured.value.message == (
        "request_id must be a non-empty string of at most 128 characters."
    )


async def test_rpc_client_rejects_over_limit_response_structurally(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "oversized-response.sock"

    async def respond_oversized(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        request = json.loads(await reader.readline())
        prefix = json.dumps(
            {
                "request_id": request["request_id"],
                "ok": True,
                "result": {"text": ""},
            }
        ).encode()
        writer.write(prefix[:-4] + b"z" * (4 * 1024 * 1024) + b'"}}\n')
        await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(respond_oversized, path=socket_path)

    with pytest.raises(RPCClientError) as captured:
        await call(
            socket_path,
            "status",
            {"worker_id": "worker-large"},
            request_id="oversized-response",
        )

    assert captured.value.code == "frame_too_large"
    server.close()
    await server.wait_closed()


@pytest.mark.parametrize("parseable", (True, False))
async def test_result_redacts_credentials_recursively_and_in_sqlite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    parseable: bool,
) -> None:
    secret = "sk-live-secret-value"
    environment_secret = "opaque-environment-credential"
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", environment_secret)
    if parseable:
        raw_result = json.dumps(
            {
                "outcome": "completed",
                "summary": f"{secret} {'x' * 10_000}",
                "files": [secret, *["safe-file"] * 200],
                "validation": [environment_secret],
                "risks": [secret],
                "verdict": secret,
                "confirmed": [secret],
                "findings": [
                    {
                        "severity": secret,
                        "evidence": environment_secret,
                        "affected_requirement_or_location": secret,
                    }
                ],
                "required_changes": [secret],
            }
        )
    else:
        raw_result = f"unparseable {secret} {environment_secret}"
    transport = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(
            ResultEvent(
                session_id="session-redacted",
                is_error=False,
                result=raw_result,
                errors=(secret,),
                permission_denials=(environment_secret,),
            ),
        ),
    )
    store = WorkerStore(tmp_path / "state")
    supervisor = Supervisor(store, lambda _: transport, sdk_version="1.0.13")
    accepted = await supervisor.spawn(
        AuditContract(objective="audit result boundary", cwd=tmp_path.parent)
    )
    worker_id = str(accepted["worker_id"])
    _events = [
        event
        async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]

    result = await supervisor.result(worker_id)
    assert result is not None
    rendered = json.dumps(result, sort_keys=True)
    assert secret not in rendered
    assert environment_secret not in rendered
    assert "[REDACTED]" in rendered
    if parseable:
        assert len(str(result["summary"])) <= 4096
        assert isinstance(result["files"], list)
        assert len(result["files"]) == 128
    with sqlite3.connect(store.database_path) as connection:
        durable = connection.execute(
            "SELECT result_summary FROM workers WHERE worker_id = ?", (worker_id,)
        ).fetchone()[0]
    assert secret not in durable
    assert environment_secret not in durable
    await supervisor.close()


async def test_result_redacts_opaque_values_under_credential_named_keys(
    tmp_path: Path,
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker = await store.create_worker(
        role="auditor",
        cwd=tmp_path.parent,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )
    opaque_password = "opaque-token-value"
    opaque_api_key = "AIzaExampleOpaqueValue"
    opaque_token = "unstructured-auth-material"

    await store.record_result(
        worker.worker_id,
        outcome="completed",
        result_summary={
            "password": opaque_password,
            "nested": {"api_key": opaque_api_key, "token": opaque_token},
        },
        resolved_model="Qwen3.8-Max",
        actual_models=("Qwen3.8-Max",),
        session_id="session-safe",
        nested_state="settled",
        warnings=(),
    )

    persisted = await store.get_worker(worker.worker_id)
    assert persisted is not None
    assert persisted.result_summary == {
        "outcome": "completed",
        "password": "[REDACTED]",
        "nested": {
            "api_key": "[REDACTED]",
            "token": "[REDACTED]",
        },
    }
    with sqlite3.connect(store.database_path) as connection:
        durable = connection.execute(
            "SELECT result_summary FROM workers WHERE worker_id = ?",
            (worker.worker_id,),
        ).fetchone()[0]
    assert opaque_password not in durable
    assert opaque_api_key not in durable
    assert opaque_token not in durable
    await store.close()


async def test_result_budget_preserves_keys_without_redaction_collisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment_secret = "opaque-environment-key"
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", environment_secret)
    store = WorkerStore(tmp_path / "state")
    worker = await store.create_worker(
        role="auditor",
        cwd=tmp_path.parent,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )

    await store.record_result(
        worker.worker_id,
        outcome="completed",
        result_summary={
            environment_secret: "first opaque value",
            "sk-another-secret-key": "second opaque value",
            "confirmed": ["x" * 4096] * 128,
            "outcome": "completed",
            "summary": "late required summary",
        },
        resolved_model="Qwen3.8-Max",
        actual_models=("Qwen3.8-Max",),
        session_id="session-safe",
        nested_state="settled",
        warnings=(),
    )

    persisted = await store.get_worker(worker.worker_id)
    assert persisted is not None
    assert persisted.result_summary is not None
    assert set(persisted.result_summary) == {
        "confirmed",
        "outcome",
        "summary",
        "[REDACTED]",
        "[REDACTED]#2",
    }
    assert persisted.result_summary["outcome"] == "completed"
    assert persisted.result_summary["summary"] == ""
    assert persisted.result_summary["[REDACTED]"] == "[REDACTED]"
    assert persisted.result_summary["[REDACTED]#2"] == "[REDACTED]"
    await store.close()


async def test_result_bounds_apply_before_serializing_deep_input(
    tmp_path: Path,
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker = await store.create_worker(
        role="auditor",
        cwd=tmp_path,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )
    nested: dict[str, object] = {}
    nested["next"] = nested

    await store.record_result(
        worker.worker_id,
        outcome="completed",
        result_summary=cast(dict[str, JsonValue], {"deep": nested}),
        resolved_model="Qwen3.8-Max",
        actual_models=("Qwen3.8-Max",),
        session_id="session-safe",
        nested_state="settled",
        warnings=(),
    )

    persisted = await store.get_worker(worker.worker_id)
    assert persisted is not None
    assert persisted.result_summary is not None
    cursor = persisted.result_summary["deep"]
    for _ in range(5):
        assert isinstance(cursor, dict)
        cursor = cursor["next"]
    assert isinstance(cursor, dict)
    assert cursor["next"] is None
    await store.close()
