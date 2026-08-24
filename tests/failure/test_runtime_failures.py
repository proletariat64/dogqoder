import asyncio
import json
import sqlite3
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Literal

import pytest

from qworker.coder import CoderContract
from qworker.domain import AuditContract
from qworker.events import AdapterEvent, ResultEvent
from qworker.model_policy import AvailableModel
from qworker.preflight import (
    AuthSelection,
    PreflightFailure,
    RuntimeInfo,
    RuntimePreflight,
)
from qworker.qoder_sdk import AdapterDiagnostic
from qworker.rpc import RPCClientError, RPCServer, call
from qworker.store import JsonValue, WorkerStore
from qworker.supervisor import Supervisor
from tests.fakes import FakeQoderTransport

type _PreflightStage = Literal["sdk", "runtime", "initialize", "none"]


class _InjectedPreflightBackend:
    def __init__(
        self,
        *,
        stage: _PreflightStage = "none",
        failure: Exception | None = None,
    ) -> None:
        self.stage = stage
        self.failure = failure

    def sdk_version(self) -> str:
        self._raise_at("sdk")
        return "1.0.13"

    async def resolve_runtime(self, _explicit_path: Path | None) -> RuntimeInfo:
        self._raise_at("runtime")
        return RuntimeInfo(
            recorded_path="bundled",
            executable=Path("/deterministic/fake-qodercli"),
            version="0.2.0",
        )

    async def initialize(
        self,
        _cwd: Path,
        _runtime: RuntimeInfo,
        _auth: AuthSelection,
    ) -> tuple[str, ...]:
        self._raise_at("initialize")
        return ("modelPolicy",)

    async def local_login_status(self, _runtime: RuntimeInfo) -> bool:
        return False

    def _raise_at(self, stage: _PreflightStage) -> None:
        if self.stage == stage and self.failure is not None:
            raise self.failure


@pytest.mark.parametrize(
    ("stage", "failure", "environ", "expected_code"),
    (
        ("none", None, {}, "auth_required"),
        (
            "runtime",
            PreflightFailure("runtime_not_found", "runtime-secret"),
            {},
            "runtime_not_found",
        ),
        (
            "runtime",
            PreflightFailure("runtime_incompatible", "runtime-secret"),
            {},
            "runtime_incompatible",
        ),
        (
            "initialize",
            PreflightFailure("initialize_timeout", "initialize-secret"),
            {"QODER_PERSONAL_ACCESS_TOKEN": "credential-sentinel"},
            "initialize_timeout",
        ),
        (
            "runtime",
            PreflightFailure("invalid_request", "configuration-secret"),
            {},
            "invalid_request",
        ),
        (
            "initialize",
            RuntimeError("protocol-secret"),
            {"QODER_PERSONAL_ACCESS_TOKEN": "credential-sentinel"},
            "sdk_protocol_error",
        ),
    ),
)
async def test_preflight_failures_have_stable_secret_free_diagnostics(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    stage: _PreflightStage,
    failure: Exception | None,
    environ: dict[str, str],
    expected_code: str,
) -> None:
    result = await RuntimePreflight(
        _InjectedPreflightBackend(stage=stage, failure=failure),
        environ=environ,
    ).run(tmp_path)

    assert result.ok is False
    assert result.error is not None
    assert result.error.code == expected_code
    rendered = json.dumps(result.to_json(), sort_keys=True)
    for sentinel in (
        "credential-sentinel",
        "runtime-secret",
        "initialize-secret",
        "configuration-secret",
        "protocol-secret",
    ):
        assert sentinel not in rendered
        assert sentinel not in caplog.text


class _PostPromptEOFTransport(FakeQoderTransport):
    async def messages(self) -> AsyncIterator[AdapterEvent]:
        raise EOFError("deterministic QoderCLI EOF")
        yield  # pragma: no cover - preserve async-generator shape


async def _follow_to_terminal(
    supervisor: Supervisor,
    worker_id: str,
) -> list[dict[str, JsonValue]]:
    return [event async for event in supervisor.watch(worker_id, since=0, follow=True)]


async def _wait_for_state(
    supervisor: Supervisor,
    worker_id: str,
    expected: str,
) -> None:
    for _ in range(100):
        if (await supervisor.status(worker_id))["state"] == expected:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"worker did not enter {expected}")


async def test_model_failure_before_initialization_is_durably_failed(
    tmp_path: Path,
) -> None:
    transport = FakeQoderTransport(models=(), events=())
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="inject unavailable model", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])

    events = await _follow_to_terminal(supervisor, worker_id)
    status = await supervisor.status(worker_id)
    result = await supervisor.result(worker_id)

    assert status["state"] == "failed"
    assert status["health"] == "unknown"
    assert result is not None
    assert result["errors"] == ["model_unavailable"]
    assert [
        event["payload"] for event in events if event["type"] == "worker.state_changed"
    ] == [{"schema_version": 1, "state": "failed"}]
    await supervisor.close()


async def test_sdk_failure_before_initialization_is_redacted_and_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = "sdk-construction-credential"
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", sentinel)
    factory_calls = 0

    def failing_factory(_cwd: Path) -> FakeQoderTransport:
        nonlocal factory_calls
        factory_calls += 1
        raise AdapterDiagnostic("sdk_protocol_error", f"failure: {sentinel}")

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        failing_factory,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="inject SDK construction failure", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])

    await _follow_to_terminal(supervisor, worker_id)
    result = await supervisor.result(worker_id)

    assert factory_calls == 1
    assert result is not None
    assert result["errors"] == ["sdk_protocol_error"]
    assert sentinel not in json.dumps(result, sort_keys=True)
    await supervisor.close()


async def test_post_prompt_coder_eof_is_lost_without_retry(
    tmp_path: Path,
) -> None:
    transport = _PostPromptEOFTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(),
    )
    factory_calls = 0

    def transport_factory(_cwd: Path) -> FakeQoderTransport:
        nonlocal factory_calls
        factory_calls += 1
        return transport

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        coder_transport_factory=transport_factory,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        CoderContract(objective="mutate once, never retry", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])

    await _follow_to_terminal(supervisor, worker_id)
    status = await supervisor.status(worker_id)

    assert factory_calls == 1
    assert transport.calls.count("send") == 1
    assert status["state"] == "lost"
    assert status["health"] == "exited"
    assert await supervisor.result(worker_id) is None
    await supervisor.close()


async def test_result_error_preserves_report_and_permission_denials(
    tmp_path: Path,
) -> None:
    report = json.dumps(
        {
            "outcome": "partial",
            "summary": "validation failed",
            "files": ["src/changed.py"],
            "validation": ["pytest: failed"],
            "risks": ["tests remain red"],
        }
    )
    transport = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(
            ResultEvent(
                session_id="session-result-error",
                is_error=True,
                result=report,
                permission_denials=("Bash: auditor_tool_denied",),
                errors=("tool execution failed",),
            ),
        ),
    )
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        coder_transport_factory=lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        CoderContract(objective="return one structured failure", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])

    await _follow_to_terminal(supervisor, worker_id)
    result = await supervisor.result(worker_id)

    assert (await supervisor.status(worker_id))["state"] == "failed"
    assert result is not None
    assert result["summary"] == "validation failed"
    assert result["files"] == ["src/changed.py"]
    assert result["errors"] == [
        "tool execution failed",
        "Bash: auditor_tool_denied",
    ]
    await supervisor.close()


async def test_rpc_maps_database_failure_without_exposing_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = "database-exception-secret"
    store = WorkerStore(tmp_path / "state")

    async def failing_create_worker(**_kwargs: object) -> object:
        raise sqlite3.OperationalError(sentinel)

    monkeypatch.setattr(store, "create_worker", failing_create_worker)
    supervisor = Supervisor(
        store,
        lambda _cwd: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()

    with pytest.raises(RPCClientError) as caught:
        await call(
            socket_path,
            "spawn",
            {
                "role": "auditor",
                "cwd": str(tmp_path),
                "objective": "inject database failure",
            },
        )

    assert caught.value.code == "sdk_protocol_error"
    assert caught.value.message == "Supervisor request failed."
    assert sentinel not in caught.value.message
    await server.close()
    await supervisor.close()


async def test_rpc_exposes_stable_control_failure_codes(
    tmp_path: Path,
) -> None:
    transport = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(),
        hang_after_events=True,
    )
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0,
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
            "objective": "exercise stable RPC failures",
        },
    )
    assert isinstance(accepted, dict)
    worker_id = str(accepted["worker_id"])
    await _wait_for_state(supervisor, worker_id, "running")

    cases: tuple[tuple[str, dict[str, JsonValue], str], ...] = (
        ("unknown", {}, "invalid_request"),
        ("status", {"worker_id": "missing-worker"}, "worker_not_found"),
        (
            "respond",
            {
                "worker_id": worker_id,
                "request_id": "missing-request",
                "response": {"action": "deny"},
            },
            "approval_not_pending",
        ),
        ("resume", {"worker_id": worker_id}, "resume_not_possible"),
    )
    for method, params, expected_code in cases:
        with pytest.raises(RPCClientError) as caught:
            await call(socket_path, method, params)
        assert caught.value.code == expected_code

    cancellation = await call(
        socket_path,
        "cancel_message",
        {"worker_id": worker_id, "message_id": str(uuid.uuid4())},
    )
    assert isinstance(cancellation, dict)
    assert cancellation == {
        "message_id": cancellation["message_id"],
        "cancelled": False,
        "code": "message_not_cancellable",
    }

    await supervisor.stop(worker_id, force=True)
    with pytest.raises(RPCClientError) as not_live:
        await call(
            socket_path,
            "steer",
            {"worker_id": worker_id, "message": "too late"},
        )
    assert not_live.value.code == "worker_not_live"
    await server.close()
    await supervisor.close()


async def test_missing_rpc_socket_is_supervisor_unavailable(tmp_path: Path) -> None:
    with pytest.raises(RPCClientError) as caught:
        await call(
            tmp_path / "missing.sock",
            "status",
            {"worker_id": "missing-worker"},
        )

    assert caught.value.code == "supervisor_unavailable"


async def test_overlapping_coders_emit_nonrejecting_conflict_warning(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    def hanging_coder(_cwd: Path) -> FakeQoderTransport:
        return FakeQoderTransport(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(),
            hang_after_events=True,
        )

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        coder_transport_factory=hanging_coder,
        sdk_version="1.0.13",
        stop_timeout=0,
    )
    first = await supervisor.spawn(CoderContract(objective="first", cwd=workspace))
    await _wait_for_state(supervisor, str(first["worker_id"]), "running")
    second = await supervisor.spawn(CoderContract(objective="second", cwd=workspace))

    assert second["state"] == "starting"
    assert second["warnings"] == [
        {
            "code": "write_conflict_warning",
            "worker_id": first["worker_id"],
            "cwd": str(workspace),
            "relation": "same",
        }
    ]
    await supervisor.close()
