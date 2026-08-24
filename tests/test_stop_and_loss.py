import asyncio
import json
from collections.abc import AsyncIterator, Mapping
from io import StringIO
from pathlib import Path

import pytest
from qoder_agent_sdk import ProcessError

from qworker.cli import run
from qworker.control import ControlCallbacks, PermissionRequest
from qworker.domain import AuditContract
from qworker.events import AdapterEvent, ResultEvent
from qworker.lifecycle import WorkerStateReducer
from qworker.model_policy import AvailableModel
from qworker.qoder_sdk import QoderSDKTransport
from qworker.rpc import RPCServer
from qworker.store import EventRecord, JsonValue, WorkerStore
from qworker.supervisor import Supervisor, SupervisorError
from tests.fakes import SUCCESSFUL_AUDIT_REPORT, FakeQoderTransport


class StoppableFakeQoderTransport(FakeQoderTransport):
    """Deterministic live transport with explicit interrupt and EOF controls."""

    def __init__(
        self,
        order: list[str],
        *,
        result_after_interrupt: bool = False,
    ) -> None:
        events: tuple[AdapterEvent, ...] = ()
        if result_after_interrupt:
            events = (
                ResultEvent(
                    session_id="session-after-interrupt",
                    is_error=False,
                    result=SUCCESSFUL_AUDIT_REPORT,
                    model_usage=("Qwen3.8-Max",),
                ),
            )
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=events,
        )
        self._order = order
        self._result_after_interrupt = result_after_interrupt
        self._finish = asyncio.Event()
        self.callbacks: ControlCallbacks | None = None

    def bind_control(self, callbacks: ControlCallbacks) -> None:
        self.callbacks = callbacks

    async def interrupt(self) -> None:
        self._order.append("interrupt")
        if self._result_after_interrupt:
            self._finish.set()

    def exit(self) -> None:
        self._finish.set()

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        await self._finish.wait()
        async for event in super().messages():
            yield event

    async def disconnect(self) -> None:
        if not self.disconnected:
            self._order.append("disconnect")
        await super().disconnect()


class OrderingStore(WorkerStore):
    def __init__(self, state_dir: Path, order: list[str]) -> None:
        super().__init__(state_dir)
        self._order = order

    async def expire_approval_requests(
        self,
        worker_id: str,
        *,
        expected_attempt: int,
        request_ids: tuple[str, ...],
    ) -> tuple[EventRecord, ...]:
        self._order.append("expire_approvals")
        return await super().expire_approval_requests(
            worker_id,
            expected_attempt=expected_attempt,
            request_ids=request_ids,
        )

    async def append_event(
        self,
        worker_id: str,
        event_type: str,
        payload: Mapping[str, JsonValue],
    ) -> EventRecord:
        if event_type == "worker.state_changed" and payload.get("state") in (
            "cancelled",
            "lost",
        ):
            self._order.append(str(payload["state"]))
        return await super().append_event(worker_id, event_type, payload)


async def _spawn_running(
    supervisor: Supervisor,
    cwd: Path,
) -> str:
    accepted = await supervisor.spawn(
        AuditContract(objective="exercise stop and loss", cwd=cwd)
    )
    worker_id = str(accepted["worker_id"])
    for _ in range(100):
        if (await supervisor.status(worker_id))["state"] == "running":
            return worker_id
        await asyncio.sleep(0)
    raise AssertionError("worker did not enter running state")


async def test_graceful_stop_interrupts_then_disconnects_and_denies_approvals(
    tmp_path: Path,
) -> None:
    order: list[str] = []
    transport = StoppableFakeQoderTransport(order)
    store = OrderingStore(tmp_path / "state", order)
    supervisor = Supervisor(
        store,
        lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0.01,
    )
    worker_id = await _spawn_running(supervisor, tmp_path)
    assert transport.callbacks is not None
    approval = asyncio.create_task(
        transport.callbacks.request_permission(
            PermissionRequest(
                tool_name="Read",
                agent_id=None,
                display_message="Read one safe file?",
            )
        )
    )
    for _ in range(100):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)

    stopped = await supervisor.stop(worker_id)

    assert stopped["state"] == "cancelled"
    assert stopped["health"] == "exited"
    assert stopped["force"] is False
    assert (await approval).action == "deny"
    assert order == ["interrupt", "disconnect", "expire_approvals", "cancelled"]
    assert (await supervisor.status(worker_id))["pending_approvals"] == []
    with pytest.raises(SupervisorError) as late_response:
        await supervisor.respond(worker_id, "not-pending", {"action": "allow"})
    assert late_response.value.code == "approval_not_pending"
    await supervisor.close()


async def test_force_stop_skips_interrupt_and_preserves_workspace_files(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "shared-workspace"
    workspace.mkdir()
    existing = workspace / "existing.txt"
    existing.write_text("caller state\n", encoding="utf-8")
    order: list[str] = []
    transport = StoppableFakeQoderTransport(order)
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    worker_id = await _spawn_running(supervisor, workspace)

    stopped = await supervisor.stop(worker_id, force=True)

    assert stopped["state"] == "cancelled"
    assert stopped["force"] is True
    assert order == ["disconnect"]
    assert existing.read_text(encoding="utf-8") == "caller state\n"
    assert transport.disconnected is True
    await supervisor.close()


async def test_grace_timeout_bounds_a_stuck_interrupt(tmp_path: Path) -> None:
    class StuckInterruptTransport(StoppableFakeQoderTransport):
        async def interrupt(self) -> None:
            self._order.append("interrupt")
            await asyncio.Event().wait()

    order: list[str] = []
    transport = StuckInterruptTransport(order)
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0.01,
    )
    worker_id = await _spawn_running(supervisor, tmp_path)

    stopped = await asyncio.wait_for(supervisor.stop(worker_id), timeout=0.2)

    assert stopped["state"] == "cancelled"
    assert order == ["interrupt", "disconnect"]
    await supervisor.close()


async def test_unexpected_eof_before_result_is_lost_without_retry(
    tmp_path: Path,
) -> None:
    transport = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(),
    )
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="classify eof", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])
    events = [
        event async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]

    status = await supervisor.status(worker_id)
    assert status["state"] == "lost"
    assert status["health"] == "exited"
    assert status["attempt"] == 1
    assert await supervisor.result(worker_id) is None
    assert "result.received" not in [event["type"] for event in events]
    assert transport.disconnected is True
    await supervisor.close()


async def test_process_exit_before_result_is_lost(tmp_path: Path) -> None:
    class ExitedTransport(FakeQoderTransport):
        async def messages(self) -> AsyncIterator[AdapterEvent]:
            raise EOFError("QoderCLI exited")
            yield  # pragma: no cover - retain async-generator shape

    transport = ExitedTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(),
    )
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="classify process exit", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])
    _events = [
        event async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]

    assert (await supervisor.status(worker_id))["state"] == "lost"
    assert await supervisor.result(worker_id) is None
    await supervisor.close()


async def test_loss_expires_pending_callback_and_late_callbacks_fail_closed(
    tmp_path: Path,
) -> None:
    order: list[str] = []
    transport = StoppableFakeQoderTransport(order)
    store = WorkerStore(tmp_path / "state")
    supervisor = Supervisor(
        store,
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    worker_id = await _spawn_running(supervisor, tmp_path)
    assert transport.callbacks is not None
    pending = asyncio.create_task(
        transport.callbacks.request_permission(
            PermissionRequest(
                tool_name="Read",
                agent_id=None,
                display_message="Read one safe file?",
            )
        )
    )
    for _ in range(100):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)

    transport.exit()
    _events = [
        event async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]

    assert (await pending).action == "deny"
    assert (await supervisor.status(worker_id))["state"] == "lost"
    resolved = [
        event
        for event in await store.events_since(worker_id)
        if event.type == "approval.resolved"
    ]
    assert [event.payload["status"] for event in resolved] == ["expired"]
    late = await transport.callbacks.request_permission(
        PermissionRequest(
            tool_name="Read",
            agent_id=None,
            display_message="late request",
        )
    )
    assert late.action == "deny"
    assert len(
        [
            event
            for event in await store.events_since(worker_id)
            if event.type == "approval.requested"
        ]
    ) == 1
    await supervisor.close()


async def test_eof_after_result_preserves_result_derived_completion(
    tmp_path: Path,
) -> None:
    transport = FakeQoderTransport.successful_audit(model="Qwen3.8-Max")
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="preserve result", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])
    _events = [
        event async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]

    assert (await supervisor.status(worker_id))["state"] == "completed"
    result = await supervisor.result(worker_id)
    assert result is not None
    assert result["session_id"] == "session-1"
    await supervisor.close()


async def test_result_wins_a_graceful_stop_race(tmp_path: Path) -> None:
    order: list[str] = []
    transport = StoppableFakeQoderTransport(order, result_after_interrupt=True)
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0.1,
    )
    worker_id = await _spawn_running(supervisor, tmp_path)

    stopped = await supervisor.stop(worker_id)

    assert stopped["state"] == "completed"
    result = await supervisor.result(worker_id)
    assert result is not None
    assert result["session_id"] == "session-after-interrupt"
    assert order == ["interrupt", "disconnect"]
    await supervisor.close()


async def test_cli_and_rpc_stop_and_reject_selected_nested_agent(
    tmp_path: Path,
) -> None:
    order: list[str] = []
    transport = StoppableFakeQoderTransport(order)
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    worker_id = await _spawn_running(supervisor, tmp_path)
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()

    unsupported_output = StringIO()
    unsupported = await run(
        [
            "--socket",
            str(socket_path),
            "stop",
            worker_id,
            "--agent-id",
            "nested-unsupported",
            "--json",
        ],
        stdin=StringIO(),
        stdout=unsupported_output,
    )
    assert unsupported == 1
    assert json.loads(unsupported_output.getvalue())["error"]["code"] == (
        "unsupported_operation"
    )
    assert (await supervisor.status(worker_id))["state"] == "running"

    output = StringIO()
    exit_code = await run(
        ["--socket", str(socket_path), "stop", worker_id, "--force", "--json"],
        stdin=StringIO(),
        stdout=output,
    )
    assert exit_code == 0
    stopped = json.loads(output.getvalue())
    assert stopped["state"] == "cancelled"
    assert stopped["force"] is True

    await server.close()
    await supervisor.close()


async def test_sdk_transport_interrupts_sdk_client() -> None:
    class InterruptClient:
        def __init__(self) -> None:
            self.interrupted = False

        async def interrupt(self) -> None:
            self.interrupted = True

    client = InterruptClient()
    transport = QoderSDKTransport(client)

    await transport.interrupt()

    assert client.interrupted is True


async def test_sdk_process_error_becomes_eof_without_exposing_stderr() -> None:
    class ExitedClient:
        async def receive_messages(self) -> AsyncIterator[object]:
            raise ProcessError(
                "process failed",
                exit_code=17,
                stderr="password=must-not-cross-adapter",
            )
            yield  # pragma: no cover - retain async-generator shape

    transport = QoderSDKTransport(ExitedClient())

    with pytest.raises(EOFError, match="QoderCLI process exited") as captured:
        await anext(transport.messages())

    assert "must-not-cross-adapter" not in str(captured.value)


def test_starting_worker_can_be_cancelled_but_not_completed() -> None:
    cancelled = WorkerStateReducer("starting")
    assert cancelled.transition("cancelled") == "cancelled"

    completed = WorkerStateReducer("starting")
    with pytest.raises(ValueError, match="starting -> completed"):
        completed.transition("completed")
