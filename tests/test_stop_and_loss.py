import asyncio
import json
import tempfile
from collections.abc import AsyncIterator, Mapping
from io import StringIO
from pathlib import Path
from typing import Literal

import pytest
from qoder_agent_sdk import ProcessError

from qworker.cli import run
from qworker.coder import CoderContract
from qworker.control import ControlCallbacks, PermissionRequest
from qworker.domain import AuditContract
from qworker.events import AdapterEvent, ResultEvent, TaskStartedEvent
from qworker.lifecycle import NestedState, WorkerStateReducer
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


class PausedResultStore(WorkerStore):
    """Expose the accepted-result persistence window to deterministic stop tests."""

    def __init__(self, state_dir: Path) -> None:
        super().__init__(state_dir)
        self.record_started = asyncio.Event()
        self.allow_record = asyncio.Event()

    async def record_result(
        self,
        worker_id: str,
        *,
        outcome: Literal["completed", "partial", "blocked", "failed"],
        result_summary: Mapping[str, JsonValue],
        resolved_model: str | None,
        actual_models: tuple[str, ...],
        session_id: str | None,
        nested_state: NestedState,
        warnings: tuple[str, ...],
    ) -> EventRecord:
        self.record_started.set()
        await self.allow_record.wait()
        return await super().record_result(
            worker_id,
            outcome=outcome,
            result_summary=result_summary,
            resolved_model=resolved_model,
            actual_models=actual_models,
            session_id=session_id,
            nested_state=nested_state,
            warnings=warnings,
        )


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


async def test_graceful_stop_induced_eof_is_cancelled(tmp_path: Path) -> None:
    class InterruptEOFTransport(StoppableFakeQoderTransport):
        async def interrupt(self) -> None:
            self._order.append("interrupt")
            self.exit()

        async def messages(self) -> AsyncIterator[AdapterEvent]:
            await self._finish.wait()
            raise EOFError("interrupt closed QoderCLI stream")
            yield  # pragma: no cover - retain async-generator shape

    order: list[str] = []
    transport = InterruptEOFTransport(order)
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0.1,
    )
    worker_id = await _spawn_running(supervisor, tmp_path)

    stopped = await supervisor.stop(worker_id)

    assert stopped["state"] == "cancelled"
    assert stopped["health"] == "exited"
    assert await supervisor.result(worker_id) is None
    assert order == ["interrupt", "disconnect"]
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


async def test_force_stop_induced_eof_is_cancelled(tmp_path: Path) -> None:
    class DisconnectEOFTransport(StoppableFakeQoderTransport):
        def __init__(self, order: list[str]) -> None:
            super().__init__(order)
            self._disconnect_started = asyncio.Event()
            self._disconnecting = False

        async def messages(self) -> AsyncIterator[AdapterEvent]:
            await self._disconnect_started.wait()
            raise EOFError("disconnect closed QoderCLI stream")
            yield  # pragma: no cover - retain async-generator shape

        async def disconnect(self) -> None:
            if self.disconnected or self._disconnecting:
                return
            self._disconnecting = True
            self._order.append("disconnect")
            self._disconnect_started.set()
            for _ in range(10):
                await asyncio.sleep(0)
            self.disconnected = True

    order: list[str] = []
    transport = DisconnectEOFTransport(order)
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    worker_id = await _spawn_running(supervisor, tmp_path)

    stopped = await supervisor.stop(worker_id, force=True)

    assert stopped["state"] == "cancelled"
    assert stopped["health"] == "exited"
    assert await supervisor.result(worker_id) is None
    assert order == ["disconnect"]
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


@pytest.mark.parametrize("joining_call", ("force", "close"))
async def test_joining_force_escalates_a_blocked_graceful_stop(
    tmp_path: Path,
    joining_call: Literal["force", "close"],
) -> None:
    class BlockedInterruptTransport(StoppableFakeQoderTransport):
        def __init__(self, order: list[str]) -> None:
            super().__init__(order)
            self.interrupt_started = asyncio.Event()

        async def interrupt(self) -> None:
            self._order.append("interrupt")
            self.interrupt_started.set()
            await asyncio.Event().wait()

    order: list[str] = []
    transport = BlockedInterruptTransport(order)
    store = WorkerStore(tmp_path / "state")
    supervisor = Supervisor(
        store,
        lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0.1,
    )
    worker_id = await _spawn_running(supervisor, tmp_path)
    normal_stop = asyncio.create_task(supervisor.stop(worker_id))
    await asyncio.wait_for(transport.interrupt_started.wait(), timeout=0.1)
    joining = asyncio.create_task(
        supervisor.close()
        if joining_call == "close"
        else supervisor.stop(worker_id, force=True)
    )

    try:
        await asyncio.wait_for(asyncio.shield(joining), timeout=0.05)
        await asyncio.wait_for(normal_stop, timeout=0.05)
    finally:
        if not joining.done():
            joining.cancel()
        await asyncio.gather(joining, return_exceptions=True)
        await asyncio.wait_for(normal_stop, timeout=0.2)
        if joining_call != "close":
            await supervisor.close()

    status = await supervisor.status(worker_id)
    terminal_states = [
        event.payload["state"]
        for event in await store.events_since(worker_id)
        if event.type == "worker.state_changed"
        and event.payload.get("state") in _TERMINAL_STATE_NAMES
    ]
    assert status["state"] == "cancelled"
    assert status["health"] == "exited"
    assert terminal_states == ["cancelled"]
    assert order == ["interrupt", "disconnect"]


@pytest.mark.parametrize("operation", ("normal", "force", "close"))
async def test_stop_and_close_bound_a_nonreturning_disconnect(
    tmp_path: Path,
    operation: Literal["normal", "force", "close"],
) -> None:
    class NonReturningDisconnectTransport(StoppableFakeQoderTransport):
        def __init__(self, order: list[str]) -> None:
            super().__init__(order)
            self.child_live = True
            self.aborted = False
            self.disconnect_started = asyncio.Event()
            self.allow_disconnect = asyncio.Event()

        async def disconnect(self) -> None:
            self._order.append("disconnect")
            self.disconnect_started.set()
            try:
                await self.allow_disconnect.wait()
            except asyncio.CancelledError:
                self.child_live = False
                self.disconnected = True
                raise
            self.child_live = False
            self.disconnected = True

        def abort(self) -> None:
            self.aborted = True
            self.child_live = False
            self.disconnected = True

    order: list[str] = []
    transport = NonReturningDisconnectTransport(order)
    store = WorkerStore(tmp_path / "state")
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
                display_message="Read while testing bounded teardown?",
            )
        )
    )
    for _ in range(100):
        if (await supervisor.status(worker_id))["state"] == "requires_action":
            break
        await asyncio.sleep(0)

    try:
        if operation == "close":
            await asyncio.wait_for(supervisor.close(), timeout=0.1)
        else:
            await asyncio.wait_for(
                supervisor.stop(worker_id, force=operation == "force"),
                timeout=0.1,
            )
    finally:
        transport.allow_disconnect.set()
        await asyncio.wait_for(supervisor.close(), timeout=0.5)

    status = await supervisor.status(worker_id)
    resolved = [
        event
        for event in await store.events_since(worker_id)
        if event.type == "approval.resolved"
    ]
    assert status["state"] == "cancelled"
    assert status["health"] == "exited"
    assert await supervisor.result(worker_id) is None
    assert (await approval).action == "deny"
    assert [event.payload["status"] for event in resolved] == ["expired"]
    assert transport.child_live is False
    assert transport.aborted is True
    assert transport.disconnect_started.is_set()
    assert not [
        task
        for task in asyncio.all_tasks()
        if task.get_name().startswith("qworker:")
    ]


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


@pytest.mark.parametrize("exceptional_eof", (False, True))
@pytest.mark.parametrize("is_error", (False, True))
async def test_eof_during_nested_settlement_preserves_main_result(
    tmp_path: Path,
    exceptional_eof: bool,
    is_error: bool,
) -> None:
    class SettlementEOFTransport(FakeQoderTransport):
        async def messages(self) -> AsyncIterator[AdapterEvent]:
            yield TaskStartedEvent(
                task_id="nested-unsettled",
                description="inspect nested behavior",
            )
            yield ResultEvent(
                session_id="session-before-eof",
                is_error=is_error,
                result=SUCCESSFUL_AUDIT_REPORT,
                model_usage=("Qwen3.8-Max",),
                errors=("model_error",) if is_error else (),
            )
            if exceptional_eof:
                raise EOFError("QoderCLI exited during settlement")

    transport = SettlementEOFTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(),
    )
    store = WorkerStore(tmp_path / "state")
    supervisor = Supervisor(
        store,
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="preserve settled result", cwd=tmp_path)
    )
    worker_id = str(accepted["worker_id"])
    events = [
        event async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]

    expected_state = "failed" if is_error else "completed"
    status = await supervisor.status(worker_id)
    result = await supervisor.result(worker_id)
    assert status["state"] == expected_state
    assert status["health"] == "exited"
    assert result is not None
    assert result["outcome"] == expected_state
    assert result["session_id"] == "session-before-eof"
    assert result["nested_state"] == "unknown"
    warnings = result["warnings"]
    assert isinstance(warnings, list)
    assert "nested_terminal_event_missing" in warnings
    assert [event["type"] for event in events][-3:] == [
        "worker.warning",
        "worker.health_changed",
        "worker.state_changed",
    ]
    assert any(event["type"] == "result.received" for event in events)
    await supervisor.close()


@pytest.mark.parametrize("role", ("auditor", "coder"))
async def test_graceful_stop_wins_result_released_by_interrupt(
    tmp_path: Path,
    role: str,
) -> None:
    order: list[str] = []
    transport = StoppableFakeQoderTransport(order, result_after_interrupt=True)
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        coder_transport_factory=lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0.1,
    )
    if role == "auditor":
        worker_id = await _spawn_running(supervisor, tmp_path)
    else:
        accepted = await supervisor.spawn(
            CoderContract(objective="exercise coder stop", cwd=tmp_path)
        )
        worker_id = str(accepted["worker_id"])
        for _ in range(100):
            if (await supervisor.status(worker_id))["state"] == "running":
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError("coder did not enter running state")

    stopped = await supervisor.stop(worker_id)

    assert stopped["state"] == "cancelled"
    assert stopped["health"] == "exited"
    result = await supervisor.result(worker_id)
    assert result is not None
    assert result["session_id"] == "session-after-interrupt"
    assert order == ["interrupt", "disconnect"]
    await supervisor.close()


@pytest.mark.parametrize("is_error", (False, True))
async def test_force_stop_preserves_result_already_seen_during_settlement(
    tmp_path: Path,
    is_error: bool,
) -> None:
    class ResultThenDisconnectEOFTransport(FakeQoderTransport):
        def __init__(self) -> None:
            super().__init__(
                models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
                events=(),
            )
            self.result_seen = asyncio.Event()
            self._disconnect_started = asyncio.Event()

        async def messages(self) -> AsyncIterator[AdapterEvent]:
            yield TaskStartedEvent(
                task_id="nested-before-force",
                description="settlement still pending",
            )
            yield ResultEvent(
                session_id="session-before-force",
                is_error=is_error,
                result=SUCCESSFUL_AUDIT_REPORT,
                errors=("model_error",) if is_error else (),
            )
            self.result_seen.set()
            await self._disconnect_started.wait()
            raise EOFError("force disconnect ended settlement")

        async def disconnect(self) -> None:
            self.disconnected = True
            self._disconnect_started.set()

    transport = ResultThenDisconnectEOFTransport()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
        settlement_timeout=60,
    )
    worker_id = await _spawn_running(supervisor, tmp_path)
    await asyncio.wait_for(transport.result_seen.wait(), timeout=0.2)

    stopped = await supervisor.stop(worker_id, force=True)

    expected_state = "failed" if is_error else "completed"
    result = await supervisor.result(worker_id)
    assert stopped["state"] == expected_state
    assert result is not None
    assert result["outcome"] == expected_state
    assert result["session_id"] == "session-before-force"
    assert result["nested_state"] == "unknown"
    warnings = result["warnings"]
    assert isinstance(warnings, list)
    assert "nested_terminal_event_missing" in warnings
    await supervisor.close()


@pytest.mark.parametrize("force", (False, True))
@pytest.mark.parametrize("is_error", (False, True))
async def test_stop_joins_accepted_result_persistence(
    tmp_path: Path,
    force: bool,
    is_error: bool,
) -> None:
    store = PausedResultStore(tmp_path / "state")
    transport = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(
            ResultEvent(
                session_id="session-before-persistence-stop",
                is_error=is_error,
                result=SUCCESSFUL_AUDIT_REPORT,
                errors=("model_error",) if is_error else (),
            ),
        ),
    )
    supervisor = Supervisor(
        store,
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    worker_id = await _spawn_running(supervisor, tmp_path)
    await asyncio.wait_for(store.record_started.wait(), timeout=0.2)

    stop_task = asyncio.create_task(supervisor.stop(worker_id, force=force))
    for _ in range(10):
        await asyncio.sleep(0)
    assert not stop_task.done()
    store.allow_record.set()
    stopped = await asyncio.wait_for(stop_task, timeout=0.2)

    expected_state = "failed" if is_error else "completed"
    result = await supervisor.result(worker_id)
    events = [event async for event in supervisor.watch(worker_id, since=0)]
    assert stopped["state"] == expected_state
    assert result is not None
    assert result["outcome"] == expected_state
    assert result["session_id"] == "session-before-persistence-stop"
    assert [event["type"] for event in events][-3:] == [
        "result.received",
        "worker.health_changed",
        "worker.state_changed",
    ]
    await supervisor.close()


async def _exercise_overlapping_result_stop(
    tmp_path: Path,
    *,
    is_error: bool,
    close_second: bool,
) -> None:
    class OverlappingStopTransport(FakeQoderTransport):
        def __init__(self) -> None:
            super().__init__(
                models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
                events=(),
            )
            self.result_seen = asyncio.Event()
            self.interrupt_started = asyncio.Event()
            self.allow_interrupt = asyncio.Event()
            self.allow_force_disconnect = asyncio.Event()
            self._stream_ended = asyncio.Event()

        async def messages(self) -> AsyncIterator[AdapterEvent]:
            yield TaskStartedEvent(
                task_id="nested-before-overlapping-stop",
                description="settlement still pending",
            )
            yield ResultEvent(
                session_id="session-before-overlapping-stop",
                is_error=is_error,
                result=SUCCESSFUL_AUDIT_REPORT,
                errors=("model_error",) if is_error else (),
            )
            self.result_seen.set()
            await self._stream_ended.wait()
            raise EOFError("overlapping stop ended settlement")

        async def interrupt(self) -> None:
            self.interrupt_started.set()
            await self.allow_interrupt.wait()
            self._stream_ended.set()
            for _ in range(10):
                await asyncio.sleep(0)
            raise EOFError("interrupt observed process exit")

        async def disconnect(self) -> None:
            if not self._stream_ended.is_set():
                await self.allow_force_disconnect.wait()
            self.disconnected = True

    store = PausedResultStore(tmp_path / "state")
    transport = OverlappingStopTransport()
    supervisor = Supervisor(
        store,
        lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0.1,
    )
    worker_id = await _spawn_running(supervisor, tmp_path)
    await asyncio.wait_for(transport.result_seen.wait(), timeout=0.2)

    normal_stop = asyncio.create_task(supervisor.stop(worker_id))
    await asyncio.wait_for(transport.interrupt_started.wait(), timeout=0.2)
    second_call = asyncio.create_task(
        supervisor.close() if close_second else supervisor.stop(worker_id, force=True)
    )
    for _ in range(10):
        await asyncio.sleep(0)
    transport.allow_force_disconnect.set()
    transport.allow_interrupt.set()
    await asyncio.wait_for(store.record_started.wait(), timeout=0.2)
    for _ in range(10):
        await asyncio.sleep(0)
    store.allow_record.set()
    await asyncio.wait_for(
        asyncio.gather(normal_stop, second_call),
        timeout=0.5,
    )

    expected_state = "failed" if is_error else "completed"
    status = await supervisor.status(worker_id)
    result = await supervisor.result(worker_id)
    events = [event async for event in supervisor.watch(worker_id, since=0)]
    terminal_states: list[JsonValue] = []
    for event in events:
        payload = event["payload"]
        if event["type"] == "worker.state_changed" and isinstance(payload, dict):
            state = payload.get("state")
            if state in _TERMINAL_STATE_NAMES:
                terminal_states.append(state)
    assert status["state"] == expected_state
    assert result is not None
    assert result["outcome"] == expected_state
    assert result["session_id"] == "session-before-overlapping-stop"
    assert terminal_states == [expected_state]
    assert [event["type"] for event in events].count("result.received") == 1
    assert not [
        task
        for task in asyncio.all_tasks()
        if task.get_name().startswith("qworker-result:")
    ]
    if not close_second:
        await supervisor.close()


_TERMINAL_STATE_NAMES = frozenset(("completed", "failed", "cancelled", "lost"))


@pytest.mark.parametrize("is_error", (False, True))
async def test_concurrent_normal_and_force_stop_join_one_result_phase(
    tmp_path: Path,
    is_error: bool,
) -> None:
    await _exercise_overlapping_result_stop(
        tmp_path,
        is_error=is_error,
        close_second=False,
    )


@pytest.mark.parametrize("is_error", (False, True))
async def test_stop_and_close_join_one_result_phase(
    tmp_path: Path,
    is_error: bool,
) -> None:
    await _exercise_overlapping_result_stop(
        tmp_path,
        is_error=is_error,
        close_second=True,
    )


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


class AbortProcess:
    def __init__(
        self,
        *,
        returncode: int | None = None,
        kill_error: Exception | None = None,
    ) -> None:
        self.returncode = returncode
        self.kill_error = kill_error
        self.kill_calls = 0

    def kill(self) -> None:
        self.kill_calls += 1
        if self.kill_error is not None:
            raise self.kill_error


class AbortRuntimeTransport:
    def __init__(self, process: AbortProcess, payload_path: Path | None) -> None:
        self._process = process
        self._auth_payload_path = payload_path


class AbortQuery:
    def __init__(self, transport: AbortRuntimeTransport) -> None:
        self.transport = transport


class AbortClient:
    def __init__(
        self,
        runtime_transport: AbortRuntimeTransport | None,
        *,
        query_present: bool,
    ) -> None:
        self._query = (
            AbortQuery(runtime_transport)
            if query_present and runtime_transport is not None
            else None
        )
        self._transport = runtime_transport if not query_present else None
        self.disconnect_calls = 0

    async def disconnect(self) -> None:
        self.disconnect_calls += 1


def _sdk_payload_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    auth_dir = tmp_path / "qoder-sdk-auth-test"
    auth_dir.mkdir()
    payload_path = auth_dir / "payload.json"
    payload_path.touch(mode=0o600)
    return payload_path


@pytest.mark.parametrize("query_present", (True, False))
async def test_sdk_abort_cleans_query_and_early_transport_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    query_present: bool,
) -> None:
    payload_path = _sdk_payload_path(tmp_path, monkeypatch)
    process = AbortProcess()
    runtime_transport = AbortRuntimeTransport(process, payload_path)
    client = AbortClient(runtime_transport, query_present=query_present)
    transport = QoderSDKTransport(client)

    transport.abort()
    await transport.disconnect()

    assert process.kill_calls == 1
    assert not payload_path.exists()
    assert not payload_path.parent.exists()
    assert runtime_transport._auth_payload_path is None
    assert client.disconnect_calls == 0


async def test_sdk_abort_does_not_kill_an_exited_process_and_cleans_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload_path = _sdk_payload_path(tmp_path, monkeypatch)
    process = AbortProcess(returncode=17)
    runtime_transport = AbortRuntimeTransport(process, payload_path)
    client = AbortClient(runtime_transport, query_present=True)
    transport = QoderSDKTransport(client)

    transport.abort()
    await transport.disconnect()

    assert process.kill_calls == 0
    assert not payload_path.exists()
    assert not payload_path.parent.exists()
    assert client.disconnect_calls == 0


async def test_sdk_abort_kill_failure_keeps_normal_cleanup_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload_path = _sdk_payload_path(tmp_path, monkeypatch)
    process = AbortProcess(kill_error=RuntimeError("kill failed"))
    runtime_transport = AbortRuntimeTransport(process, payload_path)
    client = AbortClient(runtime_transport, query_present=True)
    transport = QoderSDKTransport(client)

    transport.abort()
    await transport.disconnect()

    assert process.kill_calls == 1
    assert not payload_path.exists()
    assert not payload_path.parent.exists()
    assert client.disconnect_calls == 1


async def test_sdk_abort_without_a_safe_fallback_allows_later_disconnect() -> None:
    client = AbortClient(None, query_present=False)
    transport = QoderSDKTransport(client)

    transport.abort()
    await transport.disconnect()

    assert client.disconnect_calls == 1


def test_starting_worker_can_be_cancelled_but_not_completed() -> None:
    cancelled = WorkerStateReducer("starting")
    assert cancelled.transition("cancelled") == "cancelled"

    completed = WorkerStateReducer("starting")
    with pytest.raises(ValueError, match="starting -> completed"):
        completed.transition("completed")
