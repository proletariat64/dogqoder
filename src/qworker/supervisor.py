"""Async owner for live Qoder workers and their durable observations."""

import asyncio
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from qworker.auditor import ForegroundAuditor, TransportFactory
from qworker.control import (
    ApprovalDecision,
    ApprovalKind,
    ControlCallbacks,
    ElicitationDecision,
    ElicitationRequest,
    ElicitationSchemaSnapshot,
    PendingApproval,
    PermissionDecision,
    PermissionRequest,
    SteeringPriority,
    approval_display,
    new_request_id,
    parse_approval_response,
    snapshot_elicitation_schema,
)
from qworker.domain import AuditContract, AuditResult
from qworker.lifecycle import WorkerRecord
from qworker.store import AttemptChangedError, EventRecord, JsonValue, WorkerStore
from qworker.transport import QoderTransport

_TERMINAL_STATES = frozenset(("completed", "failed", "cancelled", "lost"))
_STEERING_PRIORITIES = frozenset(("now", "next", "later"))
_MAX_STOP_TIMEOUT = 10.0


@dataclass(frozen=True, slots=True)
class _LiveAttempt:
    attempt: int
    transport: QoderTransport


@dataclass(frozen=True, slots=True)
class _StopOperation:
    task: asyncio.Task[None]
    escalation: asyncio.Event


class SupervisorError(Exception):
    """Stable supervisor error suitable for an RPC response."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class Supervisor:
    """Sole owner of live worker tasks and their transport factories."""

    def __init__(
        self,
        store: WorkerStore,
        transport_factory: TransportFactory,
        *,
        sdk_version: str,
        runtime_path: str = "bundled",
        runtime_version: str | None = None,
        settlement_timeout: float = 5.0,
        stop_timeout: float = _MAX_STOP_TIMEOUT,
    ) -> None:
        if stop_timeout < 0:
            raise ValueError("stop_timeout must be non-negative.")
        self._store = store
        self._transport_factory = transport_factory
        self._sdk_version = sdk_version
        self._runtime_path = runtime_path
        self._runtime_version = runtime_version
        self._settlement_timeout = settlement_timeout
        self._stop_timeout = min(stop_timeout, _MAX_STOP_TIMEOUT)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._live_attempts: dict[str, _LiveAttempt] = {}
        self._result_phases: dict[tuple[str, int], asyncio.Task[None]] = {}
        self._stop_operations: dict[tuple[str, int], _StopOperation] = {}
        self._stop_requests: set[tuple[str, int]] = set()
        self._pending_approvals: dict[str, PendingApproval] = {}
        self._control_lock = asyncio.Lock()
        self._event_conditions: dict[str, asyncio.Condition] = {}
        self._closed = False

    async def spawn(self, contract: AuditContract) -> dict[str, JsonValue]:
        """Durably accept an audit and return before its transport completes."""

        if self._closed:
            raise SupervisorError("supervisor_unavailable", "Supervisor is closed.")
        worker = await self._store.create_worker(
            role="auditor",
            cwd=contract.cwd,
            write_capability="read_only",
            requested_model=contract.requested_model,
            runtime_path=self._runtime_path,
            runtime_version=self._runtime_version,
            sdk_version=self._sdk_version,
        )
        task = asyncio.create_task(
            self._run_worker(worker.worker_id, worker.attempt, contract),
            name=f"qworker:{worker.worker_id}",
        )
        self._tasks[worker.worker_id] = task

        def task_finished(completed: asyncio.Task[None]) -> None:
            self._task_finished(worker.worker_id, completed)

        task.add_done_callback(task_finished)
        return {
            "worker_id": worker.worker_id,
            "state": worker.state,
            "role": worker.role,
            "cwd": str(worker.cwd),
            "event_cursor": 1,
        }

    async def status(self, worker_id: str) -> dict[str, JsonValue]:
        """Return durable current worker state and latest persisted cursor."""

        async with self._control_lock:
            worker = await self._require_worker(worker_id)
            cursor = await self._store.latest_event_cursor(worker_id)
            status = _worker_json(worker, event_cursor=cursor)
            status["pending_approvals"] = [
                _copy_display(pending.display)
                for pending in self._pending_approvals.values()
                if pending.worker_id == worker_id
                and pending.attempt == worker.attempt
                and worker.state == "requires_action"
            ]
            return status

    async def result(self, worker_id: str) -> dict[str, JsonValue] | None:
        """Return a durable structured result, or ``None`` while none exists."""

        worker = await self._require_worker(worker_id)
        return cast(dict[str, JsonValue] | None, worker.result_summary)

    async def steer(
        self,
        worker_id: str,
        message: str,
        *,
        priority: SteeringPriority = "next",
        agent_id: str | None = None,
    ) -> dict[str, JsonValue]:
        """Deliver one UUID-stamped message to a live top-level worker."""

        if not message:
            raise SupervisorError("invalid_request", "Steering message must not be empty.")
        if priority not in _STEERING_PRIORITIES:
            raise SupervisorError("invalid_request", "Unknown steering priority.")
        if agent_id is not None:
            raise SupervisorError(
                "unsupported_operation",
                "Selected nested-agent steering is not supported.",
            )
        live = await self._require_live_attempt(worker_id)
        message_id = str(uuid.uuid4())
        await self._append_event(
            worker_id,
            "steer.queued",
            {
                "schema_version": 1,
                "message_id": message_id,
                "priority": priority,
            },
        )
        try:
            await live.transport.steer(
                message,
                priority=priority,
                message_id=message_id,
            )
        except Exception:  # noqa: BLE001 -- transport details are not an RPC surface
            raise SupervisorError(
                "sdk_protocol_error", "Unable to deliver steering message."
            ) from None
        await self._append_event(
            worker_id,
            "steer.delivered",
            {"schema_version": 1, "message_id": message_id},
        )
        return {
            "message_id": message_id,
            "priority": priority,
            "accepted": True,
        }

    async def cancel_message(
        self, worker_id: str, message_id: str
    ) -> dict[str, JsonValue]:
        """Attempt UUID cancellation without strengthening the SDK boolean."""

        _validated_message_id(message_id)
        live = await self._require_live_attempt(worker_id)
        try:
            cancelled = await live.transport.cancel_message(message_id)
        except Exception:  # noqa: BLE001 -- transport details are not an RPC surface
            raise SupervisorError(
                "sdk_protocol_error", "Unable to cancel steering message."
            ) from None
        await self._append_event(
            worker_id,
            "steer.cancelled",
            {
                "schema_version": 1,
                "message_id": message_id,
                "cancelled": cancelled,
            },
        )
        return {"message_id": message_id, "cancelled": cancelled}

    async def stop(
        self,
        worker_id: str,
        *,
        force: bool = False,
        agent_id: str | None = None,
    ) -> dict[str, JsonValue]:
        """Stop one top-level attempt without changing its workspace."""

        if agent_id is not None:
            raise SupervisorError(
                "unsupported_operation",
                "Selected nested-agent stop is not supported.",
            )
        async with self._control_lock:
            worker = await self._require_worker(worker_id)
            if worker.state in _TERMINAL_STATES:
                cursor = await self._store.latest_event_cursor(worker_id)
                response = _worker_json(worker, event_cursor=cursor)
                response["pending_approvals"] = []
                response["force"] = force
                return response
            task = self._tasks.get(worker_id)
            if task is None:
                raise SupervisorError("worker_not_live", "Worker has no live attempt.")
            attempt = worker.attempt
            stop_key = (worker_id, attempt)
            operation = self._stop_operations.get(stop_key)
            if operation is None:
                self._stop_requests.add(stop_key)
                live = self._live_attempts.get(worker_id)
                if live is not None and live.attempt != attempt:
                    live = None
                escalation = asyncio.Event()
                if force:
                    escalation.set()
                operation_task = asyncio.create_task(
                    self._stop_attempt(worker_id, attempt, task, live, escalation),
                    name=f"qworker-stop:{worker_id}",
                )
                operation = _StopOperation(operation_task, escalation)
                self._stop_operations[stop_key] = operation

                def operation_finished(completed: asyncio.Task[None]) -> None:
                    if self._stop_operations.get(stop_key) is operation:
                        self._stop_operations.pop(stop_key, None)
                    if not completed.cancelled():
                        with suppress(asyncio.CancelledError):
                            completed.exception()

                operation_task.add_done_callback(operation_finished)
            elif force:
                operation.escalation.set()

        await asyncio.shield(operation.task)
        status = await self.status(worker_id)
        status["force"] = force
        return status

    async def _stop_attempt(
        self,
        worker_id: str,
        attempt: int,
        task: asyncio.Task[None],
        live: _LiveAttempt | None,
        escalation: asyncio.Event,
    ) -> None:
        """Run the sole stop operation for one attempt; concurrent callers join it."""

        stop_key = (worker_id, attempt)
        deadline = asyncio.get_running_loop().time() + self._stop_timeout
        if await self._join_result_phase(stop_key, task):
            await self._finalize_without_result(
                worker_id,
                attempt,
                missing_state="cancelled",
            )
            self._stop_requests.discard(stop_key)
            return

        if live is None and not escalation.is_set():
            await asyncio.sleep(0)
            current = self._live_attempts.get(worker_id)
            if current is not None and current.attempt == attempt:
                live = current

        terminate = escalation.is_set()
        if not terminate and live is not None:
            terminate = await self._wait_for_graceful_stop(
                live.transport,
                task,
                escalation,
                deadline,
            )
        elif not terminate:
            terminate = True

        if terminate:
            if live is not None:
                await self._disconnect_before(live.transport, deadline)
            result_joined = await self._join_result_phase(stop_key, task)
            if not result_joined:
                await self._cancel_owner(stop_key, task)

        await self._join_result_phase(stop_key, task)
        await self._finalize_without_result(
            worker_id,
            attempt,
            missing_state="cancelled",
        )
        self._stop_requests.discard(stop_key)

    async def _wait_for_graceful_stop(
        self,
        transport: QoderTransport,
        owner: asyncio.Task[None],
        escalation: asyncio.Event,
        deadline: float,
    ) -> bool:
        """Return whether teardown is needed, with force able to preempt grace."""

        interrupt = asyncio.create_task(transport.interrupt())
        escalation_wait = asyncio.create_task(escalation.wait())
        interrupt_consumed = False
        try:
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            done, _pending = await asyncio.wait(
                (interrupt, escalation_wait),
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if escalation_wait in done:
                return True
            if interrupt not in done:
                return True
            try:
                await interrupt
                interrupt_consumed = True
            except asyncio.CancelledError:
                interrupt_consumed = True
                return True
            except Exception:  # noqa: BLE001 -- close after any interrupt failure
                interrupt_consumed = True
                return True
            if escalation.is_set():
                return True
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            done, _pending = await asyncio.wait(
                (owner, escalation_wait),
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            return escalation_wait in done or owner not in done
        finally:
            escalation_wait.cancel()
            if not interrupt.done():
                interrupt.cancel()
            if not interrupt_consumed:
                interrupt.add_done_callback(_consume_task_exception)

    async def _disconnect_before(
        self,
        transport: QoderTransport,
        deadline: float,
    ) -> None:
        """Bound graceful teardown, then synchronously request the safest abort."""

        try:
            async with asyncio.timeout_at(deadline):
                await transport.disconnect()
        except Exception:  # noqa: BLE001 -- abort is the fail-closed teardown path
            with suppress(Exception):
                transport.abort()

    async def _cancel_owner(
        self,
        stop_key: tuple[str, int],
        owner: asyncio.Task[None],
    ) -> None:
        """Cancel owner cleanup, rechecking for an accepted result before escalation."""

        if owner.done():
            return
        owner.cancel()
        for _ in range(3):
            await asyncio.sleep(0)
            if await self._join_result_phase(stop_key, owner):
                return
            if owner.done():
                break
        if not owner.done():
            owner.cancel()
        await asyncio.gather(owner, return_exceptions=True)

    async def _join_result_phase(
        self,
        stop_key: tuple[str, int],
        owner: asyncio.Task[None],
    ) -> bool:
        """Join an accepted-result phase and its owner without propagating cancellation."""

        phase = self._result_phases.get(stop_key)
        if phase is None:
            return False
        await asyncio.gather(asyncio.shield(phase), return_exceptions=True)
        await asyncio.gather(asyncio.shield(owner), return_exceptions=True)
        return True

    async def respond(
        self,
        worker_id: str,
        request_id: str,
        response: dict[str, JsonValue],
    ) -> dict[str, JsonValue]:
        """Resolve one callback future only for its current live attempt."""

        async with self._control_lock:
            pending = self._pending_approvals.get(request_id)
            if pending is None or pending.worker_id != worker_id:
                raise SupervisorError(
                    "approval_not_pending", "Approval request is not pending."
                )
            worker = await self._require_worker(worker_id)
            live = self._live_attempts.get(worker_id)
            if (
                worker.attempt != pending.attempt
                or worker.state != "requires_action"
                or live is None
                or live.attempt != pending.attempt
            ):
                raise SupervisorError(
                    "worker_not_live",
                    "Approval belongs to an attempt that is no longer live.",
                )
            try:
                decision = parse_approval_response(
                    pending.request,
                    response,
                    schema_snapshot=pending.schema_snapshot,
                )
            except (TypeError, ValueError) as error:
                raise SupervisorError("invalid_request", str(error)) from None
            status = _approval_status(decision)
            other_pending = any(
                item.request_id != request_id
                and item.worker_id == worker_id
                and item.attempt == pending.attempt
                for item in self._pending_approvals.values()
            )
            try:
                await self._store.record_approval_resolution(
                    worker_id,
                    expected_attempt=pending.attempt,
                    payload={
                        "schema_version": 1,
                        "request_id": request_id,
                        "status": status,
                    },
                    restore_running=not other_pending,
                )
            except AttemptChangedError:
                raise SupervisorError(
                    "worker_not_live",
                    "Approval belongs to an attempt that is no longer live.",
                ) from None
            await self._notify_event(worker_id)
            self._pending_approvals.pop(request_id, None)
            if not pending.future.done():
                pending.future.set_result(decision)
            return {"request_id": request_id, "status": status}

    async def watch(
        self,
        worker_id: str,
        *,
        since: int = 0,
        follow: bool = False,
    ) -> AsyncIterator[dict[str, JsonValue]]:
        """Replay persisted events, optionally following without cursor loss."""

        if since < 0:
            raise SupervisorError(
                "invalid_request", "Event cursor must be non-negative."
            )
        await self._require_worker(worker_id)
        cursor = since
        condition = self._event_conditions.setdefault(worker_id, asyncio.Condition())
        while True:
            if not follow:
                events = await self._store.events_since(worker_id, since=cursor)
            else:
                async with condition:
                    events = await self._store.events_since(worker_id, since=cursor)
                    if not events:
                        if self._closed:
                            return
                        worker = await self._require_worker(worker_id)
                        if worker.state in _TERMINAL_STATES:
                            return
                        await condition.wait()
                        continue
            if not events:
                return
            for event in events:
                cursor = event.sequence
                yield _event_json(event)
            if not follow:
                return

    async def close(self) -> None:
        """Cancel live worker tasks and close supervisor-owned durable state."""

        self._closed = True
        for worker_id in tuple(self._tasks):
            with suppress(SupervisorError):
                await self.stop(worker_id, force=True)
        tasks = tuple(self._tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        for condition in self._event_conditions.values():
            async with condition:
                condition.notify_all()
        await self._store.close()

    async def _run_worker(
        self, worker_id: str, attempt: int, contract: AuditContract
    ) -> None:
        initialized = False

        def owned_transport_factory(cwd: Path) -> QoderTransport:
            transport = self._transport_factory(cwd)

            async def request_permission(
                request: PermissionRequest,
            ) -> PermissionDecision:
                decision = await self._request_approval(
                    worker_id, attempt, request
                )
                return cast(PermissionDecision, decision)

            async def request_elicitation(
                request: ElicitationRequest,
            ) -> ElicitationDecision:
                decision = await self._request_approval(
                    worker_id, attempt, request
                )
                return cast(ElicitationDecision, decision)

            transport.bind_control(
                ControlCallbacks(
                    request_permission=request_permission,
                    request_elicitation=request_elicitation,
                )
            )
            self._live_attempts[worker_id] = _LiveAttempt(attempt, transport)
            return transport

        async def on_initialized(resolved_model: str) -> None:
            nonlocal initialized
            await self._append_event(
                worker_id,
                "model.resolved",
                {"schema_version": 1, "model": resolved_model},
            )
            await self._append_event(
                worker_id,
                "worker.state_changed",
                {"schema_version": 1, "state": "running"},
            )
            await self._append_event(
                worker_id,
                "worker.health_changed",
                {"schema_version": 1, "health": "healthy"},
            )
            initialized = True

        auditor = ForegroundAuditor(
            owned_transport_factory,
            settlement_timeout=self._settlement_timeout,
            on_initialized=on_initialized,
        )
        try:
            result = await auditor.run(contract)
        except asyncio.CancelledError:
            missing_state: Literal["cancelled", "lost"] = (
                "cancelled"
                if (worker_id, attempt) in self._stop_requests
                else "lost"
            )
            await self._finalize_without_result(
                worker_id,
                attempt,
                missing_state=missing_state,
            )
            raise
        except (BrokenPipeError, ConnectionResetError, EOFError):
            missing_state = (
                "cancelled"
                if (worker_id, attempt) in self._stop_requests
                else "lost"
            )
            await self._finalize_without_result(
                worker_id,
                attempt,
                missing_state=missing_state,
            )
            return
        except Exception:  # noqa: BLE001 -- isolate an arbitrary transport failure
            result = _execution_failure(contract)

        if "result_missing" in result.errors:
            missing_state = (
                "cancelled"
                if (worker_id, attempt) in self._stop_requests
                else "lost"
            )
            await self._finalize_without_result(
                worker_id,
                attempt,
                missing_state=missing_state,
            )
            return

        result_key = (worker_id, attempt)
        result_phase = asyncio.create_task(
            self._persist_accepted_result(worker_id, attempt, result, initialized),
            name=f"qworker-result:{worker_id}",
        )
        self._result_phases[result_key] = result_phase

        def result_finished(completed: asyncio.Task[None]) -> None:
            if self._result_phases.get(result_key) is result_phase:
                self._result_phases.pop(result_key, None)
            if not completed.cancelled():
                with suppress(asyncio.CancelledError):
                    completed.exception()

        result_phase.add_done_callback(result_finished)
        while not result_phase.done():
            try:
                await asyncio.shield(result_phase)
            except asyncio.CancelledError:
                continue
        await result_phase

    async def _persist_accepted_result(
        self,
        worker_id: str,
        attempt: int,
        result: AuditResult,
        initialized: bool,
    ) -> None:
        """Durably commit an accepted result through its result-derived terminal state."""

        await self._complete_live_attempt(worker_id, attempt)
        await self._store.record_result(
            worker_id,
            outcome=result.outcome,
            result_summary=_result_json(result),
            resolved_model=result.resolved_model,
            actual_models=result.actual_models,
            session_id=result.session_id,
            nested_state=result.nested_state,
            warnings=result.warnings,
        )
        await self._notify_event(worker_id)
        for warning in result.warnings:
            await self._append_event(
                worker_id,
                "worker.warning",
                {"schema_version": 1, "code": warning},
            )
        terminal_state = "failed" if result.outcome == "failed" else "completed"
        if initialized:
            await self._append_event(
                worker_id,
                "worker.health_changed",
                {"schema_version": 1, "health": "exited"},
            )
        await self._append_event(
            worker_id,
            "worker.state_changed",
            {"schema_version": 1, "state": terminal_state},
        )
        self._stop_requests.discard((worker_id, attempt))

    async def _append_event(
        self, worker_id: str, event_type: str, payload: dict[str, JsonValue]
    ) -> EventRecord:
        event = await self._store.append_event(worker_id, event_type, payload)
        await self._notify_event(worker_id)
        return event

    async def _notify_event(self, worker_id: str) -> None:
        condition = self._event_conditions.setdefault(worker_id, asyncio.Condition())
        async with condition:
            condition.notify_all()

    async def _require_worker(self, worker_id: str) -> WorkerRecord:
        worker = await self._store.get_worker(worker_id)
        if worker is None:
            raise SupervisorError("worker_not_found", f"Unknown worker: {worker_id}")
        return worker

    async def _require_live_attempt(self, worker_id: str) -> _LiveAttempt:
        if self._closed:
            raise SupervisorError("supervisor_unavailable", "Supervisor is closed.")
        worker = await self._require_worker(worker_id)
        live = self._live_attempts.get(worker_id)
        if (
            live is None
            or live.attempt != worker.attempt
            or worker.state not in ("running", "requires_action")
        ):
            raise SupervisorError("worker_not_live", "Worker has no live attempt.")
        return live

    async def _request_approval(
        self,
        worker_id: str,
        attempt: int,
        request: PermissionRequest | ElicitationRequest,
    ) -> ApprovalDecision:
        default = (
            PermissionDecision("deny")
            if isinstance(request, PermissionRequest)
            else ElicitationDecision("cancel")
        )
        async with self._control_lock:
            worker = await self._store.get_worker(worker_id)
            live = self._live_attempts.get(worker_id)
            if (
                self._closed
                or worker is None
                or worker.attempt != attempt
                or worker.state not in ("running", "requires_action")
                or live is None
                or live.attempt != attempt
            ):
                return default
            kind: ApprovalKind = (
                "tool_permission"
                if isinstance(request, PermissionRequest)
                else "elicitation"
            )
            schema_snapshot: ElicitationSchemaSnapshot | None = None
            if isinstance(request, ElicitationRequest):
                try:
                    schema_snapshot = snapshot_elicitation_schema(
                        request.requested_schema
                    )
                except (TypeError, ValueError):
                    return default
                request = ElicitationRequest(
                    server_name=request.server_name,
                    mode=request.mode,
                    display_message=request.display_message,
                )
            request_id = new_request_id()
            display = approval_display(
                request_id,
                attempt,
                request,
                schema_snapshot=schema_snapshot,
            )
            if display is None:
                return default
            pending = PendingApproval(
                request_id=request_id,
                worker_id=worker_id,
                attempt=attempt,
                kind=kind,
                request=request,
                schema_snapshot=schema_snapshot,
                display=display,
                future=asyncio.get_running_loop().create_future(),
            )
            self._pending_approvals[request_id] = pending
            payload = {"schema_version": 1, **display}
            payload.pop("attempt")
            try:
                await self._store.record_approval_request(
                    worker_id,
                    expected_attempt=attempt,
                    payload=payload,
                )
                await self._notify_event(worker_id)
            except AttemptChangedError:
                self._pending_approvals.pop(request_id, None)
                return default
            except BaseException:
                self._pending_approvals.pop(request_id, None)
                raise
        try:
            return await pending.future
        finally:
            if pending.future.cancelled():
                async with self._control_lock:
                    if self._pending_approvals.get(request_id) is pending:
                        self._pending_approvals.pop(request_id, None)

    async def _detach_live_attempt(self, worker_id: str, attempt: int) -> None:
        async with self._control_lock:
            current = self._live_attempts.get(worker_id)
            if current is not None and current.attempt == attempt:
                self._live_attempts.pop(worker_id, None)

    async def _complete_live_attempt(self, worker_id: str, attempt: int) -> None:
        async with self._control_lock:
            current = self._live_attempts.get(worker_id)
            if current is None or current.attempt != attempt:
                return
            pending = tuple(
                item
                for item in self._pending_approvals.values()
                if item.worker_id == worker_id and item.attempt == attempt
            )
            if pending:
                try:
                    await self._store.expire_approval_requests(
                        worker_id,
                        expected_attempt=attempt,
                        request_ids=tuple(item.request_id for item in pending),
                    )
                except AttemptChangedError:
                    pass
                else:
                    await self._notify_event(worker_id)
                for item in pending:
                    self._pending_approvals.pop(item.request_id, None)
                    if not item.future.done():
                        decision: ApprovalDecision = (
                            PermissionDecision("deny")
                            if item.kind == "tool_permission"
                            else ElicitationDecision("cancel")
                        )
                        item.future.set_result(decision)
            self._live_attempts.pop(worker_id, None)

    async def _finalize_without_result(
        self,
        worker_id: str,
        attempt: int,
        *,
        missing_state: Literal["cancelled", "lost"],
    ) -> None:
        """Close attempt controls, then preserve a result or classify its absence."""

        await self._complete_live_attempt(worker_id, attempt)
        worker = await self._require_worker(worker_id)
        if worker.attempt != attempt or worker.state in _TERMINAL_STATES:
            return
        target: Literal["completed", "failed", "cancelled", "lost"] = missing_state
        if worker.result_summary is not None:
            target = (
                "failed"
                if worker.result_summary.get("outcome") == "failed"
                else "completed"
            )
        if worker.health != "exited":
            await self._append_event(
                worker_id,
                "worker.health_changed",
                {"schema_version": 1, "health": "exited"},
            )
        current = await self._require_worker(worker_id)
        if current.attempt == attempt and current.state not in _TERMINAL_STATES:
            await self._append_event(
                worker_id,
                "worker.state_changed",
                {"schema_version": 1, "state": target},
            )

    def _task_finished(self, worker_id: str, completed: asyncio.Task[None]) -> None:
        self._tasks.pop(worker_id, None)
        if not completed.cancelled():
            with suppress(asyncio.CancelledError):
                completed.exception()


def _consume_task_exception(completed: asyncio.Task[None]) -> None:
    if not completed.cancelled():
        with suppress(asyncio.CancelledError):
            completed.exception()


def _worker_json(worker: WorkerRecord, *, event_cursor: int) -> dict[str, JsonValue]:
    return {
        "worker_id": worker.worker_id,
        "role": worker.role,
        "cwd": str(worker.cwd),
        "state": worker.state,
        "health": worker.health,
        "write_capability": worker.write_capability,
        "requested_model": worker.requested_model,
        "resolved_model": worker.resolved_model,
        "actual_models": list(worker.actual_models),
        "session_id": worker.session_id,
        "attempt": worker.attempt,
        "runtime_path": worker.runtime_path,
        "runtime_version": worker.runtime_version,
        "sdk_version": worker.sdk_version,
        "created_at": worker.created_at.isoformat(),
        "started_at": worker.started_at.isoformat() if worker.started_at else None,
        "last_event_at": (
            worker.last_event_at.isoformat() if worker.last_event_at else None
        ),
        "ended_at": worker.ended_at.isoformat() if worker.ended_at else None,
        "result_summary": cast(dict[str, JsonValue] | None, worker.result_summary),
        "nested_state": worker.nested_state,
        "warnings": list(worker.warnings),
        "event_cursor": event_cursor,
    }


def _event_json(event: EventRecord) -> dict[str, JsonValue]:
    return {
        "sequence": event.sequence,
        "event_id": event.event_id,
        "worker_id": event.worker_id,
        "attempt": event.attempt,
        "timestamp": event.timestamp.isoformat(),
        "type": event.type,
        "payload": event.payload,
    }


def _copy_display(display: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    return {
        key: list(value) if isinstance(value, list) else value
        for key, value in display.items()
    }


def _result_json(result: AuditResult) -> dict[str, JsonValue]:
    findings: list[JsonValue] = [
        {
            "severity": finding.severity,
            "evidence": finding.evidence,
            "affected_requirement_or_location": (
                finding.affected_requirement_or_location
            ),
        }
        for finding in result.findings
    ]
    return {
        "outcome": result.outcome,
        "summary": result.summary,
        "files": list(result.files),
        "validation": list(result.validation),
        "risks": list(result.risks),
        "requested_model": result.requested_model,
        "resolved_model": result.resolved_model,
        "verdict": result.verdict,
        "confirmed": list(result.confirmed),
        "findings": findings,
        "required_changes": list(result.required_changes),
        "actual_models": list(result.actual_models),
        "session_id": result.session_id,
        "nested_state": result.nested_state,
        "warnings": list(result.warnings),
        "errors": list(result.errors),
    }


def _execution_failure(contract: AuditContract) -> AuditResult:
    return AuditResult(
        outcome="failed",
        summary="Worker execution failed.",
        files=(),
        validation=(),
        risks=(),
        requested_model=contract.requested_model,
        resolved_model=None,
        errors=("sdk_protocol_error",),
    )


def _validated_message_id(message_id: str) -> None:
    try:
        parsed = uuid.UUID(message_id)
    except (AttributeError, ValueError):
        raise SupervisorError(
            "invalid_request", "message_id must be a canonical UUID."
        ) from None
    if str(parsed) != message_id:
        raise SupervisorError(
            "invalid_request", "message_id must be a canonical UUID."
        )


def _approval_status(decision: ApprovalDecision) -> str:
    if isinstance(decision, PermissionDecision):
        return "allowed" if decision.action == "allow" else "denied"
    return "answered" if decision.action == "accept" else "denied"
