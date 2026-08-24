"""Async owner for live Qoder workers and their durable observations."""

import asyncio
from collections.abc import AsyncIterator
from typing import cast

from qworker.auditor import ForegroundAuditor, TransportFactory
from qworker.domain import AuditContract, AuditResult
from qworker.lifecycle import WorkerRecord
from qworker.store import EventRecord, JsonValue, WorkerStore

_TERMINAL_STATES = frozenset(("completed", "failed", "cancelled", "lost"))


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
    ) -> None:
        self._store = store
        self._transport_factory = transport_factory
        self._sdk_version = sdk_version
        self._runtime_path = runtime_path
        self._runtime_version = runtime_version
        self._settlement_timeout = settlement_timeout
        self._tasks: dict[str, asyncio.Task[None]] = {}
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
            self._run_worker(worker.worker_id, contract),
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

        worker = await self._require_worker(worker_id)
        events = await self._store.events_since(worker_id)
        cursor = events[-1].sequence if events else 0
        return _worker_json(worker, event_cursor=cursor)

    async def result(self, worker_id: str) -> dict[str, JsonValue] | None:
        """Return a durable structured result, or ``None`` while none exists."""

        worker = await self._require_worker(worker_id)
        return cast(dict[str, JsonValue] | None, worker.result_summary)

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
        tasks = tuple(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        for condition in self._event_conditions.values():
            async with condition:
                condition.notify_all()
        await self._store.close()

    async def _run_worker(self, worker_id: str, contract: AuditContract) -> None:
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
        auditor = ForegroundAuditor(
            self._transport_factory,
            settlement_timeout=self._settlement_timeout,
        )
        try:
            result = await auditor.run(contract)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 -- isolate an arbitrary transport failure
            result = _execution_failure(contract)

        if result.resolved_model is not None:
            await self._append_event(
                worker_id,
                "model.resolved",
                {"schema_version": 1, "model": result.resolved_model},
            )
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
        await self._append_event(
            worker_id,
            "worker.state_changed",
            {"schema_version": 1, "state": terminal_state},
        )

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

    def _task_finished(self, worker_id: str, completed: asyncio.Task[None]) -> None:
        self._tasks.pop(worker_id, None)
        if not completed.cancelled():
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
