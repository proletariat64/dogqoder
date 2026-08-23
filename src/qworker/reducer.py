"""Reduce normalized transport events into one auditor result."""

import json
from typing import Literal, TypeGuard, cast

from qworker.domain import AuditResult
from qworker.events import (
    AdapterEvent,
    AssistantEvent,
    ResultEvent,
    TaskStartedEvent,
    TaskTerminalEvent,
)

type AuditOutcome = Literal["completed", "partial", "blocked", "failed"]
_REPORT_OUTCOMES = frozenset(("completed", "partial", "blocked"))
_MAX_SEMANTIC_EVENTS = 128


class ResultReducer:
    """Accumulate main-result and nested-task completion state."""

    def __init__(self, *, requested_model: str, resolved_model: str | None) -> None:
        self._requested_model = requested_model
        self._resolved_model = resolved_model
        self._active_task_ids: set[str] = set()
        self._actual_models: list[str] = []
        self._result: ResultEvent | None = None
        self._semantic_events: list[AdapterEvent] = []

    @property
    def needs_settlement(self) -> bool:
        """Whether a main result arrived while nested tasks remain active."""

        return self._result is not None and bool(self._active_task_ids)

    @property
    def semantic_events(self) -> tuple[AdapterEvent, ...]:
        """Bounded normalized telemetry, in receive order."""

        return tuple(self._semantic_events)

    def apply(self, event: AdapterEvent) -> None:
        """Record one normalized event and update task-settlement state."""

        self._record(event)
        if isinstance(event, AssistantEvent):
            self._record_model(event.model)
        elif isinstance(event, TaskStartedEvent):
            self._active_task_ids.add(event.task_id)
        elif isinstance(event, TaskTerminalEvent):
            self._active_task_ids.discard(event.task_id)
        elif isinstance(event, ResultEvent):
            self._result = event
            for model in event.model_usage:
                self._record_model(model)

    def finish(self, settlement_expired: bool) -> AuditResult:
        """Return the best available result without waiting for wall-clock time."""

        result_event = self._result
        if result_event is None:
            return AuditResult(
                outcome="failed",
                summary="No result was received.",
                files=(),
                validation=(),
                risks=(),
                requested_model=self._requested_model,
                resolved_model=self._resolved_model,
                actual_models=tuple(self._actual_models),
                nested_state=self._nested_state(settlement_expired),
                errors=("result_missing",),
            )

        target_outcome: AuditOutcome = (
            "failed" if result_event.is_error else "completed"
        )
        parsed_report = self._parse_report(result_event.result)
        warnings: list[str] = []
        if parsed_report is None:
            outcome = target_outcome
            summary = result_event.result or ""
            files: tuple[str, ...] = ()
            validation: tuple[str, ...] = ()
            risks: tuple[str, ...] = ()
            warnings.append("report_contract_unparseable")
        else:
            outcome, summary, files, validation, risks = parsed_report
            if result_event.is_error:
                outcome = "failed"

        nested_state = self._nested_state(settlement_expired)
        if nested_state == "unknown":
            warnings.append("nested_terminal_event_missing")

        return AuditResult(
            outcome=outcome,
            summary=summary,
            files=files,
            validation=validation,
            risks=risks,
            requested_model=self._requested_model,
            resolved_model=self._resolved_model,
            actual_models=tuple(self._actual_models),
            session_id=result_event.session_id,
            nested_state=nested_state,
            warnings=tuple(warnings),
            errors=result_event.errors + result_event.permission_denials,
        )

    def _record(self, event: AdapterEvent) -> None:
        if len(self._semantic_events) == _MAX_SEMANTIC_EVENTS:
            self._semantic_events.pop(0)
        self._semantic_events.append(event)

    def _record_model(self, model: str | None) -> None:
        if model is not None and model not in self._actual_models:
            self._actual_models.append(model)

    def _nested_state(
        self, settlement_expired: bool
    ) -> Literal["none", "active", "settled", "unknown"]:
        if self._result is None:
            return "active" if self._active_task_ids else "none"
        if not self._active_task_ids:
            return "settled"
        return "unknown" if settlement_expired else "active"

    def _parse_report(
        self, result_text: str | None
    ) -> (
        tuple[AuditOutcome, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]
        | None
    ):
        if result_text is None:
            return None
        try:
            payload: object = json.loads(result_text)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None

        outcome = payload.get("outcome")
        summary = payload.get("summary")
        files = payload.get("files")
        validation = payload.get("validation")
        risks = payload.get("risks")
        if (
            not isinstance(outcome, str)
            or outcome not in _REPORT_OUTCOMES
            or not isinstance(summary, str)
            or not self._is_string_list(files)
            or not self._is_string_list(validation)
            or not self._is_string_list(risks)
        ):
            return None

        return (
            cast(AuditOutcome, outcome),
            summary,
            tuple(files),
            tuple(validation),
            tuple(risks),
        )

    @staticmethod
    def _is_string_list(value: object) -> TypeGuard[list[str]]:
        return isinstance(value, list) and all(isinstance(item, str) for item in value)
