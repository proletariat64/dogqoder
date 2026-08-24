"""Reduce normalized transport events into one auditor result."""

from typing import Literal, cast

from qworker.audit_report import AuditOutcome, parse_audit_report_text
from qworker.domain import AuditFinding, AuditResult
from qworker.events import (
    AdapterEvent,
    AssistantEvent,
    ResultEvent,
    SemanticEvent,
    SemanticEventKind,
    SystemEvent,
    TaskProgressEvent,
    TaskStartedEvent,
    TaskTerminalEvent,
)

_MAX_SEMANTIC_EVENTS = 128


class ResultReducer:
    """Accumulate main-result and nested-task completion state."""

    def __init__(self, *, requested_model: str, resolved_model: str | None) -> None:
        self._requested_model = requested_model
        self._resolved_model = resolved_model
        self._active_task_ids: set[str] = set()
        self._actual_models: list[str] = []
        self._result: ResultEvent | None = None
        self._semantic_events: list[SemanticEvent] = []

    @property
    def needs_settlement(self) -> bool:
        """Whether a main result arrived while nested tasks remain active."""

        return self._result is not None and bool(self._active_task_ids)

    @property
    def semantic_events(self) -> tuple[SemanticEvent, ...]:
        """Bounded credential-safe telemetry categories, in receive order."""

        return tuple(self._semantic_events)

    def apply(self, event: AdapterEvent) -> None:
        """Record one normalized event and update task-settlement state."""

        semantic_kinds = self._semantic_kinds(event)
        self._record(semantic_kinds)
        primary_kind = semantic_kinds[0]
        if primary_kind == "assistant":
            assistant_event = cast(AssistantEvent, event)
            self._record_model(assistant_event.model)
        elif primary_kind == "task_started":
            task_started_event = cast(TaskStartedEvent, event)
            self._active_task_ids.add(task_started_event.task_id)
        elif primary_kind == "task_terminal":
            task_terminal_event = cast(TaskTerminalEvent, event)
            self._active_task_ids.discard(task_terminal_event.task_id)
        elif primary_kind == "result":
            result_event = cast(ResultEvent, event)
            self._result = result_event
            for model in result_event.model_usage:
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

        parsed_report = parse_audit_report_text(result_event.result)
        warnings: list[str] = []
        if parsed_report is None:
            outcome: AuditOutcome = "failed" if result_event.is_error else "partial"
            summary = result_event.result or ""
            files: tuple[str, ...] = ()
            validation: tuple[str, ...] = ()
            risks: tuple[str, ...] = ()
            verdict: str | None = None
            confirmed: tuple[str, ...] = ()
            findings: tuple[AuditFinding, ...] = ()
            required_changes: tuple[str, ...] = ()
            warnings.append("report_contract_unparseable")
        else:
            outcome = parsed_report.outcome
            if result_event.is_error:
                outcome = "failed"
            summary = parsed_report.summary
            files = parsed_report.files
            validation = parsed_report.validation
            risks = parsed_report.risks
            verdict = parsed_report.verdict
            confirmed = parsed_report.confirmed
            findings = parsed_report.findings
            required_changes = parsed_report.required_changes

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
            verdict=verdict,
            confirmed=confirmed,
            findings=findings,
            required_changes=required_changes,
            actual_models=tuple(self._actual_models),
            session_id=result_event.session_id,
            nested_state=nested_state,
            warnings=tuple(warnings),
            errors=result_event.errors + result_event.permission_denials,
        )

    def _record(self, semantic_kinds: tuple[SemanticEventKind, ...]) -> None:
        for kind in semantic_kinds:
            if len(self._semantic_events) == _MAX_SEMANTIC_EVENTS:
                self._semantic_events.pop(0)
            self._semantic_events.append(SemanticEvent(kind=kind))

    @staticmethod
    def _semantic_kinds(event: AdapterEvent) -> tuple[SemanticEventKind, ...]:
        if isinstance(event, AssistantEvent):
            semantic_kinds: list[SemanticEventKind] = ["assistant"]
            semantic_kinds.extend("tool" for _ in event.tools)
            return tuple(semantic_kinds)
        if isinstance(event, TaskStartedEvent):
            return ("task_started",)
        if isinstance(event, TaskProgressEvent):
            return ("task_progress",)
        if isinstance(event, TaskTerminalEvent):
            return ("task_terminal",)
        if isinstance(event, SystemEvent):
            return ("system",)
        return ("result",)

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
