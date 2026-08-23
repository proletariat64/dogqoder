"""Reduce normalized transport events into one auditor result."""

import json
from dataclasses import dataclass
from typing import Literal, TypeGuard, cast

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

type AuditOutcome = Literal["completed", "partial", "blocked", "failed"]
_REPORT_OUTCOMES = frozenset(("completed", "partial", "blocked"))
_REPORT_KEYS = frozenset(
    (
        "outcome",
        "summary",
        "files",
        "validation",
        "risks",
        "verdict",
        "confirmed",
        "findings",
        "required_changes",
    )
)
_FINDING_KEYS = frozenset(("severity", "evidence", "affected_requirement_or_location"))
_MAX_SEMANTIC_EVENTS = 128


@dataclass(frozen=True, slots=True)
class _ParsedAuditReport:
    outcome: AuditOutcome
    summary: str
    files: tuple[str, ...]
    validation: tuple[str, ...]
    risks: tuple[str, ...]
    verdict: str
    confirmed: tuple[str, ...]
    findings: tuple[AuditFinding, ...]
    required_changes: tuple[str, ...]


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

        parsed_report = self._parse_report(result_event.result)
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

    def _parse_report(self, result_text: str | None) -> _ParsedAuditReport | None:
        if result_text is None:
            return None
        normalized_text = self._normalize_report_json(result_text)
        try:
            payload: object = json.loads(normalized_text)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict) or set(payload) != _REPORT_KEYS:
            return None

        outcome = payload.get("outcome")
        summary = payload.get("summary")
        files = payload.get("files")
        validation = payload.get("validation")
        risks = payload.get("risks")
        verdict = payload.get("verdict")
        confirmed = payload.get("confirmed")
        findings = self._parse_findings(payload.get("findings"))
        required_changes = payload.get("required_changes")
        if (
            not isinstance(outcome, str)
            or outcome not in _REPORT_OUTCOMES
            or not isinstance(summary, str)
            or not self._is_string_list(files)
            or not self._is_string_list(validation)
            or not self._is_string_list(risks)
            or not isinstance(verdict, str)
            or not self._is_string_list(confirmed)
            or findings is None
            or not self._is_string_list(required_changes)
        ):
            return None

        return _ParsedAuditReport(
            outcome=cast(AuditOutcome, outcome),
            summary=summary,
            files=tuple(files),
            validation=tuple(validation),
            risks=tuple(risks),
            verdict=verdict,
            confirmed=tuple(confirmed),
            findings=findings,
            required_changes=tuple(required_changes),
        )

    @staticmethod
    def _normalize_report_json(result_text: str) -> str:
        candidate = result_text.strip()
        fence_prefix = "```json\n"
        fence_suffix = "\n```"
        if (
            candidate.startswith(fence_prefix)
            and candidate.endswith(fence_suffix)
            and candidate.count("```") == 2
        ):
            return candidate[len(fence_prefix) : -len(fence_suffix)]
        return result_text

    @staticmethod
    def _parse_findings(value: object) -> tuple[AuditFinding, ...] | None:
        if not isinstance(value, list):
            return None
        findings: list[AuditFinding] = []
        for item in value:
            if not isinstance(item, dict) or set(item) != _FINDING_KEYS:
                return None
            severity = item.get("severity")
            evidence = item.get("evidence")
            affected = item.get("affected_requirement_or_location")
            if (
                not isinstance(severity, str)
                or not isinstance(evidence, str)
                or (affected is not None and not isinstance(affected, str))
            ):
                return None
            findings.append(
                AuditFinding(
                    severity=severity,
                    evidence=evidence,
                    affected_requirement_or_location=affected,
                )
            )
        return tuple(findings)

    @staticmethod
    def _is_string_list(value: object) -> TypeGuard[list[str]]:
        return isinstance(value, list) and all(isinstance(item, str) for item in value)
