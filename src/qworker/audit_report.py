"""Strict host-owned contract for auditor report submission."""

import json
from dataclasses import dataclass
from typing import Any, Literal, TypeGuard, cast

from qworker.domain import AuditFinding

AUDIT_REPORT_MCP_SERVER = "qworker_audit"
SUBMIT_AUDIT_TOOL_NAME = "submit_audit"
SUBMIT_AUDIT_TOOL = f"mcp__{AUDIT_REPORT_MCP_SERVER}__{SUBMIT_AUDIT_TOOL_NAME}"
type AuditOutcome = Literal["completed", "partial", "blocked", "failed"]
AUDIT_REPORT_OUTCOMES = frozenset(("completed", "partial", "blocked"))
AUDIT_REPORT_KEYS = frozenset(
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
AUDIT_FINDING_KEYS = frozenset(
    ("severity", "evidence", "affected_requirement_or_location")
)

_STRING_ARRAY = {"type": "array", "items": {"type": "string"}}
AUDIT_REPORT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "outcome": {
            "type": "string",
            "enum": sorted(AUDIT_REPORT_OUTCOMES),
        },
        "summary": {"type": "string"},
        "files": _STRING_ARRAY,
        "validation": _STRING_ARRAY,
        "risks": _STRING_ARRAY,
        "verdict": {"type": "string"},
        "confirmed": _STRING_ARRAY,
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "severity": {"type": "string"},
                    "evidence": {"type": "string"},
                    "affected_requirement_or_location": {
                        "anyOf": [{"type": "string"}, {"type": "null"}]
                    },
                },
                "required": [
                    "severity",
                    "evidence",
                    "affected_requirement_or_location",
                ],
                "additionalProperties": False,
            },
        },
        "required_changes": _STRING_ARRAY,
    },
    "required": sorted(AUDIT_REPORT_KEYS),
    "additionalProperties": False,
}


@dataclass(frozen=True, slots=True)
class ParsedAuditReport:
    """Validated report fields shared by MCP capture and result reduction."""

    outcome: AuditOutcome
    summary: str
    files: tuple[str, ...]
    validation: tuple[str, ...]
    risks: tuple[str, ...]
    verdict: str
    confirmed: tuple[str, ...]
    findings: tuple[AuditFinding, ...]
    required_changes: tuple[str, ...]


class AuditReportCapture:
    """Retain the latest schema-validated report for one auditor session."""

    def __init__(self) -> None:
        self._report_text: str | None = None

    @property
    def report_text(self) -> str | None:
        return self._report_text

    def record(self, payload: dict[str, Any]) -> bool:
        if parse_audit_report_payload(payload) is None:
            return False
        self._report_text = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return True


def parse_audit_report_text(result_text: str | None) -> ParsedAuditReport | None:
    """Parse an exact report object, accepting the legacy single JSON fence."""

    if result_text is None:
        return None
    candidate = result_text.strip()
    fence_prefix = "```json\n"
    fence_suffix = "\n```"
    if (
        candidate.startswith(fence_prefix)
        and candidate.endswith(fence_suffix)
        and candidate.count("```") == 2
    ):
        candidate = candidate[len(fence_prefix) : -len(fence_suffix)]
    try:
        payload: object = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return parse_audit_report_payload(payload)


def parse_audit_report_payload(payload: object) -> ParsedAuditReport | None:
    """Validate one decoded report against the host-owned exact contract."""

    if not isinstance(payload, dict) or set(payload) != AUDIT_REPORT_KEYS:
        return None

    outcome = payload.get("outcome")
    summary = payload.get("summary")
    files = payload.get("files")
    validation = payload.get("validation")
    risks = payload.get("risks")
    verdict = payload.get("verdict")
    confirmed = payload.get("confirmed")
    findings = _parse_findings(payload.get("findings"))
    required_changes = payload.get("required_changes")
    if (
        not isinstance(outcome, str)
        or outcome not in AUDIT_REPORT_OUTCOMES
        or not isinstance(summary, str)
        or not _is_string_list(files)
        or not _is_string_list(validation)
        or not _is_string_list(risks)
        or not isinstance(verdict, str)
        or not _is_string_list(confirmed)
        or findings is None
        or not _is_string_list(required_changes)
    ):
        return None

    return ParsedAuditReport(
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


def _parse_findings(value: object) -> tuple[AuditFinding, ...] | None:
    if not isinstance(value, list):
        return None
    findings: list[AuditFinding] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != AUDIT_FINDING_KEYS:
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


def _is_string_list(value: object) -> TypeGuard[list[str]]:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)
