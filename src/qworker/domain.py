"""Stable domain contracts shared by worker orchestration and transports."""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True, slots=True)
class AuditContract:
    """Input for one read-only foreground audit."""

    objective: str
    cwd: Path
    requested_model: str = "qwen-auditor"
    context: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    acceptance_criteria: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AuditResult:
    """Structured outcome from one foreground audit."""

    outcome: Literal["completed", "partial", "blocked", "failed"]
    summary: str
    files: tuple[str, ...]
    validation: tuple[str, ...]
    risks: tuple[str, ...]
    requested_model: str
    resolved_model: str | None
    actual_models: tuple[str, ...] = ()
    session_id: str | None = None
    nested_state: Literal["none", "active", "settled", "unknown"] = "none"
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
