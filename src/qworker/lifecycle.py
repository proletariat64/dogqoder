"""Durable worker lifecycle contracts and transition validation."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

type WorkerRole = Literal["coder", "auditor"]
type WorkerState = Literal[
    "starting", "running", "requires_action", "completed", "failed", "cancelled", "lost"
]
type WorkerHealth = Literal["unknown", "healthy", "quiet", "stalled", "exited"]
type WriteCapability = Literal["shared_workspace", "read_only"]
type NestedState = Literal["none", "active", "settled", "unknown"]


@dataclass(frozen=True, slots=True)
class WorkerRecord:
    """Stable top-level worker identity and its current lifecycle state."""

    worker_id: str
    role: WorkerRole
    cwd: Path
    state: WorkerState
    health: WorkerHealth
    write_capability: WriteCapability
    requested_model: str
    resolved_model: str | None
    actual_models: tuple[str, ...]
    session_id: str | None
    attempt: int
    runtime_path: str
    runtime_version: str | None
    sdk_version: str
    created_at: datetime
    started_at: datetime | None
    last_event_at: datetime | None
    ended_at: datetime | None
    result_summary: dict[str, object] | None
    nested_state: NestedState
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """One execution attempt belonging to a stable worker identity."""

    worker_id: str
    attempt: int
    state: WorkerState
    created_at: datetime
    started_at: datetime | None
    ended_at: datetime | None


_TRANSITIONS: dict[WorkerState, frozenset[WorkerState]] = {
    "starting": frozenset(("running", "failed")),
    "running": frozenset(
        ("requires_action", "completed", "failed", "cancelled", "lost")
    ),
    "requires_action": frozenset(("running", "cancelled", "lost")),
    "completed": frozenset(),
    "failed": frozenset(("starting",)),
    "cancelled": frozenset(("starting",)),
    "lost": frozenset(("starting",)),
}


class WorkerStateReducer:
    """Validate and retain the lifecycle state for one worker attempt."""

    def __init__(self, state: WorkerState) -> None:
        self._state = state

    @property
    def state(self) -> WorkerState:
        """Current worker lifecycle state."""

        return self._state

    def transition(self, target: WorkerState) -> WorkerState:
        """Move to ``target`` only when section 8.1 permits that edge."""

        if target not in _TRANSITIONS[self._state]:
            message = f"Invalid worker lifecycle transition: {self._state} -> {target}"
            raise ValueError(message)
        self._state = target
        return self._state
