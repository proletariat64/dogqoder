"""Normalized SDK-independent events emitted by a Qoder transport."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class AssistantEvent:
    """Text, tool use, and model data from one assistant message."""

    text: tuple[str, ...]
    tools: tuple[str, ...]
    model: str | None = None


@dataclass(frozen=True, slots=True)
class TaskStartedEvent:
    """A nested task became active."""

    task_id: str
    description: str


@dataclass(frozen=True, slots=True)
class TaskProgressEvent:
    """Progress telemetry for an active nested task."""

    task_id: str
    description: str
    last_tool_name: str | None = None


@dataclass(frozen=True, slots=True)
class TaskTerminalEvent:
    """A nested task reached a terminal status."""

    task_id: str
    status: Literal["completed", "failed", "stopped"]


@dataclass(frozen=True, slots=True)
class SystemEvent:
    """A bounded, display-safe system diagnostic."""

    subtype: str
    message: str


@dataclass(frozen=True, slots=True)
class ResultEvent:
    """The main turn result and its terminal metadata."""

    session_id: str
    is_error: bool
    result: str | None
    model_usage: tuple[str, ...] = ()
    permission_denials: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


type AdapterEvent = (
    AssistantEvent
    | TaskStartedEvent
    | TaskProgressEvent
    | TaskTerminalEvent
    | SystemEvent
    | ResultEvent
)
