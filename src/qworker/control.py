"""SDK-independent contracts for live worker control."""

import asyncio
import secrets
import time
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import Literal, cast

from qworker.store import JsonValue

type SteeringPriority = Literal["now", "next", "later"]
type ApprovalKind = Literal["tool_permission", "elicitation", "mcp_oauth"]
type PermissionAction = Literal["allow", "deny"]
type ElicitationAction = Literal["accept", "decline", "cancel"]


@dataclass(frozen=True, slots=True)
class PermissionRequest:
    """Display-safe permission metadata retained only in supervisor memory."""

    tool_name: str
    agent_id: str | None
    display_message: str


@dataclass(frozen=True, slots=True)
class ElicitationRequest:
    """Live MCP elicitation details that are never copied into persistence."""

    server_name: str
    mode: Literal["form", "url"]
    display_message: str


@dataclass(frozen=True, slots=True)
class PermissionDecision:
    """One fail-closed permission callback decision."""

    action: PermissionAction


@dataclass(frozen=True, slots=True)
class ElicitationDecision:
    """One live elicitation response."""

    action: ElicitationAction
    content: dict[str, JsonValue] | None = None


type ApprovalDecision = PermissionDecision | ElicitationDecision
type PermissionCallback = Callable[
    [PermissionRequest], Coroutine[object, object, PermissionDecision]
]
type ElicitationCallback = Callable[
    [ElicitationRequest], Coroutine[object, object, ElicitationDecision]
]


@dataclass(frozen=True, slots=True)
class ControlCallbacks:
    """Supervisor callback bridge bound to exactly one live worker attempt."""

    request_permission: PermissionCallback
    request_elicitation: ElicitationCallback


@dataclass(slots=True)
class PendingApproval:
    """In-memory callback future correlated to one worker attempt."""

    request_id: str
    worker_id: str
    attempt: int
    kind: ApprovalKind
    future: asyncio.Future[ApprovalDecision]


def parse_approval_response(
    kind: ApprovalKind,
    response: Mapping[str, JsonValue],
) -> ApprovalDecision:
    """Validate one RPC response against its pending callback kind."""

    fields = set(response)
    action = response.get("action")
    if kind == "tool_permission":
        if fields != {"action"} or action not in ("allow", "deny"):
            raise ValueError(
                "Permission response must contain only action=allow|deny."
            )
        return PermissionDecision(cast(PermissionAction, action))
    if kind == "elicitation":
        if action not in ("accept", "decline", "cancel"):
            raise ValueError(
                "Elicitation response action must be accept, decline, or cancel."
            )
        if action == "accept":
            if not fields.issubset({"action", "content"}):
                raise ValueError("Elicitation response contains unexpected fields.")
            content = response.get("content")
            if content is not None and not isinstance(content, dict):
                raise ValueError("Accepted elicitation content must be an object.")
            return ElicitationDecision(cast(ElicitationAction, action), content)
        if fields != {"action"}:
            raise ValueError(
                "Declined or cancelled elicitation must contain only action."
            )
        return ElicitationDecision(cast(ElicitationAction, action))
    raise ValueError("MCP OAuth responses are not supported by Task 7.")


def new_request_id() -> str:
    """Generate one Crockford-base32 ULID for a live callback."""

    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    value = (int(time.time_ns() // 1_000_000) << 80) | secrets.randbits(80)
    characters: list[str] = []
    for _ in range(26):
        characters.append(alphabet[value & 31])
        value >>= 5
    return "".join(reversed(characters))
