"""SDK-independent contracts for live worker control."""

import asyncio
import math
import os
import re
import secrets
import time
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

from jsonschema.exceptions import SchemaError, ValidationError
from jsonschema.validators import validator_for

from qworker.store import JsonValue

type SteeringPriority = Literal["now", "next", "later"]
type ApprovalKind = Literal["tool_permission", "elicitation", "mcp_oauth"]
type PermissionAction = Literal["allow", "deny"]
type ElicitationAction = Literal["accept", "decline", "cancel"]

_MAX_APPROVAL_PROMPT_CHARS = 256
_MAX_DISPLAY_FIELDS = 32
_MAX_SCHEMA_DEPTH = 16
_MAX_SCHEMA_NODES = 1_024
_MAX_SCHEMA_STRING_CHARS = 4_096
_CREDENTIAL_ENV_VARS = (
    "QODER_PERSONAL_ACCESS_TOKEN",
    "QODER_SERVICE_ACCOUNT_KEY",
    "QODERCN_PERSONAL_ACCESS_TOKEN",
    "QODERCN_SERVICE_ACCOUNT_KEY",
)
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_KEYED_CREDENTIAL = re.compile(
    r"\b(?:api[_ -]?key|access[_ -]?token|token|secret|credential|password|"
    r"authorization)\b(?:\s*[:=]\s*|\s+)[^\s,;]+",
    re.IGNORECASE,
)
_CREDENTIAL_MARKER = re.compile(
    r"(?:^sk[-_]|^pk[-_]|^bearer[._ -]|api[_-]?key|access[_-]?token|token|"
    r"secret|credential|password|authorization)",
    re.IGNORECASE,
)
_INLINE_CREDENTIAL = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"(?:sk|pk)[-_][A-Za-z0-9][A-Za-z0-9._-]{2,}"
    r"|Bearer[ ._-]+[A-Za-z0-9._~+/=-]{4,}"
    r"|AKIA[A-Z0-9]{16}"
    r"|[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}"
    r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)


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
    requested_schema: Mapping[str, object] | None = None


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
    request: PermissionRequest | ElicitationRequest
    display: dict[str, JsonValue]
    future: asyncio.Future[ApprovalDecision]


def approval_display(
    request_id: str,
    attempt: int,
    request: PermissionRequest | ElicitationRequest,
) -> dict[str, JsonValue] | None:
    """Build one bounded, allowlisted, durable-safe approval display record."""

    if isinstance(request, PermissionRequest):
        if not safe_identifier(request.tool_name):
            return None
        display: dict[str, JsonValue] = {
            "request_id": request_id,
            "attempt": attempt,
            "kind": "tool_permission",
            "tool_name": request.tool_name,
            "prompt": f"Allow tool {request.tool_name} for this turn?",
            "choices": ["allow", "deny"],
        }
        if request.agent_id is not None and safe_identifier(request.agent_id):
            display["agent_id"] = request.agent_id
        return display

    server_name = request.server_name if safe_identifier(request.server_name) else "unknown"
    prompt = safe_display_text(request.display_message)
    if not prompt:
        prompt = f"Input requested by {server_name}."
    display = {
        "request_id": request_id,
        "attempt": attempt,
        "kind": "elicitation",
        "server_name": server_name,
        "mode": request.mode,
        "prompt": prompt,
        "choices": ["accept", "decline", "cancel"],
    }
    fields, required_fields = _schema_display(request.requested_schema)
    if request.mode == "form" and fields:
        display["fields"] = fields
    if request.mode == "form" and required_fields:
        display["required_fields"] = required_fields
    return display


def safe_display_text(value: str) -> str:
    """Redact credential forms, remove control bytes, and bound display text."""

    safe = value.replace("\x00", "").replace("\r", " ").replace("\n", " ")
    for variable in _CREDENTIAL_ENV_VARS:
        secret = os.environ.get(variable)
        if secret:
            safe = safe.replace(secret, "[REDACTED]")
    safe = _INLINE_CREDENTIAL.sub("[REDACTED]", safe)
    safe = _KEYED_CREDENTIAL.sub("[REDACTED]", safe)
    safe = " ".join(safe.split())
    if len(safe) <= _MAX_APPROVAL_PROMPT_CHARS:
        return safe
    return safe[: _MAX_APPROVAL_PROMPT_CHARS - 3] + "..."


def safe_identifier(value: str) -> bool:
    """Accept only compact ASCII metadata with no credential marker."""

    return (
        _IDENTIFIER.fullmatch(value) is not None
        and not _CREDENTIAL_MARKER.search(value)
    )


def _schema_display(
    schema: Mapping[str, object] | None,
) -> tuple[list[JsonValue], list[JsonValue]]:
    if schema is None:
        return [], []
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return [], []
    fields: list[JsonValue] = []
    safe_names: set[str] = set()
    for name, field_schema in sorted(properties.items()):
        if (
            len(fields) == _MAX_DISPLAY_FIELDS
            or not isinstance(name, str)
            or not safe_identifier(name)
            or not isinstance(field_schema, Mapping)
        ):
            continue
        field_type = field_schema.get("type")
        if field_type not in (
            "array",
            "boolean",
            "integer",
            "null",
            "number",
            "object",
            "string",
        ):
            field_type = "value"
        fields.append(f"{name}:{field_type}")
        safe_names.add(name)
    required = schema.get("required")
    required_fields: list[JsonValue] = []
    if isinstance(required, Sequence) and not isinstance(required, (str, bytes)):
        required_fields.extend(
            sorted(
                {
                    item
                    for item in required
                    if isinstance(item, str) and item in safe_names
                }
            )[:_MAX_DISPLAY_FIELDS]
        )
    return fields, required_fields


def parse_approval_response(
    request: PermissionRequest | ElicitationRequest,
    response: Mapping[str, JsonValue],
) -> ApprovalDecision:
    """Validate one RPC response against its live request shape."""

    fields = set(response)
    action = response.get("action")
    if isinstance(request, PermissionRequest):
        if fields != {"action"} or action not in ("allow", "deny"):
            raise ValueError(
                "Permission response must contain only action=allow|deny."
            )
        return PermissionDecision(cast(PermissionAction, action))
    if action not in ("accept", "decline", "cancel"):
        raise ValueError(
            "Elicitation response action must be accept, decline, or cancel."
        )
    if action != "accept":
        if fields != {"action"}:
            raise ValueError(
                "Declined or cancelled elicitation must contain only action."
            )
        return ElicitationDecision(cast(ElicitationAction, action))
    if request.mode == "url":
        if fields != {"action"}:
            raise ValueError("Accepted URL elicitation must contain only action.")
        return ElicitationDecision("accept")
    if fields != {"action", "content"}:
        raise ValueError(
            "Accepted form elicitation must contain action and object content."
        )
    content = response.get("content")
    if not isinstance(content, dict):
        raise TypeError("Accepted elicitation content must be an object.")
    _validate_elicitation_content(content, request.requested_schema)
    return ElicitationDecision("accept", content)


def _validate_elicitation_content(
    content: Mapping[str, JsonValue],
    requested_schema: Mapping[str, object] | None,
) -> None:
    if requested_schema is None:
        return
    try:
        schema = _bounded_json_schema(requested_schema)
        validator_type = validator_for(schema)
        validator_type.check_schema(schema)
        validator_type(schema).validate(dict(content))
    except (SchemaError, TypeError, ValidationError, ValueError):
        raise ValueError(
            "Elicitation content does not match the requested form schema."
        ) from None


def _bounded_json_schema(value: object) -> dict[str, JsonValue]:
    budget = [_MAX_SCHEMA_NODES]

    def copy_json(item: object, depth: int) -> JsonValue:
        budget[0] -= 1
        if budget[0] < 0 or depth > _MAX_SCHEMA_DEPTH:
            raise ValueError("Elicitation schema is too complex.")
        if item is None or isinstance(item, (bool, int, str)):
            if isinstance(item, str) and len(item) > _MAX_SCHEMA_STRING_CHARS:
                raise ValueError("Elicitation schema string is too long.")
            return item
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("Elicitation schema number is not finite.")
            return item
        if isinstance(item, Mapping):
            copied: dict[str, JsonValue] = {}
            for key, nested in item.items():
                if not isinstance(key, str) or len(key) > _MAX_SCHEMA_STRING_CHARS:
                    raise ValueError("Elicitation schema key is invalid.")
                if key in ("$ref", "$dynamicRef", "$recursiveRef"):
                    raise ValueError("Elicitation schema references are unsupported.")
                copied[key] = copy_json(nested, depth + 1)
            return copied
        if isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
            return [copy_json(nested, depth + 1) for nested in item]
        raise TypeError("Elicitation schema is not JSON-compatible.")

    copied = copy_json(value, 0)
    if not isinstance(copied, dict):
        raise TypeError("Elicitation form schema must be an object.")
    return copied


def new_request_id() -> str:
    """Generate one Crockford-base32 ULID for a live callback."""

    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    value = (int(time.time_ns() // 1_000_000) << 80) | secrets.randbits(80)
    characters: list[str] = []
    for _ in range(26):
        characters.append(alphabet[value & 31])
        value >>= 5
    return "".join(reversed(characters))
