"""SQLite-backed durable storage for worker lifecycle and semantic events."""

import json
import os
import re
import secrets
import sqlite3
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

from qworker.lifecycle import (
    AttemptRecord,
    NestedState,
    WorkerHealth,
    WorkerRecord,
    WorkerRole,
    WorkerState,
    WorkerStateReducer,
    WriteCapability,
)

type JsonValue = (
    None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
)

_CROCKFORD_BASE32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_TERMINAL_STATES = frozenset(("completed", "failed", "cancelled", "lost"))
_WORKER_HEALTH = frozenset(("unknown", "healthy", "quiet", "stalled", "exited"))
_EVENT_SCHEMA_VERSION = 1
_CREDENTIAL_ENV_VARS = (
    "QODER_PERSONAL_ACCESS_TOKEN",
    "QODER_SERVICE_ACCOUNT_KEY",
    "QODERCN_PERSONAL_ACCESS_TOKEN",
    "QODERCN_SERVICE_ACCOUNT_KEY",
)
_MAX_RESULT_STRING_CHARS = 4096
_MAX_RESULT_KEY_CHARS = 256
_MAX_RESULT_COLLECTION_ITEMS = 128
_MAX_RESULT_DEPTH = 6
_MAX_RESULT_NODES = 2048
_MAX_RESULT_TEXT_CHARS = 256 * 1024


@dataclass(frozen=True, slots=True)
class EventRecord:
    """One persisted, ordered semantic event for a worker."""

    sequence: int
    event_id: str
    worker_id: str
    attempt: int
    timestamp: datetime
    type: str
    payload: dict[str, JsonValue]

    @property
    def event_type(self) -> str:
        """Alias for callers that avoid the built-in name ``type``."""

        return self.type


class AttemptChangedError(RuntimeError):
    """A conditional event write no longer targets its live attempt."""


type _PayloadValidator = Callable[[JsonValue], bool]


@dataclass(frozen=True, slots=True)
class _EventSchema:
    """Strict bounded field contract for one persisted semantic event type."""

    required: Mapping[str, _PayloadValidator]
    optional: Mapping[str, _PayloadValidator]


def _positive_integer(value: JsonValue) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _non_negative_integer(value: JsonValue) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _boolean(value: JsonValue) -> bool:
    return isinstance(value, bool)


def _safe_display_text(value: JsonValue) -> bool:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 256
        or "\x00" in value
        or "\n" in value
        or "\r" in value
        or _INLINE_CREDENTIAL.search(value)
        or _KEYED_CREDENTIAL.search(value)
    ):
        return False
    return all(
        not (secret and secret in value)
        for secret in (os.environ.get(name) for name in _CREDENTIAL_ENV_VARS)
    )


def _approval_choices(value: JsonValue) -> bool:
    return value in (["allow", "deny"], ["accept", "decline", "cancel"])


def _display_fields(value: JsonValue) -> bool:
    if not isinstance(value, list) or len(value) > 32:
        return False
    return all(
        isinstance(item, str)
        and re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}:(?:array|boolean|integer|null|number|object|string|value)",
            item,
        )
        is not None
        for item in value
    )


def _required_display_fields(value: JsonValue) -> bool:
    return (
        isinstance(value, list)
        and len(value) <= 32
        and all(_IDENTIFIER(item) for item in value)
    )


def _one_of(*values: str) -> _PayloadValidator:
    allowed = frozenset(values)

    def validate(value: JsonValue) -> bool:
        return isinstance(value, str) and value in allowed

    return validate


_METADATA = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_VERSION = re.compile(
    r"v?\d+(?:\.\d+){1,3}(?:[-+][A-Za-z0-9._-]{1,64})?\Z", re.IGNORECASE
)
_CREDENTIAL_MARKER = re.compile(
    r"(?:^sk[-_]|^pk[-_]|^bearer[._ -]|api[_-]?key|access[_-]?token|secret|credential|password|authorization)",
    re.IGNORECASE,
)
_JWT = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\Z")
_AWS_ACCESS_KEY = re.compile(r"AKIA[A-Z0-9]{16}\Z")
_INLINE_CREDENTIAL = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"(?:sk|pk)[-_][A-Za-z0-9][A-Za-z0-9._-]{2,}"
    r"|Bearer[ ._-]+[A-Za-z0-9._~+/=-]{4,}"
    r"|AKIA[A-Z0-9]{16}"
    r"|[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}"
    r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_KEYED_CREDENTIAL = re.compile(
    r"\b(?:api[_ -]?key|access[_ -]?token|token|secret|credential|password|"
    r"authorization)\b(?:\s*[:=]\s*|\s+)[^\s,;]+",
    re.IGNORECASE,
)
_RESULT_CREDENTIAL_KEY = re.compile(
    r"(?:api[_ -]?key|token|secret|credential|password|authorization|"
    r"private[_ -]?key|service[_ -]?account[_ -]?key)",
    re.IGNORECASE,
)


@dataclass(slots=True)
class _ResultBudget:
    nodes: int = _MAX_RESULT_NODES
    text_chars: int = _MAX_RESULT_TEXT_CHARS


def _safe_metadata(value: JsonValue) -> bool:
    """Accept identifier-shaped metadata, never free-form text or credentials."""

    if not isinstance(value, str) or _METADATA.fullmatch(value) is None:
        return False
    return not (
        _CREDENTIAL_MARKER.search(value)
        or _JWT.fullmatch(value)
        or _AWS_ACCESS_KEY.fullmatch(value)
    )


def _safe_runtime_version(value: JsonValue) -> bool:
    return (
        isinstance(value, str)
        and _VERSION.fullmatch(value) is not None
        and not _CREDENTIAL_MARKER.search(value)
    )


def _contains_sensitive_marker(value: str) -> bool:
    return bool(
        _CREDENTIAL_MARKER.search(value)
        or _JWT.fullmatch(value)
        or _AWS_ACCESS_KEY.fullmatch(value)
    )


def _absolute_path(value: JsonValue) -> bool:
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or len(value) > 4096
        or "\x00" in value
        or "\n" in value
        or "\r" in value
    ):
        return False
    return all(
        not _contains_sensitive_marker(part) for part in value.split("/") if part
    )


def _runtime_path(value: JsonValue) -> bool:
    return value == "bundled" or _absolute_path(value)


def _canonical_workspace(cwd: Path) -> Path:
    try:
        canonical_cwd = cwd.resolve(strict=True)
    except (OSError, RuntimeError):
        raise ValueError("Unsafe worker creation field: cwd") from None
    if not canonical_cwd.is_dir():
        raise ValueError("Unsafe worker creation field: cwd")
    return canonical_cwd


_IDENTIFIER = _safe_metadata
_CODE = _safe_metadata
_MODEL = _safe_metadata
_PATH = _absolute_path
_RUNTIME_VERSION = _safe_runtime_version
_ROLE = _one_of("coder", "auditor")
_WRITE_CAPABILITY = _one_of("shared_workspace", "read_only")
_EVENT_SCHEMAS: dict[str, _EventSchema] = {
    "worker.created": _EventSchema(
        required={
            "attempt": _positive_integer,
            "cwd": _PATH,
            "role": _ROLE,
        },
        optional={},
    ),
    "worker.state_changed": _EventSchema(
        required={
            "state": _one_of(
                "starting",
                "running",
                "requires_action",
                "completed",
                "failed",
                "cancelled",
                "lost",
            )
        },
        optional={"attempt": _positive_integer},
    ),
    "worker.health_changed": _EventSchema(
        required={
            "health": _one_of("unknown", "healthy", "quiet", "stalled", "exited")
        },
        optional={},
    ),
    "runtime.started": _EventSchema(required={}, optional={"pid": _positive_integer}),
    "runtime.initialized": _EventSchema(
        required={},
        optional={"runtime_version": _RUNTIME_VERSION, "session_id": _IDENTIFIER},
    ),
    "model.resolved": _EventSchema(required={"model": _MODEL}, optional={}),
    "assistant.message": _EventSchema(required={}, optional={"model": _MODEL}),
    "tool.started": _EventSchema(required={"tool_name": _IDENTIFIER}, optional={}),
    "tool.finished": _EventSchema(
        required={"tool_name": _IDENTIFIER},
        optional={"duration_ms": _non_negative_integer, "succeeded": _boolean},
    ),
    "nested.started": _EventSchema(required={"task_id": _IDENTIFIER}, optional={}),
    "nested.progress": _EventSchema(
        required={"task_id": _IDENTIFIER}, optional={"tool_name": _IDENTIFIER}
    ),
    "nested.terminal": _EventSchema(
        required={
            "task_id": _IDENTIFIER,
            "status": _one_of("completed", "failed", "stopped"),
        },
        optional={},
    ),
    "approval.requested": _EventSchema(
        required={
            "request_id": _IDENTIFIER,
            "kind": _one_of("tool_permission", "elicitation", "mcp_oauth"),
            "prompt": _safe_display_text,
            "choices": _approval_choices,
        },
        optional={
            "agent_id": _IDENTIFIER,
            "tool_name": _IDENTIFIER,
            "server_name": _IDENTIFIER,
            "mode": _one_of("form", "url"),
            "fields": _display_fields,
            "required_fields": _required_display_fields,
        },
    ),
    "approval.resolved": _EventSchema(
        required={
            "request_id": _IDENTIFIER,
            "status": _one_of("allowed", "denied", "answered", "expired"),
        },
        optional={},
    ),
    "steer.queued": _EventSchema(
        required={
            "message_id": _IDENTIFIER,
            "priority": _one_of("now", "next", "later"),
        },
        optional={},
    ),
    "steer.delivered": _EventSchema(required={"message_id": _IDENTIFIER}, optional={}),
    "steer.cancelled": _EventSchema(
        required={"message_id": _IDENTIFIER}, optional={"cancelled": _boolean}
    ),
    "result.received": _EventSchema(
        required={"outcome": _one_of("completed", "partial", "blocked", "failed")},
        optional={"model": _MODEL},
    ),
    "worker.warning": _EventSchema(required={"code": _CODE}, optional={}),
    "runtime.exited": _EventSchema(
        required={}, optional={"exit_code": _non_negative_integer}
    ),
}


class WorkerStore:
    """Persist worker records, their execution attempts, and safe event cursors."""

    def __init__(self, state_dir: Path) -> None:
        self._state_dir = state_dir
        self._database_path = state_dir / "qworker.db"
        self._connection: sqlite3.Connection | None = None

    @property
    def state_dir(self) -> Path:
        """Directory containing the private SQLite state database."""

        return self._state_dir

    @property
    def database_path(self) -> Path:
        """SQLite database path, made available for maintenance and diagnostics."""

        return self._database_path

    async def create_worker(
        self,
        *,
        role: WorkerRole,
        cwd: Path,
        write_capability: WriteCapability,
        requested_model: str,
        runtime_path: str,
        sdk_version: str,
        worker_id: str | None = None,
        runtime_version: str | None = None,
    ) -> WorkerRecord:
        """Create a stable worker identity with its first positive attempt."""

        worker_identifier = worker_id or _new_ulid()
        canonical_cwd = _canonical_workspace(cwd)
        _validate_worker_creation(
            worker_id=worker_identifier,
            role=role,
            cwd=canonical_cwd,
            write_capability=write_capability,
            requested_model=requested_model,
            runtime_path=runtime_path,
            runtime_version=runtime_version,
            sdk_version=sdk_version,
        )
        connection = self._open()
        created_at = _utc_now()
        created_at_text = _dump_timestamp(created_at)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO workers (
                    worker_id, role, cwd, state, health, write_capability,
                    requested_model, resolved_model, actual_models, session_id, attempt,
                    runtime_path, runtime_version, sdk_version, created_at, started_at,
                    last_event_at, ended_at, result_summary, nested_state, warnings
                ) VALUES (?, ?, ?, 'starting', 'unknown', ?, ?, NULL, '[]', NULL, 1,
                          ?, ?, ?, ?, NULL, NULL, NULL, NULL, 'none', '[]')
                """,
                (
                    worker_identifier,
                    role,
                    str(canonical_cwd),
                    write_capability,
                    requested_model,
                    runtime_path,
                    runtime_version,
                    sdk_version,
                    created_at_text,
                ),
            )
            connection.execute(
                """
                INSERT INTO attempts (worker_id, attempt, state, created_at, started_at, ended_at)
                VALUES (?, 1, 'starting', ?, NULL, NULL)
                """,
                (worker_identifier, created_at_text),
            )
            self._append_event(
                connection,
                worker_id=worker_identifier,
                attempt=1,
                event_type="worker.created",
                payload={
                    "schema_version": _EVENT_SCHEMA_VERSION,
                    "attempt": 1,
                    "cwd": str(canonical_cwd),
                    "role": role,
                },
                timestamp=created_at,
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return self._get_worker(connection, worker_identifier)

    async def start_attempt(self, worker_id: str) -> AttemptRecord:
        """Begin the next attempt under an existing stable worker identity."""

        connection = self._open()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt, state FROM workers WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown worker: {worker_id}")
            WorkerStateReducer(cast(WorkerState, row["state"])).transition("starting")
            attempt = int(row["attempt"]) + 1
            created_at = _utc_now()
            created_at_text = _dump_timestamp(created_at)
            connection.execute(
                """
                INSERT INTO attempts (worker_id, attempt, state, created_at, started_at, ended_at)
                VALUES (?, ?, 'starting', ?, NULL, NULL)
                """,
                (worker_id, attempt, created_at_text),
            )
            connection.execute(
                """
                UPDATE workers
                SET attempt = ?, state = 'starting', health = 'unknown', session_id = NULL,
                    started_at = NULL, last_event_at = NULL, ended_at = NULL,
                    result_summary = NULL, nested_state = 'none', warnings = '[]'
                WHERE worker_id = ?
                """,
                (attempt, worker_id),
            )
            self._append_event(
                connection,
                worker_id=worker_id,
                attempt=attempt,
                event_type="worker.state_changed",
                payload={
                    "schema_version": _EVENT_SCHEMA_VERSION,
                    "attempt": attempt,
                    "state": "starting",
                },
                timestamp=created_at,
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return AttemptRecord(
            worker_id=worker_id,
            attempt=attempt,
            state="starting",
            created_at=created_at,
            started_at=None,
            ended_at=None,
        )

    async def append_event(
        self,
        worker_id: str,
        event_type: str,
        payload: Mapping[str, JsonValue],
    ) -> EventRecord:
        """Append one allowed semantic event and return its durable cursor."""

        connection = self._open()
        safe_payload = _validated_payload(event_type, payload)
        if event_type == "worker.created":
            raise ValueError("Use create_worker() to persist worker.created.")
        if event_type == "worker.state_changed" and safe_payload["state"] == "starting":
            raise ValueError(
                "Use start_attempt() to create a resumed starting attempt."
            )
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt FROM workers WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown worker: {worker_id}")
            event = self._append_event(
                connection,
                worker_id=worker_id,
                attempt=int(row["attempt"]),
                event_type=event_type,
                payload=safe_payload,
                timestamp=_utc_now(),
            )
            self._project_event(connection, event)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return event

    async def record_approval_request(
        self,
        worker_id: str,
        *,
        expected_attempt: int,
        payload: Mapping[str, JsonValue],
    ) -> tuple[EventRecord, ...]:
        """Atomically persist a live-attempt request and requires-action state."""

        safe_payload = _validated_payload("approval.requested", payload)
        connection = self._open()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt, state FROM workers WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown worker: {worker_id}")
            attempt = int(row["attempt"])
            state = cast(WorkerState, row["state"])
            if attempt != expected_attempt or state not in (
                "running",
                "requires_action",
            ):
                raise AttemptChangedError("Approval attempt is no longer live.")
            timestamp = _utc_now()
            requested = self._append_event(
                connection,
                worker_id=worker_id,
                attempt=expected_attempt,
                event_type="approval.requested",
                payload=safe_payload,
                timestamp=timestamp,
            )
            events = [requested]
            if state == "running":
                state_event = self._append_event(
                    connection,
                    worker_id=worker_id,
                    attempt=expected_attempt,
                    event_type="worker.state_changed",
                    payload={"schema_version": 1, "state": "requires_action"},
                    timestamp=timestamp,
                )
                self._project_event(connection, state_event)
                events.append(state_event)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return tuple(events)

    async def record_approval_resolution(
        self,
        worker_id: str,
        *,
        expected_attempt: int,
        payload: Mapping[str, JsonValue],
        restore_running: bool,
    ) -> tuple[EventRecord, ...]:
        """Atomically persist one resolution and optional running transition."""

        safe_payload = _validated_payload("approval.resolved", payload)
        connection = self._open()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt, state FROM workers WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown worker: {worker_id}")
            if int(row["attempt"]) != expected_attempt or row["state"] != (
                "requires_action"
            ):
                raise AttemptChangedError("Approval attempt is no longer live.")
            timestamp = _utc_now()
            resolved = self._append_event(
                connection,
                worker_id=worker_id,
                attempt=expected_attempt,
                event_type="approval.resolved",
                payload=safe_payload,
                timestamp=timestamp,
            )
            events = [resolved]
            if restore_running:
                state_event = self._append_event(
                    connection,
                    worker_id=worker_id,
                    attempt=expected_attempt,
                    event_type="worker.state_changed",
                    payload={"schema_version": 1, "state": "running"},
                    timestamp=timestamp,
                )
                self._project_event(connection, state_event)
                events.append(state_event)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return tuple(events)

    async def expire_approval_requests(
        self,
        worker_id: str,
        *,
        expected_attempt: int,
        request_ids: tuple[str, ...],
    ) -> tuple[EventRecord, ...]:
        """Atomically close raced approvals before a completed attempt exits."""

        if not request_ids:
            return ()
        payloads = tuple(
            _validated_payload(
                "approval.resolved",
                {
                    "schema_version": 1,
                    "request_id": request_id,
                    "status": "expired",
                },
            )
            for request_id in request_ids
        )
        connection = self._open()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt, state FROM workers WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown worker: {worker_id}")
            if int(row["attempt"]) != expected_attempt or row["state"] != (
                "requires_action"
            ):
                raise AttemptChangedError("Approval attempt is no longer live.")
            timestamp = _utc_now()
            events = [
                self._append_event(
                    connection,
                    worker_id=worker_id,
                    attempt=expected_attempt,
                    event_type="approval.resolved",
                    payload=payload,
                    timestamp=timestamp,
                )
                for payload in payloads
            ]
            state_event = self._append_event(
                connection,
                worker_id=worker_id,
                attempt=expected_attempt,
                event_type="worker.state_changed",
                payload={"schema_version": 1, "state": "running"},
                timestamp=timestamp,
            )
            self._project_event(connection, state_event)
            events.append(state_event)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return tuple(events)

    async def record_result(
        self,
        worker_id: str,
        *,
        outcome: Literal["completed", "partial", "blocked", "failed"],
        result_summary: Mapping[str, JsonValue],
        resolved_model: str | None,
        actual_models: tuple[str, ...],
        session_id: str | None,
        nested_state: NestedState,
        warnings: tuple[str, ...],
    ) -> EventRecord:
        """Persist a structured result and its semantic event in one transaction."""

        safe_result = _safe_result_summary(result_summary, outcome=outcome)
        if resolved_model is not None and not _MODEL(resolved_model):
            raise ValueError("Unsafe worker result field: resolved_model")
        if any(not _MODEL(model) for model in actual_models):
            raise ValueError("Unsafe worker result field: actual_models")
        if session_id is not None and not _IDENTIFIER(session_id):
            raise ValueError("Unsafe worker result field: session_id")
        if nested_state not in ("none", "active", "settled", "unknown"):
            raise ValueError("Unsafe worker result field: nested_state")
        if any(not _CODE(warning) for warning in warnings):
            raise ValueError("Unsafe worker result field: warnings")

        connection = self._open()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt FROM workers WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown worker: {worker_id}")
            payload: dict[str, JsonValue] = {
                "schema_version": _EVENT_SCHEMA_VERSION,
                "outcome": outcome,
            }
            if resolved_model is not None:
                payload["model"] = resolved_model
            event = self._append_event(
                connection,
                worker_id=worker_id,
                attempt=int(row["attempt"]),
                event_type="result.received",
                payload=payload,
                timestamp=_utc_now(),
            )
            connection.execute(
                """
                UPDATE workers
                SET result_summary = ?, resolved_model = ?, actual_models = ?,
                    session_id = ?, nested_state = ?, warnings = ?
                WHERE worker_id = ?
                """,
                (
                    json.dumps(safe_result, separators=(",", ":"), sort_keys=True),
                    resolved_model,
                    json.dumps(actual_models, separators=(",", ":")),
                    session_id,
                    nested_state,
                    json.dumps(warnings, separators=(",", ":")),
                    worker_id,
                ),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return event

    async def get_worker(self, worker_id: str) -> WorkerRecord | None:
        """Return current durable state for ``worker_id``, if it exists."""

        connection = self._open()
        row = connection.execute(
            "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
        ).fetchone()
        return _worker_from_row(row) if row is not None else None

    async def latest_event_cursor(self, worker_id: str) -> int:
        """Return the latest per-worker sequence without loading event history."""

        connection = self._open()
        row = connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) AS cursor FROM events WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()
        return int(row["cursor"])

    async def events_since(
        self, worker_id: str, since: int = 0
    ) -> tuple[EventRecord, ...]:
        """Replay persisted worker events with sequence strictly greater than ``since``."""

        if since < 0:
            raise ValueError("Event cursor must be non-negative.")
        connection = self._open()
        rows = connection.execute(
            """
            SELECT sequence, event_id, worker_id, attempt, timestamp, type, payload
            FROM events
            WHERE worker_id = ? AND sequence > ?
            ORDER BY sequence ASC
            """,
            (worker_id, since),
        ).fetchall()
        return tuple(_event_from_row(row) for row in rows)

    async def close(self) -> None:
        """Close the SQLite connection when the owning supervisor stops."""

        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _open(self) -> sqlite3.Connection:
        if self._connection is not None:
            return self._connection
        self._state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        if os.name == "posix":
            self._state_dir.chmod(0o700)
            descriptor = os.open(self._database_path, os.O_RDWR | os.O_CREAT, 0o600)
            os.close(descriptor)
            self._database_path.chmod(0o600)
        connection = sqlite3.connect(self._database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                role TEXT NOT NULL,
                cwd TEXT NOT NULL,
                state TEXT NOT NULL,
                health TEXT NOT NULL,
                write_capability TEXT NOT NULL,
                requested_model TEXT NOT NULL,
                resolved_model TEXT,
                actual_models TEXT NOT NULL,
                session_id TEXT,
                attempt INTEGER NOT NULL CHECK (attempt > 0),
                runtime_path TEXT NOT NULL,
                runtime_version TEXT,
                sdk_version TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                last_event_at TEXT,
                ended_at TEXT,
                result_summary TEXT,
                nested_state TEXT NOT NULL,
                warnings TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS attempts (
                worker_id TEXT NOT NULL REFERENCES workers(worker_id),
                attempt INTEGER NOT NULL CHECK (attempt > 0),
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                PRIMARY KEY (worker_id, attempt)
            );
            CREATE TABLE IF NOT EXISTS events (
                worker_id TEXT NOT NULL REFERENCES workers(worker_id),
                sequence INTEGER NOT NULL CHECK (sequence > 0),
                event_id TEXT NOT NULL UNIQUE,
                attempt INTEGER NOT NULL CHECK (attempt > 0),
                timestamp TEXT NOT NULL,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (worker_id, sequence)
            );
            """
        )
        self._connection = connection
        return connection

    def _append_event(
        self,
        connection: sqlite3.Connection,
        *,
        worker_id: str,
        attempt: int,
        event_type: str,
        payload: Mapping[str, JsonValue],
        timestamp: datetime,
    ) -> EventRecord:
        safe_payload = _validated_payload(event_type, payload)
        row = connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) AS last_sequence FROM events WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()
        sequence = int(row["last_sequence"]) + 1
        event = EventRecord(
            sequence=sequence,
            event_id=_new_ulid(),
            worker_id=worker_id,
            attempt=attempt,
            timestamp=timestamp,
            type=event_type,
            payload=safe_payload,
        )
        timestamp_text = _dump_timestamp(timestamp)
        connection.execute(
            """
            INSERT INTO events (worker_id, sequence, event_id, attempt, timestamp, type, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.worker_id,
                event.sequence,
                event.event_id,
                event.attempt,
                timestamp_text,
                event.type,
                json.dumps(event.payload, separators=(",", ":"), sort_keys=True),
            ),
        )
        connection.execute(
            "UPDATE workers SET last_event_at = ? WHERE worker_id = ?",
            (timestamp_text, worker_id),
        )
        return event

    def _get_worker(
        self, connection: sqlite3.Connection, worker_id: str
    ) -> WorkerRecord:
        row = connection.execute(
            "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
        ).fetchone()
        if row is None:
            raise AssertionError("Worker insert did not persist.")
        return _worker_from_row(row)

    @staticmethod
    def _project_event(connection: sqlite3.Connection, event: EventRecord) -> None:
        """Reflect lifecycle and health semantic events in current worker state."""

        if event.type == "worker.state_changed":
            target = event.payload.get("state")
            if not isinstance(target, str):
                raise ValueError("worker.state_changed requires a string state.")
            row = connection.execute(
                "SELECT state FROM workers WHERE worker_id = ?", (event.worker_id,)
            ).fetchone()
            if row is None:
                raise AssertionError("Event worker disappeared during its transaction.")
            state = WorkerStateReducer(cast(WorkerState, row["state"])).transition(
                cast(WorkerState, target)
            )
            event_time = _dump_timestamp(event.timestamp)
            started_at = event_time if state == "running" else None
            ended_at = event_time if state in _TERMINAL_STATES else None
            connection.execute(
                """
                UPDATE workers
                SET state = ?, started_at = COALESCE(?, started_at),
                    ended_at = COALESCE(?, ended_at)
                WHERE worker_id = ?
                """,
                (state, started_at, ended_at, event.worker_id),
            )
            connection.execute(
                """
                UPDATE attempts
                SET state = ?, started_at = COALESCE(?, started_at),
                    ended_at = COALESCE(?, ended_at)
                WHERE worker_id = ? AND attempt = ?
                """,
                (state, started_at, ended_at, event.worker_id, event.attempt),
            )
        elif event.type == "worker.health_changed":
            health = event.payload.get("health")
            if not isinstance(health, str) or health not in _WORKER_HEALTH:
                raise ValueError("worker.health_changed requires a valid health value.")
            connection.execute(
                "UPDATE workers SET health = ? WHERE worker_id = ?",
                (health, event.worker_id),
            )


def _worker_from_row(row: sqlite3.Row) -> WorkerRecord:
    result_summary = _load_json_object(row["result_summary"])
    return WorkerRecord(
        worker_id=cast(str, row["worker_id"]),
        role=cast(WorkerRole, row["role"]),
        cwd=Path(cast(str, row["cwd"])),
        state=cast(WorkerState, row["state"]),
        health=cast(WorkerHealth, row["health"]),
        write_capability=cast(WriteCapability, row["write_capability"]),
        requested_model=cast(str, row["requested_model"]),
        resolved_model=cast(str | None, row["resolved_model"]),
        actual_models=tuple(
            cast(list[str], json.loads(cast(str, row["actual_models"])))
        ),
        session_id=cast(str | None, row["session_id"]),
        attempt=cast(int, row["attempt"]),
        runtime_path=cast(str, row["runtime_path"]),
        runtime_version=cast(str | None, row["runtime_version"]),
        sdk_version=cast(str, row["sdk_version"]),
        created_at=_load_timestamp(cast(str, row["created_at"])),
        started_at=_load_optional_timestamp(cast(str | None, row["started_at"])),
        last_event_at=_load_optional_timestamp(cast(str | None, row["last_event_at"])),
        ended_at=_load_optional_timestamp(cast(str | None, row["ended_at"])),
        result_summary=result_summary,
        nested_state=cast(NestedState, row["nested_state"]),
        warnings=tuple(cast(list[str], json.loads(cast(str, row["warnings"])))),
    )


def _event_from_row(row: sqlite3.Row) -> EventRecord:
    payload = _load_json_object(cast(str, row["payload"]))
    if payload is None:
        raise AssertionError("Persisted event payload must be an object.")
    return EventRecord(
        sequence=cast(int, row["sequence"]),
        event_id=cast(str, row["event_id"]),
        worker_id=cast(str, row["worker_id"]),
        attempt=cast(int, row["attempt"]),
        timestamp=_load_timestamp(cast(str, row["timestamp"])),
        type=cast(str, row["type"]),
        payload=cast(dict[str, JsonValue], payload),
    )


def _validated_payload(
    event_type: str, payload: Mapping[str, JsonValue]
) -> dict[str, JsonValue]:
    schema = _EVENT_SCHEMAS.get(event_type)
    if schema is None:
        raise ValueError(f"Unsupported semantic event type: {event_type}")
    copied = _copy_json_object(payload)
    version = copied.get("schema_version")
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or version != _EVENT_SCHEMA_VERSION
    ):
        raise ValueError(
            f"Event payload requires schema_version={_EVENT_SCHEMA_VERSION}."
        )
    allowed = frozenset(("schema_version", *schema.required, *schema.optional))
    unexpected = set(copied).difference(allowed)
    if unexpected:
        fields = ", ".join(sorted(unexpected))
        raise ValueError(f"Non-semantic event payload field: {fields}")
    missing = set(schema.required).difference(copied)
    if missing:
        fields = ", ".join(sorted(missing))
        raise ValueError(f"Event payload is missing required field: {fields}")
    validators = {**schema.required, **schema.optional}
    for field, validator in validators.items():
        if field in copied and not validator(copied[field]):
            raise ValueError(f"Invalid semantic event payload field: {field}")
    if event_type == "approval.requested":
        _validate_approval_request_shape(copied)
    return copied


def _validate_approval_request_shape(payload: Mapping[str, JsonValue]) -> None:
    kind = payload["kind"]
    choices = payload["choices"]
    if kind == "tool_permission":
        if choices != ["allow", "deny"] or "tool_name" not in payload:
            raise ValueError("Invalid tool permission display shape.")
        unexpected = set(payload).intersection(
            {"server_name", "mode", "fields", "required_fields"}
        )
        if unexpected:
            raise ValueError("Invalid tool permission display shape.")
        return
    if kind == "elicitation":
        if (
            choices != ["accept", "decline", "cancel"]
            or "server_name" not in payload
            or "mode" not in payload
            or "tool_name" in payload
        ):
            raise ValueError("Invalid elicitation display shape.")
        if payload["mode"] == "url" and (
            "fields" in payload or "required_fields" in payload
        ):
            raise ValueError("URL elicitation cannot expose form fields.")
        return
    raise ValueError("MCP OAuth approval display is not supported.")


def _validate_worker_creation(
    *,
    worker_id: str,
    role: WorkerRole,
    cwd: Path,
    write_capability: WriteCapability,
    requested_model: str,
    runtime_path: str,
    runtime_version: str | None,
    sdk_version: str,
) -> None:
    """Reject unsafe public worker metadata before any SQLite write begins."""

    values: tuple[tuple[str, JsonValue, _PayloadValidator], ...] = (
        ("worker_id", worker_id, _IDENTIFIER),
        ("role", role, _ROLE),
        ("cwd", str(cwd), _PATH),
        ("write_capability", write_capability, _WRITE_CAPABILITY),
        ("requested_model", requested_model, _MODEL),
        ("runtime_path", runtime_path, _runtime_path),
        ("sdk_version", sdk_version, _RUNTIME_VERSION),
    )
    for field, value, validator in values:
        if not validator(value):
            raise ValueError(f"Unsafe worker creation field: {field}")
    if runtime_version is not None and not _RUNTIME_VERSION(runtime_version):
        raise ValueError("Unsafe worker creation field: runtime_version")


def _copy_json_object(payload: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    try:
        serialized = json.dumps(dict(payload), separators=(",", ":"), sort_keys=True)
        decoded = json.loads(serialized)
    except (TypeError, ValueError) as error:
        raise TypeError("Event payload must be a JSON object.") from error
    if not isinstance(decoded, dict) or not all(
        isinstance(key, str) for key in decoded
    ):
        raise TypeError("Event payload must be a JSON object.")
    return cast(dict[str, JsonValue], decoded)


def _safe_result_summary(
    result_summary: Mapping[str, JsonValue],
    *,
    outcome: Literal["completed", "partial", "blocked", "failed"],
) -> dict[str, JsonValue]:
    sanitized = _sanitize_result_value(
        result_summary,
        depth=0,
        budget=_ResultBudget(
            text_chars=_MAX_RESULT_TEXT_CHARS - len(outcome)
        ),
        secret_context=False,
    )
    if not isinstance(sanitized, dict):
        raise TypeError("Structured result must remain a JSON object.")
    if "outcome" not in sanitized and len(sanitized) == _MAX_RESULT_COLLECTION_ITEMS:
        sanitized.pop(next(reversed(sanitized)))
    sanitized["outcome"] = outcome
    return sanitized


def _sanitize_result_value(
    value: object, *, depth: int, budget: _ResultBudget, secret_context: bool
) -> JsonValue:
    if budget.nodes <= 0 or depth > _MAX_RESULT_DEPTH:
        return None
    budget.nodes -= 1
    if secret_context:
        return _safe_result_text("[REDACTED]", budget)
    if isinstance(value, str):
        return _safe_result_text(value, budget)
    if isinstance(value, list):
        sanitized_list: list[JsonValue] = []
        for index, item in enumerate(value):
            if index == _MAX_RESULT_COLLECTION_ITEMS or budget.nodes <= 0:
                break
            sanitized_list.append(
                _sanitize_result_value(
                    item,
                    depth=depth + 1,
                    budget=budget,
                    secret_context=False,
                )
            )
        return sanitized_list
    if isinstance(value, Mapping):
        sanitized_object: dict[str, JsonValue] = {}
        for index, (key, item) in enumerate(value.items()):
            if index == _MAX_RESULT_COLLECTION_ITEMS or budget.nodes <= 0:
                break
            if not isinstance(key, str):
                raise TypeError("Structured result keys must be strings.")
            safe_key = _unique_result_key(
                _safe_result_key(key), sanitized_object
            )
            sanitized_object[safe_key] = _sanitize_result_value(
                item,
                depth=depth + 1,
                budget=budget,
                secret_context=_credential_result_key(key),
            )
        return sanitized_object
    if value is None or isinstance(value, (bool, int, float)):
        return value
    raise TypeError("Structured result contains a non-JSON value.")


def _credential_result_key(key: str) -> bool:
    if _RESULT_CREDENTIAL_KEY.search(key) or _contains_sensitive_marker(key):
        return True
    for variable in _CREDENTIAL_ENV_VARS:
        secret = os.environ.get(variable)
        if secret and secret in key:
            return True
    return False


def _safe_result_key(key: str) -> str:
    return _redact_result_text(key)[:_MAX_RESULT_KEY_CHARS]


def _unique_result_key(
    candidate: str, existing: Mapping[str, JsonValue]
) -> str:
    if candidate not in existing:
        return candidate
    index = 2
    while True:
        suffix = f"#{index}"
        unique = candidate[: _MAX_RESULT_KEY_CHARS - len(suffix)] + suffix
        if unique not in existing:
            return unique
        index += 1


def _safe_result_text(value: str, budget: _ResultBudget) -> str:
    safe = _redact_result_text(value)
    limit = min(_MAX_RESULT_STRING_CHARS, max(budget.text_chars, 0))
    if len(safe) > limit:
        safe = safe[: max(limit - 3, 0)] + ("..." if limit >= 3 else "")
    budget.text_chars -= len(safe)
    return safe


def _redact_result_text(value: str) -> str:
    safe = value
    for variable in _CREDENTIAL_ENV_VARS:
        secret = os.environ.get(variable)
        if secret:
            safe = safe.replace(secret, "[REDACTED]")
    safe = _INLINE_CREDENTIAL.sub("[REDACTED]", safe)
    safe = _KEYED_CREDENTIAL.sub("[REDACTED]", safe)
    return safe


def _load_json_object(raw: str | None) -> dict[str, object] | None:
    if raw is None:
        return None
    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        raise TypeError("Persisted JSON object was malformed.")
    return cast(dict[str, object], decoded)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _dump_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _load_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise AssertionError("Persisted timestamp must include UTC offset.")
    return parsed.astimezone(UTC)


def _load_optional_timestamp(value: str | None) -> datetime | None:
    return _load_timestamp(value) if value is not None else None


def _new_ulid() -> str:
    timestamp = int(time.time_ns() // 1_000_000)
    value = (timestamp << 80) | secrets.randbits(80)
    characters: list[str] = []
    for _ in range(26):
        characters.append(_CROCKFORD_BASE32[value & 31])
        value >>= 5
    return "".join(reversed(characters))
