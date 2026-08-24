"""SQLite-backed durable storage for worker lifecycle and semantic events."""

import json
import os
import secrets
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

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

_EVENT_TYPES = frozenset(
    (
        "worker.created",
        "worker.state_changed",
        "worker.health_changed",
        "runtime.started",
        "runtime.initialized",
        "model.resolved",
        "assistant.message",
        "tool.started",
        "tool.finished",
        "nested.started",
        "nested.progress",
        "nested.terminal",
        "approval.requested",
        "approval.resolved",
        "steer.queued",
        "steer.delivered",
        "steer.cancelled",
        "result.received",
        "worker.warning",
        "runtime.exited",
    )
)
_FORBIDDEN_PAYLOAD_KEY_PARTS = frozenset(
    (
        "token",
        "secret",
        "credential",
        "password",
        "authorization",
        "transcript",
        "stdout",
        "delta",
    )
)
_FORBIDDEN_PAYLOAD_KEYS = frozenset(("text", "content", "raw", "prompt", "tool_input"))
_CROCKFORD_BASE32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_TERMINAL_STATES = frozenset(("completed", "failed", "cancelled", "lost"))
_WORKER_HEALTH = frozenset(("unknown", "healthy", "quiet", "stalled", "exited"))


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

        connection = self._open()
        worker_identifier = worker_id or _new_ulid()
        canonical_cwd = cwd.resolve(strict=False)
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
                payload={"attempt": 1, "cwd": str(canonical_cwd), "role": role},
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
                payload={"attempt": attempt, "state": "starting"},
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
        safe_payload = _validated_payload(payload)
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

    async def get_worker(self, worker_id: str) -> WorkerRecord | None:
        """Return current durable state for ``worker_id``, if it exists."""

        connection = self._open()
        row = connection.execute(
            "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
        ).fetchone()
        return _worker_from_row(row) if row is not None else None

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
        if event_type not in _EVENT_TYPES:
            raise ValueError(f"Unsupported semantic event type: {event_type}")
        safe_payload = _validated_payload(payload)
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


def _validated_payload(payload: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    copied = _copy_json_object(payload)
    for key in _walk_payload_keys(copied):
        normalized_key = key.casefold()
        if normalized_key in _FORBIDDEN_PAYLOAD_KEYS or any(
            part in normalized_key for part in _FORBIDDEN_PAYLOAD_KEY_PARTS
        ):
            raise ValueError(f"Non-semantic payload field is not persistable: {key}")
    return copied


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


def _walk_payload_keys(payload: JsonValue) -> tuple[str, ...]:
    if isinstance(payload, dict):
        keys: list[str] = []
        for key, value in payload.items():
            keys.append(key)
            keys.extend(_walk_payload_keys(value))
        return tuple(keys)
    if isinstance(payload, list):
        keys = []
        for value in payload:
            keys.extend(_walk_payload_keys(value))
        return tuple(keys)
    return ()


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
