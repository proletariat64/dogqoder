import os
import sqlite3
from pathlib import Path

import pytest

from qworker.store import WorkerStore


async def test_store_round_trips_workers_attempts_and_ordered_events(
    tmp_path: Path,
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker = await store.create_worker(
        role="auditor",
        cwd=tmp_path,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )
    await store.append_event(
        worker.worker_id,
        "runtime.started",
        {"schema_version": 1, "pid": 42},
    )
    await store.append_event(
        worker.worker_id,
        "model.resolved",
        {"schema_version": 1, "model": "Qwen3.8-Max"},
    )
    await store.append_event(
        worker.worker_id,
        "runtime.exited",
        {"schema_version": 1, "exit_code": 1},
    )
    await store.append_event(
        worker.worker_id,
        "worker.state_changed",
        {"schema_version": 1, "state": "failed"},
    )
    retry = await store.start_attempt(worker.worker_id)
    await store.append_event(
        retry.worker_id,
        "runtime.started",
        {"schema_version": 1, "pid": 43},
    )

    restored = await store.get_worker(worker.worker_id)
    events = await store.events_since(worker.worker_id, since=1)

    assert restored is not None
    assert restored.attempt == 2
    assert retry.attempt == 2
    assert [event.sequence for event in events] == [2, 3, 4, 5, 6, 7]
    assert [event.type for event in events] == [
        "runtime.started",
        "model.resolved",
        "runtime.exited",
        "worker.state_changed",
        "worker.state_changed",
        "runtime.started",
    ]
    assert [event.sequence for event in await store.events_since(worker.worker_id)] == [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
    ]

    with sqlite3.connect(store.database_path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)


async def test_store_lists_persisted_workers_newest_first_with_cursors(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    store = WorkerStore(state_dir)
    assert await store.list_workers() == ()
    first = await store.create_worker(
        role="auditor",
        cwd=tmp_path,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
        worker_id="00000000000000000000000001",
    )
    await store.append_event(
        first.worker_id,
        "model.resolved",
        {"schema_version": 1, "model": "Qwen3.8-Max"},
    )
    second = await store.create_worker(
        role="coder",
        cwd=tmp_path,
        write_capability="shared_workspace",
        requested_model="qwen-coder",
        runtime_path="bundled",
        sdk_version="1.0.13",
        worker_id="00000000000000000000000002",
    )
    await store.close()

    reopened = WorkerStore(state_dir)
    listed = await reopened.list_workers()

    assert [item.worker.worker_id for item in listed] == [
        second.worker_id,
        first.worker_id,
    ]
    assert [item.event_cursor for item in listed] == [1, 2]
    await reopened.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode bits are required")
async def test_store_creates_private_state_directory_and_database(
    tmp_path: Path,
) -> None:
    store = WorkerStore(tmp_path / "state")
    await store.create_worker(
        role="auditor",
        cwd=tmp_path,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )

    assert store.state_dir.stat().st_mode & 0o777 == 0o700
    assert store.database_path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("terminal_state", ("failed", "cancelled", "lost"))
async def test_append_event_cannot_resume_without_a_new_attempt(
    tmp_path: Path,
    terminal_state: str,
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker = await store.create_worker(
        role="auditor",
        cwd=tmp_path,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )
    if terminal_state != "failed":
        await store.append_event(
            worker.worker_id,
            "worker.state_changed",
            {"schema_version": 1, "state": "running"},
        )
    await store.append_event(
        worker.worker_id,
        "worker.state_changed",
        {"schema_version": 1, "state": terminal_state},
    )

    with pytest.raises(ValueError, match="start_attempt"):
        await store.append_event(
            worker.worker_id,
            "worker.state_changed",
            {"schema_version": 1, "state": "starting"},
        )

    current = await store.get_worker(worker.worker_id)
    assert current is not None
    assert current.attempt == 1
    assert (await store.start_attempt(worker.worker_id)).attempt == 2


@pytest.mark.parametrize(
    ("event_type", "payload"),
    (
        ("assistant.message", {"schema_version": 1, "message": "raw transcript"}),
        ("assistant.message", {"schema_version": 1, "token_delta": "delta"}),
        ("assistant.message", {"schema_version": 1, "tool_input": {"path": "/tmp"}}),
        (
            "tool.started",
            {"schema_version": 1, "tool_name": "Read", "api_key": "secret"},
        ),
        ("assistant.message", {"schema_version": 1, "access_token": "secret"}),
        ("assistant.message", {"schema_version": 1, "authorization": "secret"}),
        ("assistant.message", {"schema_version": 1, "password": "secret"}),
        ("assistant.message", {"model": "Qwen3.8-Max"}),
        ("assistant.message", {"schema_version": True}),
        ("assistant.message", {"schema_version": 2}),
        ("model.resolved", {"schema_version": 1}),
        ("model.resolved", {"schema_version": 1, "model": "x" * 257}),
    ),
)
async def test_store_rejects_unversioned_or_nonsemantic_payloads(
    tmp_path: Path,
    event_type: str,
    payload: dict[str, object],
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker = await store.create_worker(
        role="auditor",
        cwd=tmp_path,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )

    with pytest.raises(ValueError, match="payload|schema_version"):
        await store.append_event(worker.worker_id, event_type, payload)  # type: ignore[arg-type]

    assert [event.type for event in await store.events_since(worker.worker_id)] == [
        "worker.created"
    ]


@pytest.mark.parametrize(
    ("event_type", "payload"),
    (
        ("model.resolved", {"schema_version": 1, "model": "sk-live-secret"}),
        (
            "model.resolved",
            {"schema_version": 1, "model": "this is a raw assistant transcript"},
        ),
        ("tool.started", {"schema_version": 1, "tool_name": "Bearer secret"}),
        ("worker.warning", {"schema_version": 1, "code": "api_key_ABC123"}),
        (
            "runtime.initialized",
            {"schema_version": 1, "runtime_version": "credential-value"},
        ),
    ),
)
async def test_store_rejects_sensitive_or_free_form_allowed_values(
    tmp_path: Path,
    event_type: str,
    payload: dict[str, object],
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker = await store.create_worker(
        role="auditor",
        cwd=tmp_path,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )

    with pytest.raises(ValueError, match="semantic event payload"):
        await store.append_event(worker.worker_id, event_type, payload)  # type: ignore[arg-type]

    assert len(await store.events_since(worker.worker_id)) == 1


async def test_create_worker_preserves_caller_workspace_with_spaced_segments(
    tmp_path: Path,
) -> None:
    caller_cwd = tmp_path / "Caller Workspace" / "My Qoder Worker Project"
    caller_cwd.mkdir(parents=True)
    store = WorkerStore(tmp_path / "state")

    worker = await store.create_worker(
        role="coder",
        cwd=caller_cwd,
        write_capability="shared_workspace",
        requested_model="qwen-coder",
        runtime_path="bundled",
        sdk_version="1.0.13",
        worker_id="worker-1",
    )

    assert worker.cwd == caller_cwd
    assert (await store.events_since(worker.worker_id))[0].payload == {
        "schema_version": 1,
        "attempt": 1,
        "cwd": str(caller_cwd),
        "role": "coder",
    }


async def test_create_worker_rejects_transcript_cwd_without_persisting(
    tmp_path: Path,
) -> None:
    transcript_cwd = tmp_path / "this is a raw assistant transcript"
    store = WorkerStore(tmp_path / "state")

    with pytest.raises(ValueError, match="worker creation field: cwd"):
        await store.create_worker(
            role="auditor",
            cwd=transcript_cwd,
            write_capability="read_only",
            requested_model="qwen-auditor",
            runtime_path="bundled",
            sdk_version="1.0.13",
            worker_id="worker-1",
        )

    assert not store.database_path.exists()
    assert await store.events_since("worker-1") == ()
    with sqlite3.connect(store.database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM workers").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone() == (0,)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    (
        ("role", "sk-live-secret"),
        ("write_capability", "sk-live-secret"),
        ("requested_model", "sk-live-secret"),
        ("runtime_path", "a raw runtime transcript"),
        ("runtime_version", "credential-value"),
        ("sdk_version", "Bearer secret"),
        ("cwd", Path("/workspace/sk-live-secret")),
        ("worker_id", "sk-live-secret"),
    ),
)
async def test_create_worker_rejects_unsafe_ingress_without_persisting(
    tmp_path: Path,
    field: str,
    invalid_value: object,
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker_arguments: dict[str, object] = {
        "role": "auditor",
        "cwd": tmp_path,
        "write_capability": "read_only",
        "requested_model": "qwen-auditor",
        "runtime_path": "bundled",
        "sdk_version": "1.0.13",
        "worker_id": "worker-1",
    }
    worker_arguments[field] = invalid_value

    with pytest.raises(ValueError, match="worker creation field"):
        await store.create_worker(**worker_arguments)  # type: ignore[arg-type]

    assert not store.database_path.exists()
    assert await store.events_since("worker-1") == ()
    with sqlite3.connect(store.database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM workers").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone() == (0,)
