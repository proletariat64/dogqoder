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
    await store.append_event(worker.worker_id, "runtime.started", {"pid": 42})
    await store.append_event(
        worker.worker_id,
        "model.resolved",
        {"model": "Qwen3.8-Max"},
    )
    await store.append_event(worker.worker_id, "runtime.exited", {"exit_code": 1})
    await store.append_event(
        worker.worker_id, "worker.state_changed", {"state": "failed"}
    )
    retry = await store.start_attempt(worker.worker_id)
    await store.append_event(retry.worker_id, "runtime.started", {"pid": 43})

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
