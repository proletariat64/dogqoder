import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from qworker.coder import CoderContract
from qworker.events import AdapterEvent, ResultEvent
from qworker.model_policy import AvailableModel
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor
from qworker.workspace import canonical_workspace, classify_workspace_overlap
from tests.fakes import FakeQoderTransport


@pytest.mark.parametrize(
    ("requested_name", "live_name", "expected"),
    (
        ("workspace", "workspace", "same"),
        ("workspace/child", "workspace", "ancestor"),
        ("workspace", "workspace/child", "descendant"),
        ("workspace/left", "workspace/right", None),
    ),
)
def test_workspace_overlap_uses_canonical_path_relationships(
    tmp_path: Path,
    requested_name: str,
    live_name: str,
    expected: str | None,
) -> None:
    requested = tmp_path / requested_name
    live = tmp_path / live_name
    requested.mkdir(parents=True, exist_ok=True)
    live.mkdir(parents=True, exist_ok=True)

    assert classify_workspace_overlap(requested, live) == expected


def test_workspace_overlap_resolves_symlinks(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    child = workspace / "child"
    child.mkdir(parents=True)
    alias = tmp_path / "workspace-alias"
    alias.symlink_to(workspace, target_is_directory=True)

    assert canonical_workspace(alias) == workspace
    assert classify_workspace_overlap(alias, workspace) == "same"
    assert classify_workspace_overlap(alias / "child", workspace) == "ancestor"


class _GatedCoderTransport(FakeQoderTransport):
    def __init__(self, gate: asyncio.Event, *, session_id: str) -> None:
        report = (
            '{"outcome":"completed","summary":"done","files":[],'
            '"validation":["fake validation passed"],"risks":[]}'
        )
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(
                ResultEvent(
                    session_id=session_id,
                    is_error=False,
                    result=report,
                    model_usage=("Qwen3.8-Max",),
                ),
            ),
        )
        self._gate = gate

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        await self._gate.wait()
        async for event in super().messages():
            yield event


async def test_overlap_warning_is_visible_durable_and_never_rejects_spawn(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    git_dir = workspace / ".git"
    git_dir.mkdir()
    head = git_dir / "HEAD"
    head.write_text("ref: refs/heads/main\n", encoding="utf-8")
    caller_change = workspace / "caller-change.txt"
    caller_change.write_text("preserve me\n", encoding="utf-8")
    original_inventory = tuple(
        sorted(path.relative_to(workspace) for path in workspace.rglob("*"))
    )
    original_head = head.read_text(encoding="utf-8")
    original_change = caller_change.read_text(encoding="utf-8")

    gates = (asyncio.Event(), asyncio.Event())
    transports = iter(
        (
            _GatedCoderTransport(gates[0], session_id="coder-session-1"),
            _GatedCoderTransport(gates[1], session_id="coder-session-2"),
        )
    )
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        coder_transport_factory=lambda _: next(transports),
        sdk_version="1.0.13",
    )

    first = await supervisor.spawn(
        CoderContract(objective="first change", cwd=workspace)
    )
    second = await supervisor.spawn(
        CoderContract(objective="second change", cwd=workspace)
    )

    warning = {
        "code": "write_conflict_warning",
        "worker_id": first["worker_id"],
        "cwd": str(workspace),
        "relation": "same",
    }
    assert second["state"] == "starting"
    assert second["role"] == "coder"
    assert second["warnings"] == [warning]
    second_status = await supervisor.status(str(second["worker_id"]))
    assert second_status["write_capability"] == "shared_workspace"
    assert second_status["warnings"] == ["write_conflict_warning"]
    warning_events = [
        event
        async for event in supervisor.watch(
            str(second["worker_id"]), since=0, follow=False
        )
        if event["type"] == "worker.warning"
    ]
    assert [event["payload"] for event in warning_events] == [
        {"schema_version": 1, **warning}
    ]

    assert (
        tuple(sorted(path.relative_to(workspace) for path in workspace.rglob("*")))
        == original_inventory
    )
    assert head.read_text(encoding="utf-8") == original_head
    assert caller_change.read_text(encoding="utf-8") == original_change
    assert not any(path.name.endswith(".lock") for path in workspace.rglob("*"))
    assert not (workspace / ".git" / "worktrees").exists()

    for gate in gates:
        gate.set()
    await asyncio.wait_for(
        _wait_for_terminal(supervisor, str(second["worker_id"])),
        timeout=1,
    )
    completed_warning_events = [
        event
        async for event in supervisor.watch(
            str(second["worker_id"]), since=0, follow=False
        )
        if event["type"] == "worker.warning"
    ]
    assert len(completed_warning_events) == 1
    await supervisor.close()


async def _wait_for_terminal(supervisor: Supervisor, worker_id: str) -> None:
    async for _event in supervisor.watch(worker_id, since=0, follow=True):
        pass
