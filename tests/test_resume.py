import asyncio
import json
from collections.abc import AsyncIterator
from io import StringIO
from pathlib import Path
from typing import Literal

import pytest

from qworker.cli import run
from qworker.control import PermissionRequest
from qworker.domain import AuditContract
from qworker.events import AdapterEvent, ResultEvent
from qworker.lifecycle import WorkerState
from qworker.model_policy import AvailableModel
from qworker.qoder_sdk import build_auditor_options
from qworker.rpc import RPCClientError, RPCServer, call
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor, SupervisorError
from tests.fakes import SUCCESSFUL_AUDIT_REPORT, FakeQoderTransport


class GatedResumeTransport(FakeQoderTransport):
    def __init__(self, gate: asyncio.Event, *, session_id: str) -> None:
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(
                ResultEvent(
                    session_id=session_id,
                    is_error=False,
                    result=SUCCESSFUL_AUDIT_REPORT,
                    model_usage=("Qwen3.8-Max",),
                ),
            ),
        )
        self._gate = gate
        self.session_id = session_id

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        await self._gate.wait()
        async for event in super().messages():
            yield event


class GatedFailedTransport(FakeQoderTransport):
    def __init__(self, gate: asyncio.Event, *, session_id: str) -> None:
        failed_report = (
            '{"outcome":"failed","summary":"prior process failed",'
            '"files":[],"validation":[],"risks":[],"verdict":"changes_required",'
            '"confirmed":[],"findings":[],"required_changes":["continue"]}'
        )
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(
                ResultEvent(
                    session_id=session_id,
                    is_error=True,
                    result=failed_report,
                    model_usage=("Qwen3.8-Max",),
                ),
            ),
        )
        self._gate = gate
        self.steering_messages: list[str] = []

    async def steer(
        self,
        message: str,
        *,
        priority: Literal["now", "next", "later"],
        message_id: str,
    ) -> None:
        del priority, message_id
        self.steering_messages.append(message)

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        await self._gate.wait()
        async for event in super().messages():
            yield event


async def _create_terminal_worker(
    store: WorkerStore,
    cwd: Path,
    *,
    state: WorkerState,
    session_id: str | None = "session-resume",
) -> str:
    worker = await store.create_worker(
        role="auditor",
        cwd=cwd,
        write_capability="read_only",
        requested_model="qwen-auditor",
        runtime_path="bundled",
        sdk_version="1.0.13",
    )
    if state == "starting":
        return worker.worker_id
    await store.append_event(
        worker.worker_id,
        "worker.state_changed",
        {"schema_version": 1, "state": "running"},
    )
    if state == "running":
        return worker.worker_id
    if state == "requires_action":
        await store.append_event(
            worker.worker_id,
            "worker.state_changed",
            {"schema_version": 1, "state": "requires_action"},
        )
        return worker.worker_id
    if session_id is not None:
        await store.record_result(
            worker.worker_id,
            outcome="failed",
            result_summary={"outcome": "failed", "summary": "prior attempt"},
            resolved_model="Qwen3.8-Max",
            actual_models=("Qwen3.8-Max",),
            session_id=session_id,
            nested_state="unknown",
            warnings=(),
        )
    await store.append_event(
        worker.worker_id,
        "worker.health_changed",
        {"schema_version": 1, "health": "exited"},
    )
    await store.append_event(
        worker.worker_id,
        "worker.state_changed",
        {"schema_version": 1, "state": state},
    )
    return worker.worker_id


def test_resume_transport_sets_public_sdk_option(tmp_path: Path) -> None:
    options = build_auditor_options(tmp_path, resume="session-resume")

    assert options.cwd == tmp_path
    assert options.resume == "session-resume"


async def test_failed_worker_resumes_as_new_attempt(tmp_path: Path) -> None:
    store = WorkerStore(tmp_path / "state")
    worker_id = await _create_terminal_worker(store, tmp_path, state="failed")
    gate = asyncio.Event()
    resumed = GatedResumeTransport(gate, session_id="session-resume")
    constructions: list[tuple[Path, str]] = []

    def resume_factory(cwd: Path, session_id: str) -> GatedResumeTransport:
        constructions.append((cwd, session_id))
        return resumed

    supervisor = Supervisor(
        store,
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
        resume_transport_factory=resume_factory,
    )

    accepted = await supervisor.resume(worker_id)

    assert accepted["worker_id"] == worker_id
    assert accepted["state"] == "starting"
    assert accepted["attempt"] == 2
    assert constructions == []

    gate.set()
    await supervisor.close()


@pytest.mark.parametrize("state", ("lost", "failed", "cancelled"))
async def test_resume_accepts_each_eligible_terminal_state(
    tmp_path: Path,
    state: Literal["lost", "failed", "cancelled"],
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker_id = await _create_terminal_worker(store, tmp_path, state=state)
    gate = asyncio.Event()
    supervisor = Supervisor(
        store,
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
        resume_transport_factory=lambda _cwd, session_id: GatedResumeTransport(
            gate, session_id=session_id
        ),
    )

    accepted = await supervisor.resume(worker_id)

    assert accepted["attempt"] == 2
    assert accepted["state"] == "starting"
    gate.set()
    await supervisor.close()


@pytest.mark.parametrize(
    ("state", "session_id"),
    (
        ("starting", "session-resume"),
        ("running", "session-resume"),
        ("requires_action", "session-resume"),
        ("completed", "session-resume"),
        ("lost", None),
    ),
)
async def test_resume_rejects_ineligible_state_or_missing_session(
    tmp_path: Path,
    state: WorkerState,
    session_id: str | None,
) -> None:
    store = WorkerStore(tmp_path / "state")
    worker_id = await _create_terminal_worker(
        store,
        tmp_path,
        state=state,
        session_id=session_id,
    )
    supervisor = Supervisor(
        store,
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
        resume_transport_factory=lambda _cwd, resumed_session: GatedResumeTransport(
            asyncio.Event(), session_id=resumed_session
        ),
    )

    with pytest.raises(SupervisorError) as rejected:
        await supervisor.resume(worker_id)

    assert rejected.value.code == "resume_not_possible"
    assert (await supervisor.status(worker_id))["attempt"] == 1
    await supervisor.close()


async def test_resume_sends_recovery_only_and_does_not_replay_prior_controls(
    tmp_path: Path,
) -> None:
    first_gate = asyncio.Event()
    prior = GatedFailedTransport(first_gate, session_id="session-history")
    resume_gate = asyncio.Event()
    resumed = GatedResumeTransport(resume_gate, session_id="session-history")
    constructions: list[tuple[Path, str]] = []

    def resume_factory(cwd: Path, session_id: str) -> GatedResumeTransport:
        constructions.append((cwd, session_id))
        return resumed

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: prior,
        sdk_version="1.0.13",
        resume_transport_factory=resume_factory,
    )
    accepted = await supervisor.spawn(
        AuditContract(objective="OLD_ORIGINAL_OBJECTIVE", cwd=tmp_path)
    )
    worker_id = accepted["worker_id"]
    assert isinstance(worker_id, str)
    while (await supervisor.status(worker_id))["state"] != "running":
        await asyncio.sleep(0)

    assert prior.control_callbacks is not None
    old_approval = asyncio.create_task(
        prior.control_callbacks.request_permission(
            PermissionRequest(
                tool_name="Read",
                agent_id=None,
                display_message="OLD_APPROVAL_BODY",
            )
        )
    )
    while (await supervisor.status(worker_id))["state"] != "requires_action":
        await asyncio.sleep(0)
    await supervisor.steer(worker_id, "OLD_STEERING_BODY", priority="later")

    first_gate.set()
    terminal_events = [
        event async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]
    assert terminal_events[-1]["type"] == "worker.state_changed"
    assert (await supervisor.status(worker_id))["state"] == "failed"
    assert (await old_approval).action == "deny"
    assert prior.steering_messages == ["OLD_STEERING_BODY"]

    resumed_acceptance = await supervisor.resume(worker_id)
    while not resumed.sent_prompts:
        await asyncio.sleep(0)

    assert resumed_acceptance["attempt"] == 2
    assert constructions == [(tmp_path.resolve(), "session-history")]
    assert len(resumed.sent_prompts) == 1
    recovery_prompt = resumed.sent_prompts[0]
    assert "prior Qoder process ended" in recovery_prompt
    assert "Recheck the current workspace state" in recovery_prompt
    assert "Do not assume interrupted work completed" in recovery_prompt
    assert "OLD_ORIGINAL_OBJECTIVE" not in recovery_prompt
    assert "OLD_APPROVAL_BODY" not in recovery_prompt
    assert "OLD_STEERING_BODY" not in recovery_prompt
    assert (await supervisor.status(worker_id))["pending_approvals"] == []

    resume_gate.set()
    await supervisor.close()


async def test_resume_rejects_missing_original_working_directory(
    tmp_path: Path,
) -> None:
    original_cwd = tmp_path / "original"
    original_cwd.mkdir()
    store = WorkerStore(tmp_path / "state")
    worker_id = await _create_terminal_worker(
        store,
        original_cwd,
        state="lost",
    )
    moved_cwd = tmp_path / "moved"
    original_cwd.rename(moved_cwd)
    constructions: list[tuple[Path, str]] = []

    def resume_factory(cwd: Path, session_id: str) -> GatedResumeTransport:
        constructions.append((cwd, session_id))
        raise AssertionError("invalid cwd must be rejected before construction")

    supervisor = Supervisor(
        store,
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
        resume_transport_factory=resume_factory,
    )

    with pytest.raises(SupervisorError) as rejected:
        await supervisor.resume(worker_id)

    assert rejected.value.code == "resume_not_possible"
    assert constructions == []
    assert (await supervisor.status(worker_id))["attempt"] == 1
    await supervisor.close()


async def test_json_cli_resumes_through_closed_rpc_method(tmp_path: Path) -> None:
    store = WorkerStore(tmp_path / "state")
    worker_id = await _create_terminal_worker(store, tmp_path, state="cancelled")
    gate = asyncio.Event()
    supervisor = Supervisor(
        store,
        lambda _: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        sdk_version="1.0.13",
        resume_transport_factory=lambda _cwd, session_id: GatedResumeTransport(
            gate, session_id=session_id
        ),
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    stdout = StringIO()

    exit_code = await run(
        ["--socket", str(socket_path), "resume", worker_id, "--json"],
        stdin=StringIO(),
        stdout=stdout,
    )

    output = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert output["worker_id"] == worker_id
    assert output["state"] == "starting"
    assert output["attempt"] == 2

    with pytest.raises(RPCClientError) as rejected:
        await call(
            socket_path,
            "resume",
            {"worker_id": worker_id, "unexpected": True},
        )
    assert rejected.value.code == "invalid_request"

    gate.set()
    await server.close()
    await supervisor.close()
