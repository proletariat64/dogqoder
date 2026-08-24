"""Public-interface acceptance checks for stop, loss, and explicit resume."""

import asyncio
import json
import sys
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import cast

from qworker.control import ControlCallbacks, PermissionRequest
from qworker.events import AdapterEvent, ResultEvent
from qworker.model_policy import AvailableModel
from qworker.rpc import RPCServer
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor
from tests.fakes import SUCCESSFUL_AUDIT_REPORT, FakeQoderTransport

_CLI_BOOTSTRAP = "from qworker.cli import main; raise SystemExit(main())"


class _HangingTransport(FakeQoderTransport):
    """Expose one live child whose EOF can be normal or unexpected."""

    def __init__(self) -> None:
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(),
        )
        self.finish = asyncio.Event()
        self.callbacks: ControlCallbacks | None = None
        self.child_live = True
        self.interrupted = False

    def bind_control(self, callbacks: ControlCallbacks) -> None:
        self.callbacks = callbacks

    async def interrupt(self) -> None:
        self.interrupted = True
        self.finish.set()

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        await self.finish.wait()
        if False:  # pragma: no cover - retain async-generator shape
            yield ResultEvent("unused", False, None)

    async def disconnect(self) -> None:
        self.child_live = False
        await super().disconnect()

    def abort(self) -> None:
        self.child_live = False
        super().abort()


class _ResumedTransport(FakeQoderTransport):
    """Capture resume construction and complete only after test release."""

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

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        await self._gate.wait()
        async for event in super().messages():
            yield event


async def _run_cli(
    socket_path: Path,
    arguments: Sequence[str],
    *,
    stdin: str = "",
    expected_exit: int = 0,
    timeout: float = 5.0,
) -> list[dict[str, object]]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        _CLI_BOOTSTRAP,
        "--socket",
        str(socket_path),
        *arguments,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, _stderr = await asyncio.wait_for(
            process.communicate(stdin.encode()), timeout=timeout
        )
    except TimeoutError:
        process.kill()
        await process.wait()
        raise AssertionError(
            "qworker CLI process exceeded its acceptance timeout"
        ) from None
    if process.returncode != expected_exit:
        raise AssertionError(
            f"qworker CLI exited {process.returncode}; expected {expected_exit}"
        )
    frames: list[dict[str, object]] = []
    for line in stdout.splitlines():
        decoded = json.loads(line)
        if not isinstance(decoded, dict):
            raise TypeError("qworker CLI emitted a non-object JSON frame")
        frames.append(cast(dict[str, object], decoded))
    return frames


async def _one_cli_frame(
    socket_path: Path,
    arguments: Sequence[str],
    *,
    stdin: str = "",
    expected_exit: int = 0,
) -> dict[str, object]:
    frames = await _run_cli(
        socket_path,
        arguments,
        stdin=stdin,
        expected_exit=expected_exit,
    )
    assert len(frames) == 1
    return frames[0]


async def _wait_for_state(
    socket_path: Path, worker_id: str, expected: str
) -> dict[str, object]:
    for _ in range(100):
        status = await _one_cli_frame(socket_path, ("status", worker_id, "--json"))
        if status.get("state") == expected:
            return status
        await asyncio.sleep(0)
    raise AssertionError(f"worker did not reach {expected}")


async def test_public_stop_preserves_edits_and_disconnects_live_child(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    preserved = workspace / "preserved.txt"
    preserved.write_text("existing edit\n", encoding="utf-8")
    transport = _HangingTransport()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
        stop_timeout=0.05,
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    try:
        accepted = await _one_cli_frame(
            socket_path,
            ("spawn", "--role", "auditor", "--cwd", str(workspace), "--json"),
            stdin="wait for an explicit stop",
        )
        worker_id = str(accepted["worker_id"])
        await _wait_for_state(socket_path, worker_id, "running")

        nested_stop = await _one_cli_frame(
            socket_path,
            (
                "stop",
                worker_id,
                "--agent-id",
                "nested-helper",
                "--json",
            ),
            expected_exit=1,
        )
        assert cast(dict[str, object], nested_stop["error"])["code"] == (
            "unsupported_operation"
        )

        stopped = await _one_cli_frame(socket_path, ("stop", worker_id, "--json"))
        assert stopped["state"] == "cancelled"
        assert stopped["health"] == "exited"
        assert stopped["force"] is False
        assert transport.interrupted is True
        assert transport.child_live is False
        assert preserved.read_text(encoding="utf-8") == "existing edit\n"
    finally:
        await server.close()
        await supervisor.close()


async def test_public_loss_expires_live_approval_and_rejects_response(
    tmp_path: Path,
) -> None:
    transport = _HangingTransport()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    try:
        accepted = await _one_cli_frame(
            socket_path,
            ("spawn", "--role", "auditor", "--cwd", str(tmp_path), "--json"),
            stdin="lose this attempt after requesting approval",
        )
        worker_id = str(accepted["worker_id"])
        await _wait_for_state(socket_path, worker_id, "running")
        assert transport.callbacks is not None
        approval = asyncio.create_task(
            transport.callbacks.request_permission(
                PermissionRequest(
                    tool_name="Read",
                    agent_id=None,
                    display_message="Inspect one safe fixture?",
                )
            )
        )
        pending_status = await _wait_for_state(
            socket_path, worker_id, "requires_action"
        )
        pending = cast(list[dict[str, object]], pending_status["pending_approvals"])
        request_id = str(pending[0]["request_id"])

        terminal_watch = asyncio.create_task(
            _run_cli(
                socket_path,
                ("watch", worker_id, "--since", "0", "--follow", "--json"),
            )
        )
        await asyncio.sleep(0.05)
        transport.finish.set()
        events = await terminal_watch
        assert events[-1]["payload"] == {"schema_version": 1, "state": "lost"}
        assert (await approval).action == "deny"
        lost = await _one_cli_frame(socket_path, ("status", worker_id, "--json"))
        assert lost["state"] == "lost"
        assert lost["health"] == "exited"
        assert lost["pending_approvals"] == []

        rejected = await _one_cli_frame(
            socket_path,
            ("respond", worker_id, request_id, "--json"),
            stdin='{"action":"allow"}',
            expected_exit=1,
        )
        assert cast(dict[str, object], rejected["error"])["code"] == (
            "approval_not_pending"
        )
    finally:
        await server.close()
        await supervisor.close()


async def test_public_resume_uses_same_id_cwd_session_and_new_attempt(
    tmp_path: Path,
) -> None:
    workspace = (tmp_path / "workspace").resolve()
    workspace.mkdir()
    preserved = workspace / "partial-edit.txt"
    preserved.write_text("partial writer state\n", encoding="utf-8")
    failed_report = (
        '{"outcome":"failed","summary":"retry needed","files":[],'
        '"validation":[],"risks":[],"verdict":"changes_required",'
        '"confirmed":[],"findings":[],"required_changes":["resume"]}'
    )
    initial = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(
            ResultEvent(
                session_id="acceptance-resume-session",
                is_error=True,
                result=failed_report,
                model_usage=("Qwen3.8-Max",),
            ),
        ),
    )
    resume_gate = asyncio.Event()
    resumed = _ResumedTransport(resume_gate, session_id="acceptance-resume-session")
    constructions: list[tuple[Path, str]] = []

    def resume_factory(cwd: Path, session_id: str) -> _ResumedTransport:
        constructions.append((cwd, session_id))
        return resumed

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: initial,
        sdk_version="1.0.13",
        resume_transport_factory=resume_factory,
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    try:
        accepted = await _one_cli_frame(
            socket_path,
            ("spawn", "--role", "auditor", "--cwd", str(workspace), "--json"),
            stdin="produce one resumable failed attempt",
        )
        worker_id = str(accepted["worker_id"])
        await _run_cli(
            socket_path,
            ("watch", worker_id, "--since", "0", "--follow", "--json"),
        )
        failed = await _one_cli_frame(socket_path, ("status", worker_id, "--json"))
        assert failed["state"] == "failed"
        assert failed["attempt"] == 1

        resumed_acceptance = await _one_cli_frame(
            socket_path, ("resume", worker_id, "--json")
        )
        assert resumed_acceptance["worker_id"] == worker_id
        assert resumed_acceptance["cwd"] == str(workspace)
        assert resumed_acceptance["attempt"] == 2
        assert resumed_acceptance["state"] == "starting"
        running = await _wait_for_state(socket_path, worker_id, "running")
        assert running["attempt"] == 2
        assert constructions == [(workspace, "acceptance-resume-session")]
        assert len(resumed.sent_prompts) == 1
        recovery_prompt = resumed.sent_prompts[0]
        assert "prior Qoder process ended" in recovery_prompt
        assert "Recheck the current workspace state" in recovery_prompt
        assert preserved.read_text(encoding="utf-8") == "partial writer state\n"

        resume_gate.set()
        await _run_cli(
            socket_path,
            (
                "watch",
                worker_id,
                "--since",
                str(resumed_acceptance["event_cursor"]),
                "--follow",
                "--json",
            ),
        )
        completed = await _one_cli_frame(socket_path, ("status", worker_id, "--json"))
        assert completed["state"] == "completed"
        assert completed["attempt"] == 2
    finally:
        await server.close()
        await supervisor.close()
