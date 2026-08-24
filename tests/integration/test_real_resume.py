"""Public-interface acceptance checks for stop, loss, and explicit resume."""

import asyncio
import json
import os
import sys
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from qworker.control import ControlCallbacks, PermissionRequest
from qworker.events import AdapterEvent, ResultEvent
from qworker.model_policy import AvailableModel
from qworker.preflight import RuntimePreflight
from qworker.qoder_sdk import (
    QoderPreflightBackend,
    QoderSDKTransport,
    create_default_transport,
)
from qworker.rpc import RPCServer
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor
from tests.fakes import SUCCESSFUL_AUDIT_REPORT, FakeQoderTransport
from tests.real_qoder import require_real_qoder_credentials

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
    forbidden: str | None = None,
    timeout: float = 5.0,
) -> list[dict[str, object]]:
    command = (
        sys.executable,
        "-c",
        _CLI_BOOTSTRAP,
        "--socket",
        str(socket_path),
        *arguments,
    )
    if forbidden is not None:
        assert forbidden not in "\0".join(command)
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
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
    if forbidden is not None:
        assert forbidden.encode() not in stdout
        assert forbidden.encode() not in stderr
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
    forbidden: str | None = None,
    timeout: float = 5.0,
) -> dict[str, object]:
    frames = await _run_cli(
        socket_path,
        arguments,
        stdin=stdin,
        expected_exit=expected_exit,
        forbidden=forbidden,
        timeout=timeout,
    )
    assert len(frames) == 1
    return frames[0]


async def _wait_for_state(
    socket_path: Path,
    worker_id: str,
    expected: str,
    *,
    forbidden: str | None = None,
    timeout: float = 5.0,
) -> dict[str, object]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        status = await _one_cli_frame(
            socket_path,
            ("status", worker_id, "--json"),
            forbidden=forbidden,
        )
        if status.get("state") == expected:
            return status
        await asyncio.sleep(0.05)
    raise AssertionError(f"worker did not reach {expected}")


@dataclass(frozen=True, slots=True)
class _OwnedChildIdentity:
    pid: int
    parent_pid: int
    start_time_ticks: int


def _read_process_identity(
    pid: int, *, proc_root: Path = Path("/proc")
) -> _OwnedChildIdentity:
    """Read stable identity fields only; never inspect process arguments."""

    stat = (proc_root / str(pid) / "stat").read_text(encoding="utf-8")
    closing_paren = stat.rfind(")")
    if closing_paren < 0:
        raise AssertionError("owned child process identity was malformed")
    fields = stat[closing_paren + 2 :].split()
    if len(fields) < 20:
        raise AssertionError("owned child process identity was incomplete")
    stat_pid = int(stat.split(" ", 1)[0])
    if stat_pid != pid:
        raise AssertionError("owned child process identity did not match")
    return _OwnedChildIdentity(
        pid=pid,
        parent_pid=int(fields[1]),
        start_time_ticks=int(fields[19]),
    )


def _owned_sdk_child_identity(
    transport: QoderSDKTransport, *, proc_root: Path = Path("/proc")
) -> _OwnedChildIdentity:
    client = getattr(transport, "_client", None)
    query = getattr(client, "_query", None)
    candidates = (
        getattr(query, "transport", None),
        getattr(client, "_transport", None),
    )
    for candidate in candidates:
        process = getattr(candidate, "_process", None)
        pid = getattr(process, "pid", None)
        if isinstance(pid, int) and pid > 0:
            identity = _read_process_identity(pid, proc_root=proc_root)
            if identity.parent_pid != os.getpid():
                raise AssertionError("SDK process is not owned by this test process")
            return identity
    raise AssertionError("SDK-owned QoderCLI child process was unavailable")


def _owned_child_is_gone(
    identity: _OwnedChildIdentity, *, proc_root: Path = Path("/proc")
) -> bool:
    try:
        current = _read_process_identity(identity.pid, proc_root=proc_root)
    except FileNotFoundError:
        return True
    return current.start_time_ticks != identity.start_time_ticks


def _workspace_snapshot(workspace: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(workspace)): path.read_bytes()
        for path in workspace.rglob("*")
        if path.is_file()
    }


def test_owned_child_identity_uses_only_sdk_process_and_proc_stat(
    tmp_path: Path,
) -> None:
    """Regress exact-child tracking without process scans or command arguments."""

    pid = 43210
    proc_dir = tmp_path / str(pid)
    proc_dir.mkdir()

    def write_stat(start_time_ticks: int) -> None:
        fields = ["S", str(os.getpid()), *("0" for _ in range(17))]
        fields.append(str(start_time_ticks))
        (proc_dir / "stat").write_text(
            f"{pid} (Qoder CLI worker) {' '.join(fields)}\n",
            encoding="utf-8",
        )

    write_stat(987_654)
    sdk_process = SimpleNamespace(pid=pid)
    sdk_transport = SimpleNamespace(_process=sdk_process)
    client = SimpleNamespace(
        _query=SimpleNamespace(transport=sdk_transport),
        _transport=None,
    )
    transport = QoderSDKTransport(client)

    identity = _owned_sdk_child_identity(transport, proc_root=tmp_path)
    assert identity == _OwnedChildIdentity(pid, os.getpid(), 987_654)
    assert _owned_child_is_gone(identity, proc_root=tmp_path) is False

    write_stat(987_655)
    assert _owned_child_is_gone(identity, proc_root=tmp_path) is True


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


@pytest.mark.real_qoder
async def test_real_normal_stop_preserves_edit_and_reaps_owned_child_ac11(
    tmp_path: Path,
) -> None:
    """Stop one real auditor normally and verify its exact owned child is gone."""

    require_real_qoder_credentials()
    credential = os.environ["QODER_PERSONAL_ACCESS_TOKEN"]
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    preserved = workspace / "preserved.txt"
    preserved.write_text("disposable edit must survive\n", encoding="utf-8")
    before = _workspace_snapshot(workspace)
    created_transports: list[QoderSDKTransport] = []

    def transport_factory(cwd: Path) -> QoderSDKTransport:
        transport = create_default_transport(cwd)
        created_transports.append(transport)
        return transport

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        transport_factory,
        sdk_version=version("qoder-agent-sdk"),
        preflight=RuntimePreflight(QoderPreflightBackend()).run,
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    try:
        async with asyncio.timeout(190):
            accepted = await _one_cli_frame(
                socket_path,
                ("spawn", "--role", "auditor", "--cwd", str(workspace), "--json"),
                stdin=(
                    "Read preserved.txt, then remain active by repeatedly inspecting "
                    "that same file until an explicit stop arrives. Do not edit files "
                    "and do not return a final report before the stop."
                ),
                forbidden=credential,
                timeout=45,
            )
            worker_id = str(accepted["worker_id"])
            running = await _wait_for_state(
                socket_path,
                worker_id,
                "running",
                forbidden=credential,
                timeout=45,
            )
            if len(created_transports) != 1:
                raise AssertionError("expected exactly one SDK worker transport")
            child = _owned_sdk_child_identity(created_transports[0])
            stopped = await _one_cli_frame(
                socket_path,
                ("stop", worker_id, "--json"),
                forbidden=credential,
                timeout=45,
            )

        evidence = {
            "observed_running": running.get("state") == "running",
            "normal_cancel": stopped.get("state") == "cancelled",
            "health_exited": stopped.get("health") == "exited",
            "not_forced": stopped.get("force") is False,
            "workspace_preserved": _workspace_snapshot(workspace) == before,
            "owned_child_gone": _owned_child_is_gone(child),
            "credential_absent": credential
            not in json.dumps((accepted, running, stopped), sort_keys=True),
        }
        if not all(evidence.values()):
            pytest.fail(
                f"AC11 live evidence: {json.dumps(evidence, sort_keys=True)}",
                pytrace=False,
            )
    finally:
        await server.close()
        await supervisor.close()
