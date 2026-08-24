"""Public-boundary acceptance checks for coder writes and auditor isolation."""

import asyncio
import json
import sys
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import cast

import pytest
from qoder_agent_sdk import PermissionResultAllow, PermissionResultDeny
from qoder_agent_sdk.types import ToolPermissionContext

from qworker.events import AdapterEvent, ResultEvent
from qworker.model_policy import AvailableModel
from qworker.qoder_sdk import build_auditor_options
from qworker.rpc import RPCServer
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor
from tests.fakes import FakeQoderTransport

_CLI_BOOTSTRAP = "from qworker.cli import main; raise SystemExit(main())"


class _EditingCoderTransport(FakeQoderTransport):
    """Represent one external coder that edits only its disposable workspace."""

    def __init__(self, workspace: Path, credential: str) -> None:
        report = json.dumps(
            {
                "outcome": "completed",
                "summary": f"wrote acceptance.txt while suppressing {credential}",
                "files": ["acceptance.txt"],
                "validation": [f"marker exact; credential={credential}"],
                "risks": [],
            }
        )
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(
                ResultEvent(
                    session_id="acceptance-coder-session",
                    is_error=False,
                    result=report,
                    model_usage=("Qwen3.8-Max",),
                ),
            ),
        )
        self._workspace = workspace

    async def send(self, prompt: str) -> None:
        await super().send(prompt)
        (self._workspace / "acceptance.txt").write_text(
            "coder acceptance marker\n", encoding="utf-8"
        )

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        async for event in super().messages():
            yield event


async def _run_cli(
    socket_path: Path,
    arguments: Sequence[str],
    *,
    stdin: str = "",
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
    if process.returncode != 0:
        raise AssertionError(f"qworker CLI exited {process.returncode}; expected 0")
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
    forbidden: str | None = None,
) -> dict[str, object]:
    frames = await _run_cli(
        socket_path,
        arguments,
        stdin=stdin,
        forbidden=forbidden,
    )
    assert len(frames) == 1
    return frames[0]


async def test_auditor_callback_denies_top_level_and_direct_helper_mutation(
    tmp_path: Path,
) -> None:
    """Exercise the actual SDK permission callback with nested agent IDs."""

    fixture = tmp_path / "audited.txt"
    fixture.write_text("unchanged\n", encoding="utf-8")
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    options = build_auditor_options(tmp_path)
    assert options.can_use_tool is not None

    safe_input = {"file_path": str(fixture)}
    allowed = await options.can_use_tool(
        "Read", safe_input, ToolPermissionContext(agent_id="direct-helper")
    )
    assert isinstance(allowed, PermissionResultAllow)
    assert allowed.updated_input is safe_input

    denied_tools = (
        "Write",
        "Edit",
        "NotebookEdit",
        "Bash",
        "UnknownTool",
        "mcp__filesystem__write_file",
    )
    for agent_id in (None, "direct-helper"):
        context = ToolPermissionContext(agent_id=agent_id)
        for tool_name in denied_tools:
            denied = await options.can_use_tool(
                tool_name,
                {"file_path": str(tmp_path / "blocked.txt"), "command": "touch x"},
                context,
            )
            assert isinstance(denied, PermissionResultDeny)

    nested_recursion = await options.can_use_tool(
        "Agent",
        {"prompt": "spawn another nested writer"},
        ToolPermissionContext(agent_id="direct-helper"),
    )
    assert isinstance(nested_recursion, PermissionResultDeny)
    after = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    assert after == before


async def test_public_coder_edits_disposable_workspace_without_leaking_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive a disposable coder through CLI, events, result, and durable state."""

    credential = "opaque-qworker-acceptance-credential"
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", credential)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    state_dir = tmp_path / "state"
    transport = _EditingCoderTransport(workspace, credential)
    store = WorkerStore(state_dir)
    supervisor = Supervisor(
        store,
        lambda _cwd: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        coder_transport_factory=lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    try:
        objective = f"write acceptance marker; password={credential}"
        accepted = await _one_cli_frame(
            socket_path,
            ("spawn", "--role", "coder", "--cwd", str(workspace), "--json"),
            stdin=objective,
            forbidden=credential,
        )
        worker_id = str(accepted["worker_id"])
        events = await _run_cli(
            socket_path,
            ("watch", worker_id, "--since", "0", "--follow", "--json"),
            forbidden=credential,
        )
        result = await _one_cli_frame(
            socket_path,
            ("result", worker_id, "--json"),
            forbidden=credential,
        )

        assert (workspace / "acceptance.txt").read_text(encoding="utf-8") == (
            "coder acceptance marker\n"
        )
        assert not (workspace / ".git").exists()
        assert result["outcome"] == "completed"
        assert result["files"] == ["acceptance.txt"]
        assert result["requested_model"] == "qwen-coder"
        assert result["resolved_model"] == "Qwen3.8-Max"
        assert result["actual_models"] == ["Qwen3.8-Max"]
        rendered_events = json.dumps(events, sort_keys=True)
        rendered_result = json.dumps(result, sort_keys=True)
        assert credential not in rendered_events
        assert credential not in rendered_result
        assert "[REDACTED]" in rendered_result

        for durable_file in state_dir.rglob("*"):
            if durable_file.is_file():
                assert credential.encode() not in durable_file.read_bytes()
        assert credential not in json.dumps(transport.calls)
    finally:
        await server.close()
        await supervisor.close()
