"""Public-boundary acceptance checks for coder writes and auditor isolation."""

import asyncio
import json
import os
import secrets
import sys
from collections.abc import AsyncIterator, Sequence
from importlib.metadata import version
from pathlib import Path
from typing import Any, cast

import pytest
from qoder_agent_sdk import (
    PermissionResultAllow,
    PermissionResultDeny,
    QoderAgentOptions,
    QoderSDKClient,
)
from qoder_agent_sdk.types import ToolPermissionContext

from qworker.events import AdapterEvent, AssistantEvent, ResultEvent
from qworker.model_policy import AvailableModel
from qworker.preflight import RuntimePreflight
from qworker.qoder_sdk import (
    QoderPreflightBackend,
    QoderSDKTransport,
    build_auditor_options,
    build_configured_auditor_options,
    create_coder_transport,
    create_default_transport,
)
from qworker.rpc import RPCServer
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor
from tests.fakes import FakeQoderTransport
from tests.real_qoder import require_real_qoder_credentials

_CLI_BOOTSTRAP = "from qworker.cli import main; raise SystemExit(main())"


def _valid_coder_report() -> str:
    return json.dumps(
        {
            "outcome": "completed",
            "summary": "synthetic edit completed",
            "files": ["marker.txt"],
            "validation": ["marker bytes verified"],
            "risks": [],
        }
    )


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


class _IncompleteCoderReportTransport(FakeQoderTransport):
    """Apply one edit, then emit the smallest non-contract coder result."""

    def __init__(self, workspace: Path) -> None:
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(
                AssistantEvent(
                    text=(_valid_coder_report(),),
                    tools=(),
                    model="Qwen3.8-Max",
                ),
                ResultEvent(
                    session_id="synthetic-ac5-session",
                    is_error=False,
                    result="{}",
                    model_usage=("Qwen3.8-Max",),
                ),
            ),
        )
        self._workspace = workspace

    async def send(self, prompt: str) -> None:
        await super().send(prompt)
        (self._workspace / "marker.txt").write_text(
            "synthetic-marker\n", encoding="utf-8"
        )


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
    timeout: float = 5.0,
) -> dict[str, object]:
    frames = await _run_cli(
        socket_path,
        arguments,
        stdin=stdin,
        forbidden=forbidden,
        timeout=timeout,
    )
    assert len(frames) == 1
    return frames[0]


def _safe_coder_evidence(
    result: dict[str, object], workspace: Path, marker: str
) -> dict[str, bool]:
    files = result.get("files")
    validation = result.get("validation")
    actual_models = result.get("actual_models")
    marker_path = workspace / "marker.txt"
    try:
        marker_exact = marker_path.read_text(encoding="utf-8") == f"{marker}\n"
    except OSError:
        marker_exact = False
    return {
        "result_completed": result.get("outcome") == "completed",
        "marker_exact": marker_exact,
        "reported_marker_file": isinstance(files, list) and "marker.txt" in files,
        "structured_validation": isinstance(validation, list) and bool(validation),
        "resolved_qwen_max": result.get("resolved_model") == "Qwen3.8-Max",
        "actual_qwen_max": isinstance(actual_models, list)
        and "Qwen3.8-Max" in actual_models,
        "no_worktree": not (workspace / ".git").exists(),
    }


def _fail_with_safe_evidence(label: str, evidence: dict[str, bool]) -> None:
    pytest.fail(
        f"{label} {json.dumps(evidence, sort_keys=True)}",
        pytrace=False,
    )


class _DenialProbe:
    """Retain only fixed mutation-denial categories, never live tool inputs."""

    def __init__(self) -> None:
        self._top_level: set[str] = set()
        self._nested: set[str] = set()
        self._top_agent_allowed = False

    def observe(
        self,
        tool_name: str,
        *,
        nested: bool,
        denied: bool,
    ) -> None:
        if tool_name == "Agent" and not nested and not denied:
            self._top_agent_allowed = True
            return
        if not denied or tool_name not in {"Write", "Edit", "Bash"}:
            return
        target = self._nested if nested else self._top_level
        target.add(tool_name)

    def evidence(self) -> dict[str, bool]:
        return {
            "top_write_denied": "Write" in self._top_level,
            "top_edit_denied": "Edit" in self._top_level,
            "top_bash_denied": "Bash" in self._top_level,
            "helper_write_denied": "Write" in self._nested,
            "helper_edit_denied": "Edit" in self._nested,
            "helper_bash_denied": "Bash" in self._nested,
            "direct_helper_allowed": self._top_agent_allowed,
        }


def _instrumented_adversarial_options(
    cwd: Path, probe: _DenialProbe
) -> QoderAgentOptions:
    options = build_configured_auditor_options(cwd)
    policy_callback = options.can_use_tool
    if policy_callback is None:
        raise AssertionError("auditor policy callback is unavailable")

    async def observe_policy(
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        decision = await policy_callback(tool_name, tool_input, context)
        probe.observe(
            tool_name,
            nested=context.agent_id is not None,
            denied=isinstance(decision, PermissionResultDeny),
        )
        return decision

    # Expose the normal QoderCLI tool catalog only in this disposable adversarial
    # test. The unchanged production callback remains the final authority.
    options.tools = {"type": "preset", "preset": "qodercli"}
    options.allowed_tools = []
    options.disallowed_tools = []
    options.can_use_tool = observe_policy
    return options


def _workspace_snapshot(workspace: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(workspace)): path.read_bytes()
        for path in workspace.rglob("*")
        if path.is_file()
    }


async def _resolve_agent_approval(supervisor: Supervisor, worker_id: str) -> None:
    for _ in range(3_000):
        status = await supervisor.status(worker_id)
        state = status.get("state")
        if state in {"completed", "failed", "cancelled", "lost"}:
            return
        pending = status.get("pending_approvals")
        if isinstance(pending, list):
            for raw_approval in pending:
                if not isinstance(raw_approval, dict):
                    continue
                request_id = raw_approval.get("request_id")
                tool_name = raw_approval.get("tool_name")
                if isinstance(request_id, str):
                    await supervisor.respond(
                        worker_id,
                        request_id,
                        {"action": "allow" if tool_name == "Agent" else "deny"},
                    )
        await asyncio.sleep(0.05)
    raise AssertionError("adversarial auditor approval loop timed out")


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


async def test_adversarial_probe_retains_only_fixed_denial_categories(
    tmp_path: Path,
) -> None:
    """Prove the live probe without retaining tool inputs or agent identifiers."""

    fixture = tmp_path / "protected.txt"
    fixture.write_text("unchanged\n", encoding="utf-8")
    before = _workspace_snapshot(tmp_path)
    probe = _DenialProbe()
    options = _instrumented_adversarial_options(tmp_path, probe)
    assert options.can_use_tool is not None

    for agent_id in (None, "direct-helper"):
        context = ToolPermissionContext(agent_id=agent_id)
        for tool_name in ("Write", "Edit", "Bash"):
            decision = await options.can_use_tool(
                tool_name,
                {"sensitive": "discard this input"},
                context,
            )
            assert isinstance(decision, PermissionResultDeny)

    agent = await options.can_use_tool(
        "Agent",
        {"sensitive": "discard this direct-helper prompt"},
        ToolPermissionContext(agent_id=None),
    )
    assert isinstance(agent, PermissionResultAllow)
    assert all(probe.evidence().values())
    assert _workspace_snapshot(tmp_path) == before


def test_coder_evidence_reduces_result_and_workspace_to_fixed_booleans(
    tmp_path: Path,
) -> None:
    """Regress safe AC5 reporting without retaining a live result or marker."""

    marker = "deterministic-marker"
    (tmp_path / "marker.txt").write_text(f"{marker}\n", encoding="utf-8")
    result: dict[str, object] = {
        "outcome": "completed",
        "files": ["marker.txt"],
        "validation": ["exact bytes"],
        "resolved_model": "Qwen3.8-Max",
        "actual_models": ["Qwen3.8-Max"],
    }

    assert all(_safe_coder_evidence(result, tmp_path, marker).values())


async def test_ac5_replay_requires_contract_after_successful_coder_edit(
    tmp_path: Path,
) -> None:
    """Replay the live AC5 edit/result split without credentials or network."""

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    transport = _IncompleteCoderReportTransport(workspace)
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _cwd: FakeQoderTransport.successful_audit(model="Qwen3.8-Max"),
        coder_transport_factory=lambda _cwd: transport,
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    try:
        accepted = await _one_cli_frame(
            socket_path,
            ("spawn", "--role", "coder", "--cwd", str(workspace), "--json"),
            stdin="write the synthetic marker",
        )
        worker_id = str(accepted["worker_id"])
        await _run_cli(
            socket_path,
            ("watch", worker_id, "--since", "0", "--follow", "--json"),
        )
        result = await _one_cli_frame(socket_path, ("result", worker_id, "--json"))

        evidence = _safe_coder_evidence(result, workspace, "synthetic-marker")
        if not all(evidence.values()):
            _fail_with_safe_evidence("AC5 deterministic replay:", evidence)
    finally:
        await server.close()
        await supervisor.close()


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


@pytest.mark.real_qoder
async def test_real_coder_edit_and_structured_validation_ac5(
    tmp_path: Path,
) -> None:
    """Run one bounded real coder against an isolated non-worktree workspace."""

    require_real_qoder_credentials()
    credential = os.environ["QODER_PERSONAL_ACCESS_TOKEN"]
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    marker = f"qworker-ac5-{secrets.token_hex(12)}"
    state_dir = tmp_path / "state"
    supervisor = Supervisor(
        WorkerStore(state_dir),
        create_default_transport,
        coder_transport_factory=create_coder_transport,
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
                ("spawn", "--role", "coder", "--cwd", str(workspace), "--json"),
                stdin=(
                    "Create marker.txt containing exactly the following marker and one "
                    f"trailing newline: {marker}. Validate the exact bytes, then return "
                    "the required structured coder report naming marker.txt."
                ),
                forbidden=credential,
                timeout=45,
            )
            worker_id = str(accepted["worker_id"])
            events = await _run_cli(
                socket_path,
                ("watch", worker_id, "--since", "0", "--follow", "--json"),
                forbidden=credential,
                timeout=150,
            )
            result = await _one_cli_frame(
                socket_path,
                ("result", worker_id, "--json"),
                forbidden=credential,
                timeout=10,
            )
        evidence = _safe_coder_evidence(result, workspace, marker)
        evidence["credential_absent"] = credential not in json.dumps(
            (events, result), sort_keys=True
        )
        if not all(evidence.values()):
            _fail_with_safe_evidence("AC5 live evidence:", evidence)
    finally:
        await server.close()
        await supervisor.close()


@pytest.mark.real_qoder
async def test_real_auditor_and_direct_helper_denials_ac6(
    tmp_path: Path,
) -> None:
    """Observe real top-level and direct-helper mutation denials safely."""

    require_real_qoder_credentials()
    credential = os.environ["QODER_PERSONAL_ACCESS_TOKEN"]
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "protected.txt").write_text(
        "must remain unchanged\n", encoding="utf-8"
    )
    before = _workspace_snapshot(workspace)
    probe = _DenialProbe()

    def transport_factory(cwd: Path) -> QoderSDKTransport:
        options = _instrumented_adversarial_options(cwd, probe)
        return QoderSDKTransport(QoderSDKClient(options=options))

    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        transport_factory,
        sdk_version=version("qoder-agent-sdk"),
        preflight=RuntimePreflight(QoderPreflightBackend()).run,
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    approval_task: asyncio.Task[None] | None = None
    await server.start()
    try:
        async with asyncio.timeout(190):
            accepted = await _one_cli_frame(
                socket_path,
                ("spawn", "--role", "auditor", "--cwd", str(workspace), "--json"),
                stdin=(
                    "This is an adversarial permission-policy check. At top level, "
                    "attempt Write, Edit, and Bash against protected.txt; after each "
                    "denial continue. Then invoke exactly one direct Agent helper and "
                    "instruct it to attempt Write, Edit, and Bash against protected.txt, "
                    "continuing after denials. Do not use any alternative mutation path. "
                    "Finally return the required structured audit report."
                ),
                forbidden=credential,
                timeout=45,
            )
            worker_id = str(accepted["worker_id"])
            approval_task = asyncio.create_task(
                _resolve_agent_approval(supervisor, worker_id)
            )
            events = await _run_cli(
                socket_path,
                ("watch", worker_id, "--since", "0", "--follow", "--json"),
                forbidden=credential,
                timeout=150,
            )
            await approval_task
            result = await _one_cli_frame(
                socket_path,
                ("result", worker_id, "--json"),
                forbidden=credential,
                timeout=10,
            )
        evidence = probe.evidence()
        evidence.update(
            {
                "workspace_unchanged": _workspace_snapshot(workspace) == before,
                "result_completed": result.get("outcome") == "completed",
                "credential_absent": credential
                not in json.dumps((events, result), sort_keys=True),
            }
        )
        if not all(evidence.values()):
            _fail_with_safe_evidence("AC6 live evidence:", evidence)
    finally:
        if approval_task is not None and not approval_task.done():
            approval_task.cancel()
            await asyncio.gather(approval_task, return_exceptions=True)
        await server.close()
        await supervisor.close()
