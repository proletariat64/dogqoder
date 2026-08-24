import json
import sqlite3
from collections.abc import AsyncIterator, Sequence
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from qoder_agent_sdk import (
    AssistantMessage,
    QoderAgentOptions,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolUseBlock,
)

from qworker.cli import run
from qworker.qoder_sdk import QoderSDKTransport, build_auditor_options
from qworker.rpc import RPCServer, call
from qworker.rpc import watch as rpc_watch
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor


class _AdversarialSDKClient:
    def __init__(
        self,
        options: QoderAgentOptions,
        *,
        raw_event_secret: str,
        tool_input_secret: str,
        exception_secret: str,
    ) -> None:
        self.options = options
        self._raw_event_secret = raw_event_secret
        self._tool_input_secret = tool_input_secret
        self._exception_secret = exception_secret
        self.prompts: list[str] = []

    async def connect(self, _prompt: None) -> None:
        return None

    async def get_available_models(self) -> list[dict[str, Any]]:
        return [{"value": "Qwen3.8-Max", "isEnabled": True}]

    async def set_model(self, _model: str) -> None:
        return None

    async def query(self, prompt: str) -> None:
        self.prompts.append(prompt)

    async def receive_messages(self) -> AsyncIterator[object]:
        report = json.dumps(
            {
                "outcome": "completed",
                "summary": f"audited {self._raw_event_secret}",
                "files": [],
                "validation": [],
                "risks": [self._raw_event_secret],
                "verdict": "approved",
                "confirmed": [self._raw_event_secret],
                "findings": [],
                "required_changes": [],
            }
        )
        yield AssistantMessage(
            content=[
                TextBlock(text=f"observed {self._raw_event_secret}"),
                ToolUseBlock(
                    id="tool-secret-input",
                    name="Read",
                    input={"file_path": self._tool_input_secret},
                ),
            ],
            model="Qwen3.8-Max",
        )
        yield SystemMessage(
            subtype="status",
            data={"diagnostic": self._raw_event_secret},
        )
        yield ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="session-redaction",
            result=report,
            errors=[f"raw diagnostic {self._raw_event_secret}"],
        )

    async def disconnect(self) -> None:
        raise RuntimeError(f"disconnect failed: {self._exception_secret}")


async def _run_cli(
    socket_path: Path,
    arguments: Sequence[str],
    *,
    stdin_text: str = "",
) -> tuple[int, str]:
    stdout = StringIO()
    exit_code = await run(
        ("--socket", str(socket_path), *arguments),
        stdin=StringIO(stdin_text),
        stdout=stdout,
    )
    return exit_code, stdout.getvalue()


async def test_credentials_never_cross_persistence_rpc_cli_or_log_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    safe_root = tmp_path.parent / "safe-boundary"
    safe_root.mkdir()
    environment_secret = "environment-credential-sentinel"
    raw_event_secret = "raw-event-credential-sentinel"
    tool_input_secret = "tool-input-credential-sentinel"
    exception_secret = "exception-credential-sentinel"
    sentinels = (
        environment_secret,
        raw_event_secret,
        tool_input_secret,
        exception_secret,
    )
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", environment_secret)
    monkeypatch.setenv("QODER_SERVICE_ACCOUNT_KEY", raw_event_secret)
    monkeypatch.setenv("QODERCN_PERSONAL_ACCESS_TOKEN", tool_input_secret)
    monkeypatch.setenv("QODERCN_SERVICE_ACCOUNT_KEY", exception_secret)

    options = build_auditor_options(safe_root)
    sdk_client = _AdversarialSDKClient(
        options,
        raw_event_secret=raw_event_secret,
        tool_input_secret=tool_input_secret,
        exception_secret=exception_secret,
    )
    store = WorkerStore(safe_root / "state")
    supervisor = Supervisor(
        store,
        lambda _cwd: QoderSDKTransport(sdk_client),
        sdk_version="1.0.13",
    )
    socket_path = safe_root / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()

    spawn_argv = (
        "spawn",
        "--role",
        "auditor",
        "--cwd",
        str(safe_root),
        "--no-start-supervisor",
        "--json",
    )
    spawn_code, spawn_output = await _run_cli(
        socket_path,
        spawn_argv,
        stdin_text="perform deterministic redaction audit",
    )
    assert spawn_code == 0, spawn_output
    accepted = json.loads(spawn_output)
    worker_id = str(accepted["worker_id"])
    _events = [
        event async for event in supervisor.watch(worker_id, since=0, follow=True)
    ]

    cli_outputs = [spawn_output]
    for arguments in (
        ("status", worker_id, "--json"),
        ("result", worker_id, "--json"),
        ("watch", worker_id, "--since", "0", "--json"),
    ):
        exit_code, output = await _run_cli(socket_path, arguments)
        assert exit_code == 0
        cli_outputs.append(output)

    rpc_status = await call(socket_path, "status", {"worker_id": worker_id})
    rpc_result = await call(socket_path, "result", {"worker_id": worker_id})
    rpc_events = [
        event
        async for event in rpc_watch(
            socket_path,
            {"worker_id": worker_id, "since": 0, "follow": False},
        )
    ]
    rpc_output = json.dumps(
        {"status": rpc_status, "result": rpc_result, "events": rpc_events},
        sort_keys=True,
    )

    with sqlite3.connect(store.database_path) as connection:
        durable_output = "\n".join(connection.iterdump())

    assert all(
        sentinel not in argument for sentinel in sentinels for argument in spawn_argv
    )
    assert sdk_client.prompts
    inspected_outputs = (*cli_outputs, rpc_output, durable_output, caplog.text)
    for sentinel in sentinels:
        assert all(sentinel not in output for output in inspected_outputs)
    assert "[REDACTED]" in "".join(cli_outputs)
    assert isinstance(rpc_status, dict)
    assert rpc_status["state"] == "failed"
    assert isinstance(rpc_result, dict)
    assert rpc_result["warnings"] == ["disconnect_failed"]
    errors = rpc_result["errors"]
    assert isinstance(errors, list)
    assert errors[-1] == "sdk_protocol_error"

    await server.close()
    await supervisor.close()
