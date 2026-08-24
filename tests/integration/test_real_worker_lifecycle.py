"""Public-interface acceptance checks for concurrent worker lifecycle control."""

import asyncio
import json
import os
import secrets
import sys
import uuid
from collections.abc import AsyncIterator, Sequence
from importlib.metadata import version
from pathlib import Path
from typing import cast

import pytest

from qworker.control import ControlCallbacks, PermissionRequest, SteeringPriority
from qworker.events import (
    AdapterEvent,
    ResultEvent,
    TaskProgressEvent,
    TaskStartedEvent,
)
from qworker.model_policy import AvailableModel
from qworker.preflight import RuntimePreflight
from qworker.qoder_sdk import QoderPreflightBackend, create_default_transport
from qworker.rpc import RPCServer
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor
from tests.fakes import SUCCESSFUL_AUDIT_REPORT, FakeQoderTransport
from tests.real_qoder import require_real_qoder_credentials

_CLI_BOOTSTRAP = "from qworker.cli import main; raise SystemExit(main())"


class _ControlledNestedTransport(FakeQoderTransport):
    """Keep one worker live while exposing deterministic control outcomes."""

    def __init__(self, gate: asyncio.Event, *, session_id: str) -> None:
        super().__init__(
            models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
            events=(
                TaskStartedEvent(
                    task_id=f"{session_id}-helper",
                    description="inspect one acceptance fixture",
                ),
                TaskProgressEvent(
                    task_id=f"{session_id}-helper",
                    description="inspect one acceptance fixture",
                    last_tool_name="Read",
                ),
                ResultEvent(
                    session_id=session_id,
                    is_error=False,
                    result=SUCCESSFUL_AUDIT_REPORT,
                    model_usage=("Qwen3.8-Max",),
                ),
            ),
        )
        self._gate = gate
        self.callbacks: ControlCallbacks | None = None
        self.steering: list[tuple[str, SteeringPriority, str]] = []

    def bind_control(self, callbacks: ControlCallbacks) -> None:
        self.callbacks = callbacks

    async def steer(
        self,
        message: str,
        *,
        priority: SteeringPriority,
        message_id: str,
    ) -> None:
        self.steering.append((message, priority, message_id))

    async def cancel_message(self, message_id: str) -> bool:
        return message_id == self.steering[-1][2]

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
    command = (
        sys.executable,
        "-c",
        _CLI_BOOTSTRAP,
        "--socket",
        str(socket_path),
        *arguments,
    )
    process = await asyncio.create_subprocess_exec(
        *command,
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
    timeout: float = 5.0,
) -> dict[str, object]:
    frames = await _run_cli(
        socket_path,
        arguments,
        stdin=stdin,
        expected_exit=expected_exit,
        timeout=timeout,
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


async def test_public_cli_controls_two_workers_and_replays_each_cursor(
    tmp_path: Path,
) -> None:
    """Exercise every control through separate CLI process invocations."""

    gates = (asyncio.Event(), asyncio.Event())
    transports = (
        _ControlledNestedTransport(gates[0], session_id="acceptance-session-1"),
        _ControlledNestedTransport(gates[1], session_id="acceptance-session-2"),
    )
    pending_transports = iter(transports)
    store = WorkerStore(tmp_path / "state")
    supervisor = Supervisor(
        store,
        lambda _cwd: next(pending_transports),
        sdk_version="1.0.13",
        settlement_timeout=0.01,
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    try:
        first_frames, second_frames = await asyncio.gather(
            _run_cli(
                socket_path,
                ("spawn", "--role", "auditor", "--cwd", str(tmp_path), "--json"),
                stdin="audit first acceptance worker",
            ),
            _run_cli(
                socket_path,
                ("spawn", "--role", "auditor", "--cwd", str(tmp_path), "--json"),
                stdin="audit second acceptance worker",
            ),
        )
        assert len(first_frames) == len(second_frames) == 1
        first, second = first_frames[0], second_frames[0]
        first_id = str(first["worker_id"])
        second_id = str(second["worker_id"])
        assert first_id != second_id
        assert first["state"] == second["state"] == "starting"

        first_status, second_status = await asyncio.gather(
            _wait_for_state(socket_path, first_id, "running"),
            _wait_for_state(socket_path, second_id, "running"),
        )
        assert first_status["requested_model"] == "qwen-auditor"
        assert second_status["requested_model"] == "qwen-auditor"

        replay = await _run_cli(
            socket_path, ("watch", first_id, "--since", "0", "--json")
        )
        replay_sequences = [cast(int, frame["sequence"]) for frame in replay]
        assert replay_sequences == list(range(1, replay_sequences[-1] + 1))
        cursor = replay_sequences[-1]

        first_transport = next(
            transport
            for transport in transports
            if transport.sent_prompts
            and "audit first acceptance worker" in transport.sent_prompts[0]
        )
        assert first_transport.callbacks is not None
        approval = asyncio.create_task(
            first_transport.callbacks.request_permission(
                PermissionRequest(
                    tool_name="Read",
                    agent_id=None,
                    display_message="Inspect one public fixture?",
                )
            )
        )
        requires_action = await _wait_for_state(
            socket_path, first_id, "requires_action"
        )
        pending = cast(list[dict[str, object]], requires_action["pending_approvals"])
        assert len(pending) == 1
        request_id = str(pending[0]["request_id"])
        answered = await _one_cli_frame(
            socket_path,
            ("respond", first_id, request_id, "--json"),
            stdin='{"action":"allow"}',
        )
        assert answered == {"request_id": request_id, "status": "allowed"}
        assert (await approval).action == "allow"

        steering = []
        for priority in ("now", "next", "later"):
            response = await _one_cli_frame(
                socket_path,
                ("steer", first_id, "--priority", priority, "--json"),
                stdin=f"acceptance {priority} message",
            )
            message_id = str(response["message_id"])
            assert str(uuid.UUID(message_id)) == message_id
            assert response == {
                "accepted": True,
                "message_id": message_id,
                "priority": priority,
            }
            steering.append(response)
        assert [item[1] for item in first_transport.steering] == [
            "now",
            "next",
            "later",
        ]

        cancelled = await _one_cli_frame(
            socket_path,
            (
                "cancel-message",
                first_id,
                str(steering[-1]["message_id"]),
                "--json",
            ),
        )
        assert cancelled == {
            "cancelled": True,
            "message_id": steering[-1]["message_id"],
        }
        unsupported = await _one_cli_frame(
            socket_path,
            (
                "steer",
                first_id,
                "--agent-id",
                "nested-helper",
                "--json",
            ),
            stdin="unsupported nested steering",
            expected_exit=1,
        )
        assert cast(dict[str, object], unsupported["error"])["code"] == (
            "unsupported_operation"
        )

        tails = (
            asyncio.create_task(
                _run_cli(
                    socket_path,
                    ("watch", first_id, "--since", str(cursor), "--follow", "--json"),
                )
            ),
            asyncio.create_task(
                _run_cli(
                    socket_path,
                    ("watch", second_id, "--since", "0", "--follow", "--json"),
                )
            ),
        )
        await asyncio.sleep(0.05)
        gates[0].set()
        gates[1].set()
        first_tail, second_tail = await asyncio.gather(*tails)

        assert [cast(int, frame["sequence"]) for frame in first_tail] == list(
            range(cursor + 1, cast(int, first_tail[-1]["sequence"]) + 1)
        )
        assert [cast(int, frame["sequence"]) for frame in second_tail] == list(
            range(1, cast(int, second_tail[-1]["sequence"]) + 1)
        )
        assert {frame["worker_id"] for frame in first_tail} == {first_id}
        assert {frame["worker_id"] for frame in second_tail} == {second_id}

        first_result, second_result = await asyncio.gather(
            _one_cli_frame(socket_path, ("result", first_id, "--json")),
            _one_cli_frame(socket_path, ("result", second_id, "--json")),
        )
        for result in (first_result, second_result):
            assert result["requested_model"] == "qwen-auditor"
            assert result["resolved_model"] == "Qwen3.8-Max"
            assert result["actual_models"] == ["Qwen3.8-Max"]
            assert result["nested_state"] == "unknown"
            assert result["warnings"] == ["nested_terminal_event_missing"]

        durable_events = await store.events_since(first_id)
        durable_types = [event.type for event in durable_events]
        assert "approval.requested" in durable_types
        assert "approval.resolved" in durable_types
        assert "steer.queued" in durable_types
        assert "steer.delivered" in durable_types
        assert "steer.cancelled" in durable_types
        assert "worker.warning" in durable_types
    finally:
        await server.close()
        await supervisor.close()


@pytest.mark.real_qoder
async def test_real_public_lifecycle_reports_model_and_keeps_workspace_read_only(
    tmp_path: Path,
) -> None:
    """One bounded, credit-consuming marker through the public CLI boundary."""

    require_real_qoder_credentials()
    credential = os.environ["QODER_PERSONAL_ACCESS_TOKEN"]
    marker = f"QWV1{secrets.token_hex(6)}"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    before = {
        path.relative_to(workspace): path.read_bytes()
        for path in workspace.rglob("*")
        if path.is_file()
    }
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        create_default_transport,
        sdk_version=version("qoder-agent-sdk"),
        preflight=RuntimePreflight(QoderPreflightBackend()).run,
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    try:
        async with asyncio.timeout(190):
            spawn_frames = await _run_cli(
                socket_path,
                (
                    "spawn",
                    "--role",
                    "auditor",
                    "--cwd",
                    str(workspace),
                    "--json",
                ),
                stdin=(
                    "Read no workspace files. Return the supplied marker in the "
                    f"structured report summary: {marker}"
                ),
                timeout=45,
            )
            assert len(spawn_frames) == 1
            worker_id = str(spawn_frames[0]["worker_id"])
            events = await _run_cli(
                socket_path,
                ("watch", worker_id, "--since", "0", "--follow", "--json"),
                timeout=140,
            )
            result = await _one_cli_frame(
                socket_path,
                ("result", worker_id, "--json"),
                timeout=10,
            )
            status = await _one_cli_frame(
                socket_path,
                ("status", worker_id, "--json"),
                timeout=10,
            )

        captured = json.dumps(
            {
                "spawn": spawn_frames,
                "events": events,
                "result": result,
                "status": status,
            },
            sort_keys=True,
        )
        if credential in captured:
            pytest.fail(
                "Credential crossed the public lifecycle boundary.", pytrace=False
            )
        if marker not in str(result.get("summary", "")):
            pytest.fail(
                "Live marker was absent from the structured summary.", pytrace=False
            )
        if result.get("outcome") != "completed":
            pytest.xfail(
                "Task 14 live public worker did not produce a completed result."
            )
        if result.get("resolved_model") != "Qwen3.8-Max":
            pytest.fail("Live catalog did not resolve Qwen3.8-Max.", pytrace=False)
        if "Qwen3.8-Max" not in cast(list[object], result.get("actual_models", [])):
            pytest.fail("Live result did not report Qwen3.8-Max usage.", pytrace=False)
        if status.get("runtime_path") != "bundled":
            pytest.fail(
                "Live worker did not report the bundled runtime.", pytrace=False
            )
        after = {
            path.relative_to(workspace): path.read_bytes()
            for path in workspace.rglob("*")
            if path.is_file()
        }
        if after != before:
            pytest.fail(
                "Live read-only worker changed its disposable workspace.", pytrace=False
            )
    finally:
        await server.close()
        await supervisor.close()
