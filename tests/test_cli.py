import asyncio
import json
from io import StringIO
from pathlib import Path

import pytest

from qworker.cli import run
from qworker.rpc import RPCServer
from qworker.store import WorkerStore
from qworker.supervisor import Supervisor
from tests.test_supervisor_rpc import GatedFakeQoderTransport


async def test_json_cli_reports_validation_error_on_stdout() -> None:
    stdout = StringIO()

    exit_code = await run(["status", "--json"], stdin=StringIO(), stdout=stdout)

    assert exit_code == 2
    assert json.loads(stdout.getvalue()) == {
        "ok": False,
        "error": {
            "code": "invalid_request",
            "message": "the following arguments are required: worker_id",
        },
    }


async def test_json_cli_reports_supervisor_connection_failure(
    tmp_path: Path,
) -> None:
    stdout = StringIO()

    exit_code = await run(
        [
            "--socket",
            str(tmp_path / "missing.sock"),
            "status",
            "worker-1",
            "--json",
        ],
        stdin=StringIO(),
        stdout=stdout,
    )

    assert exit_code == 3
    assert json.loads(stdout.getvalue()) == {
        "ok": False,
        "error": {
            "code": "supervisor_unavailable",
            "message": "qworker supervisor is unavailable.",
        },
    }


async def test_json_spawn_can_disable_supervisor_auto_start(tmp_path: Path) -> None:
    stdout = StringIO()

    exit_code = await run(
        [
            "--socket",
            str(tmp_path / "missing.sock"),
            "spawn",
            "--role",
            "auditor",
            "--cwd",
            str(tmp_path),
            "--no-start-supervisor",
            "--json",
        ],
        stdin=StringIO("audit without daemon launch"),
        stdout=stdout,
    )

    assert exit_code == 3
    assert json.loads(stdout.getvalue())["error"]["code"] == (
        "supervisor_unavailable"
    )


@pytest.mark.parametrize("input_mode", ("spec_file", "stdin"))
async def test_json_spawn_exits_zero_on_acceptance_without_claiming_completion(
    tmp_path: Path,
    input_mode: str,
) -> None:
    gate = asyncio.Event()
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: GatedFakeQoderTransport(gate, session_id="session-cli"),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()
    spec_file = tmp_path / "audit-spec.txt"
    spec_file.write_text("x" * 70_000, encoding="utf-8")
    spec_arguments = (
        ["--spec-file", str(spec_file)] if input_mode == "spec_file" else []
    )
    stdin = StringIO() if input_mode == "spec_file" else StringIO("x" * 70_000)
    stdout = StringIO()

    exit_code = await run(
        [
            "--socket",
            str(socket_path),
            "spawn",
            "--role",
            "auditor",
            "--cwd",
            str(tmp_path),
            *spec_arguments,
            "--json",
        ],
        stdin=stdin,
        stdout=stdout,
    )

    output = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert output["state"] == "starting"
    assert output["role"] == "auditor"
    assert output["cwd"] == str(tmp_path)
    assert output["event_cursor"] == 1
    assert "outcome" not in output
    assert gate.is_set() is False

    await server.close()
    await supervisor.close()


async def test_json_coder_spawn_uses_role_default_and_reports_overlap(
    tmp_path: Path,
) -> None:
    gates = (asyncio.Event(), asyncio.Event())
    transports = iter(
        (
            GatedFakeQoderTransport(gates[0], session_id="coder-cli-1"),
            GatedFakeQoderTransport(gates[1], session_id="coder-cli-2"),
        )
    )
    supervisor = Supervisor(
        WorkerStore(tmp_path / "state"),
        lambda _: GatedFakeQoderTransport(
            asyncio.Event(), session_id="unexpected-auditor"
        ),
        coder_transport_factory=lambda _: next(transports),
        sdk_version="1.0.13",
    )
    socket_path = tmp_path / "runtime" / "qworker.sock"
    server = RPCServer(supervisor, socket_path)
    await server.start()

    async def spawn(objective: str) -> dict[str, object]:
        stdout = StringIO()
        exit_code = await run(
            [
                "--socket",
                str(socket_path),
                "spawn",
                "--role",
                "coder",
                "--cwd",
                str(tmp_path),
                "--json",
            ],
            stdin=StringIO(objective),
            stdout=stdout,
        )
        assert exit_code == 0
        result = json.loads(stdout.getvalue())
        assert isinstance(result, dict)
        return result

    first = await spawn("first coder")
    second = await spawn("second coder")

    assert first["role"] == "coder"
    assert second["role"] == "coder"
    assert second["warnings"] == [
        {
            "code": "shared_workspace_overlap",
            "worker_id": first["worker_id"],
            "cwd": str(tmp_path),
            "relation": "same",
        }
    ]
    status = await supervisor.status(str(first["worker_id"]))
    assert status["requested_model"] == "qwen-coder"

    await server.close()
    await supervisor.close()


async def test_qworker_console_entrypoint_emits_json_connection_error(
    tmp_path: Path,
) -> None:
    process = await asyncio.create_subprocess_exec(
        "uv",
        "run",
        "qworker",
        "--socket",
        str(tmp_path / "missing.sock"),
        "status",
        "worker-1",
        "--json",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _stderr = await process.communicate()

    assert process.returncode == 3
    assert json.loads(stdout) == {
        "ok": False,
        "error": {
            "code": "supervisor_unavailable",
            "message": "qworker supervisor is unavailable.",
        },
    }


@pytest.mark.parametrize("reset", (False, True), ids=("eof", "reset"))
async def test_follow_watch_disconnect_after_ack_is_structured_failure(
    tmp_path: Path,
    reset: bool,
) -> None:
    socket_path = tmp_path / "dropped-watch.sock"

    async def acknowledge_then_drop(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        request = json.loads(await reader.readline())
        writer.write(
            json.dumps(
                {
                    "request_id": request["request_id"],
                    "ok": True,
                    "result": {
                        "worker_id": "worker-1",
                        "since": 0,
                        "follow": True,
                    },
                }
            ).encode()
            + b"\n"
        )
        await writer.drain()
        if reset:
            writer.transport.abort()
        else:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(
        acknowledge_then_drop, path=socket_path
    )
    stdout = StringIO()

    exit_code = await run(
        [
            "--socket",
            str(socket_path),
            "watch",
            "worker-1",
            "--follow",
            "--json",
        ],
        stdin=StringIO(),
        stdout=stdout,
    )

    assert exit_code == 3
    assert json.loads(stdout.getvalue()) == {
        "ok": False,
        "error": {
            "code": "supervisor_unavailable",
            "message": "qworker watch connection ended before a terminal frame.",
        },
    }
    server.close()
    await server.wait_closed()
