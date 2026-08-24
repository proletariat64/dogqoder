import asyncio
import json
from io import StringIO
from pathlib import Path

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


async def test_json_spawn_exits_zero_on_acceptance_without_claiming_completion(
    tmp_path: Path,
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
    spec_file.write_text("audit task six CLI semantics", encoding="utf-8")
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
            "--spec-file",
            str(spec_file),
            "--json",
        ],
        stdin=StringIO(),
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
