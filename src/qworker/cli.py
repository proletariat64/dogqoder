"""Thin JSON CLI client for the qworker supervisor."""

import argparse
import asyncio
import json
import os
import subprocess
import sys
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import NoReturn, TextIO, cast

from qworker.preflight import (
    DoctorResult,
    PreflightRunner,
    PreflightWarningCode,
    RuntimePreflight,
)
from qworker.rpc import RPCClientError, call, default_state_dir, watch
from qworker.store import JsonValue

SupervisorLauncher = Callable[[Path, Path], Awaitable[None]]


class _ValidationError(Exception):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ValidationError(message)


async def run(
    argv: Sequence[str],
    *,
    stdin: TextIO,
    stdout: TextIO,
    doctor_runner: PreflightRunner | None = None,
    supervisor_launcher: SupervisorLauncher | None = None,
) -> int:
    """Run one CLI invocation and return its process exit code."""

    try:
        arguments = _parser().parse_args(argv)
        socket_path = cast(Path, arguments.socket)
        if arguments.command == "doctor":
            doctor_result = await (
                _run_default_doctor(Path.cwd())
                if doctor_runner is None
                else doctor_runner(Path.cwd())
            )
            _write_json(stdout, doctor_result.to_json())
            return 0 if doctor_result.ok else 1
        state_dir = cast(Path, arguments.state_dir)
        start_supervisor = not cast(bool, arguments.no_start_supervisor)
        launcher = supervisor_launcher or _start_supervisor

        async def request(method: str, params: dict[str, JsonValue]) -> JsonValue:
            return await _call_with_supervisor(
                socket_path,
                state_dir,
                method,
                params,
                start_supervisor=start_supervisor,
                supervisor_launcher=launcher,
            )

        if arguments.command == "spawn":
            objective = _read_objective(arguments.spec_file, stdin)
            role = cast(str, arguments.role)
            model = cast(str | None, arguments.model)
            if model is None:
                model = "qwen-coder" if role == "coder" else "qwen-auditor"
            params: dict[str, JsonValue] = {
                "role": role,
                "cwd": str(cast(Path, arguments.cwd)),
                "model": model,
                "objective": objective,
            }
            result = await request("spawn", params)
            _write_json(stdout, result)
            return 0
        if arguments.command == "list":
            result = await request("list", {})
            _write_json(stdout, result)
            return 0
        if arguments.command == "status":
            result = await request(
                "status",
                {"worker_id": cast(str, arguments.worker_id)},
            )
            _write_json(stdout, result)
            return 0
        if arguments.command == "result":
            result = await request(
                "result",
                {"worker_id": cast(str, arguments.worker_id)},
            )
            _write_json(stdout, result)
            return 0
        if arguments.command == "resume":
            result = await request(
                "resume",
                {"worker_id": cast(str, arguments.worker_id)},
            )
            _write_json(stdout, result)
            return 0
        if arguments.command == "watch":
            watch_params: dict[str, JsonValue] = {
                "worker_id": cast(str, arguments.worker_id),
                "since": cast(int, arguments.since),
                "follow": cast(bool, arguments.follow),
            }
            await request("list", {})
            async for event in watch(socket_path, watch_params):
                _write_json(stdout, event)
            return 0
        if arguments.command == "steer":
            message = _read_required_text(
                cast(Path | None, arguments.message_file),
                stdin,
                label="steering message",
            )
            steer_params: dict[str, JsonValue] = {
                "worker_id": cast(str, arguments.worker_id),
                "message": message,
                "priority": cast(str, arguments.priority),
            }
            agent_id = cast(str | None, arguments.agent_id)
            if agent_id is not None:
                steer_params["agent_id"] = agent_id
            result = await request("steer", steer_params)
            _write_json(stdout, result)
            return 0
        if arguments.command == "cancel-message":
            result = await request(
                "cancel_message",
                {
                    "worker_id": cast(str, arguments.worker_id),
                    "message_id": cast(str, arguments.message_id),
                },
            )
            _write_json(stdout, result)
            return 0
        if arguments.command == "stop":
            stop_params: dict[str, JsonValue] = {
                "worker_id": cast(str, arguments.worker_id),
                "force": cast(bool, arguments.force),
            }
            agent_id = cast(str | None, arguments.agent_id)
            if agent_id is not None:
                stop_params["agent_id"] = agent_id
            result = await request("stop", stop_params)
            _write_json(stdout, result)
            return 0
        if arguments.command == "respond":
            raw_response = _read_required_text(
                cast(Path | None, arguments.response_file),
                stdin,
                label="approval response",
            )
            try:
                decoded: object = json.loads(raw_response)
            except json.JSONDecodeError:
                raise _ValidationError(
                    "approval response must be one JSON object"
                ) from None
            if not isinstance(decoded, dict) or not all(
                isinstance(key, str) for key in decoded
            ):
                raise _ValidationError("approval response must be one JSON object")
            result = await request(
                "respond",
                {
                    "worker_id": cast(str, arguments.worker_id),
                    "request_id": cast(str, arguments.request_id),
                    "response": cast(dict[str, JsonValue], decoded),
                },
            )
            _write_json(stdout, result)
            return 0
        raise _ValidationError("a command is required")
    except _ValidationError as error:
        _write_error(stdout, "invalid_request", str(error))
        return 2
    except RPCClientError as error:
        _write_error(
            stdout,
            error.code,
            error.message,
            warnings=error.warnings,
        )
        if error.code == "invalid_request":
            return 2
        if error.code == "supervisor_unavailable":
            return 3
        return 1


def main(argv: Sequence[str] | None = None) -> int:
    """Console-script entry point."""

    return asyncio.run(
        run(
            tuple(sys.argv[1:] if argv is None else argv),
            stdin=sys.stdin,
            stdout=sys.stdout,
        )
    )


def default_socket_path() -> Path:
    """Return the documented per-user local supervisor socket location."""

    runtime_directory = os.environ.get("XDG_RUNTIME_DIR")
    if runtime_directory:
        return Path(runtime_directory) / "qworker" / "qworker.sock"
    return Path.home() / ".local" / "state" / "qworker" / "qworker.sock"


def _parser() -> _ArgumentParser:
    parser = _ArgumentParser(prog="qworker")
    parser.add_argument("--socket", type=Path, default=default_socket_path())
    parser.add_argument("--state-dir", type=Path, default=default_state_dir())
    commands = parser.add_subparsers(dest="command", required=True)

    spawn = commands.add_parser("spawn")
    spawn.add_argument("--role", required=True, choices=("auditor", "coder"))
    spawn.add_argument("--cwd", required=True, type=Path)
    spawn.add_argument("--model")
    spawn.add_argument("--spec-file", type=Path)
    spawn.add_argument("--no-start-supervisor", action="store_true")
    spawn.add_argument("--json", action="store_true")

    list_parser = commands.add_parser("list")
    list_parser.add_argument("--no-start-supervisor", action="store_true")
    list_parser.add_argument("--json", action="store_true")

    status = commands.add_parser("status")
    status.add_argument("worker_id")
    status.add_argument("--no-start-supervisor", action="store_true")
    status.add_argument("--json", action="store_true")

    result = commands.add_parser("result")
    result.add_argument("worker_id")
    result.add_argument("--no-start-supervisor", action="store_true")
    result.add_argument("--json", action="store_true")

    resume = commands.add_parser("resume")
    resume.add_argument("worker_id")
    resume.add_argument("--no-start-supervisor", action="store_true")
    resume.add_argument("--json", action="store_true")

    watch_parser = commands.add_parser("watch")
    watch_parser.add_argument("worker_id")
    watch_parser.add_argument("--since", type=int, default=0)
    watch_parser.add_argument("--follow", action="store_true")
    watch_parser.add_argument("--no-start-supervisor", action="store_true")
    watch_parser.add_argument("--json", action="store_true")

    steer = commands.add_parser("steer")
    steer.add_argument("worker_id")
    steer.add_argument("--priority", choices=("now", "next", "later"), default="next")
    steer.add_argument("--message-file", type=Path)
    steer.add_argument("--agent-id")
    steer.add_argument("--no-start-supervisor", action="store_true")
    steer.add_argument("--json", action="store_true")

    cancel_message = commands.add_parser("cancel-message")
    cancel_message.add_argument("worker_id")
    cancel_message.add_argument("message_id")
    cancel_message.add_argument("--no-start-supervisor", action="store_true")
    cancel_message.add_argument("--json", action="store_true")

    stop = commands.add_parser("stop")
    stop.add_argument("worker_id")
    stop.add_argument("--force", action="store_true")
    stop.add_argument("--agent-id")
    stop.add_argument("--no-start-supervisor", action="store_true")
    stop.add_argument("--json", action="store_true")

    respond = commands.add_parser("respond")
    respond.add_argument("worker_id")
    respond.add_argument("request_id")
    respond.add_argument("--response-file", type=Path)
    respond.add_argument("--no-start-supervisor", action="store_true")
    respond.add_argument("--json", action="store_true")

    doctor = commands.add_parser("doctor")
    doctor.add_argument("--json", action="store_true")
    return parser


async def _run_default_doctor(cwd: Path) -> DoctorResult:
    from qworker.qoder_sdk import QoderPreflightBackend

    return await RuntimePreflight(QoderPreflightBackend()).run(cwd)


def _read_objective(spec_file: Path | None, stdin: TextIO) -> str:
    return _read_required_text(spec_file, stdin, label="spawn specification")


def _read_required_text(
    input_file: Path | None,
    stdin: TextIO,
    *,
    label: str,
) -> str:
    try:
        value = (
            input_file.read_text(encoding="utf-8")
            if input_file is not None
            else stdin.read()
        )
    except OSError:
        raise _ValidationError(f"unable to read {label}") from None
    if not value.strip():
        raise _ValidationError(f"{label} must not be empty")
    return value


async def _call_with_supervisor(
    socket_path: Path,
    state_dir: Path,
    method: str,
    params: dict[str, JsonValue],
    *,
    start_supervisor: bool,
    supervisor_launcher: SupervisorLauncher,
) -> JsonValue:
    try:
        return await call(socket_path, method, params)
    except RPCClientError as error:
        if error.code != "supervisor_unavailable" or not start_supervisor:
            raise

    try:
        await supervisor_launcher(socket_path, state_dir)
    except OSError:
        raise RPCClientError(
            "supervisor_unavailable", "qworker supervisor is unavailable."
        ) from None
    for _ in range(40):
        try:
            return await call(socket_path, method, params)
        except RPCClientError as error:
            if error.code != "supervisor_unavailable":
                raise
        await asyncio.sleep(0.05)
    raise RPCClientError("supervisor_unavailable", "qworker supervisor is unavailable.")


async def _start_supervisor(socket_path: Path, state_dir: Path) -> None:
    await asyncio.to_thread(_launch_supervisor, socket_path, state_dir)


def _launch_supervisor(socket_path: Path, state_dir: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        (
            sys.executable,
            "-m",
            "qworker.rpc",
            "--serve",
            "--socket",
            str(socket_path),
            "--state-dir",
            str(state_dir),
        ),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        start_new_session=os.name == "posix",
    )


def _write_json(stdout: TextIO, value: JsonValue) -> None:
    stdout.write(json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n")
    stdout.flush()


def _write_error(
    stdout: TextIO,
    code: str,
    message: str,
    *,
    warnings: tuple[PreflightWarningCode, ...] = (),
) -> None:
    error: dict[str, JsonValue] = {"code": code, "message": message}
    if warnings:
        error["warnings"] = list(warnings)
    _write_json(
        stdout,
        {"ok": False, "error": error},
    )
