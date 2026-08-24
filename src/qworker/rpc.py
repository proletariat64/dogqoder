"""Owner-only local NDJSON RPC for the qworker supervisor."""

import asyncio
import json
import os
import re
import signal
import stat
import sys
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import suppress
from importlib.metadata import version
from pathlib import Path
from typing import cast

from qworker.control import SteeringPriority
from qworker.domain import AuditContract
from qworker.store import JsonValue
from qworker.supervisor import Supervisor, SupervisorError

type JsonObject = dict[str, JsonValue]

_MAX_FRAME_BYTES = 4 * 1024 * 1024
_MAX_REQUEST_ID_CHARS = 128
_MAX_ENCODED_REQUEST_ID_BYTES = _MAX_REQUEST_ID_CHARS * 12 + 2
_TERMINAL_STATES = frozenset(("completed", "failed", "cancelled", "lost"))
_REQUEST_ID_PATTERN = re.compile(
    rb'"request_id"\s*:\s*("(?:\\.|[^"\\])*")'
)


class _FrameTooLargeError(Exception):
    def __init__(self, request_id: JsonValue = None) -> None:
        super().__init__(_frame_limit_message())
        self.request_id = request_id


class RPCServer:
    """Serve correlated supervisor requests over one private Unix socket."""

    def __init__(self, supervisor: Supervisor, socket_path: Path) -> None:
        self._supervisor = supervisor
        self._socket_path = socket_path
        self._server: asyncio.Server | None = None
        self._socket_identity: tuple[int, int] | None = None

    @property
    def socket_path(self) -> Path:
        return self._socket_path

    async def start(self) -> None:
        """Bind the local socket after enforcing owner-only state permissions."""

        if self._server is not None:
            raise RuntimeError("RPC server is already started.")
        self._socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if os.name == "posix":
            self._socket_path.parent.chmod(0o700)
        try:
            existing = self._socket_path.lstat()
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISSOCK(existing.st_mode):
                raise RuntimeError("RPC socket path exists and is not a socket.")
            if await _socket_is_active(self._socket_path):
                raise RuntimeError("A qworker supervisor is already active.")
            self._socket_path.unlink()
        self._server = await asyncio.start_unix_server(
            self._handle_connection,
            path=self._socket_path,
            limit=_MAX_FRAME_BYTES,
        )
        if os.name == "posix":
            self._socket_path.chmod(0o600)
        bound = self._socket_path.stat()
        self._socket_identity = (bound.st_dev, bound.st_ino)

    async def close(self) -> None:
        """Stop accepting connections and remove only the bound socket file."""

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        identity = self._socket_identity
        self._socket_identity = None
        if identity is None:
            return
        try:
            current = self._socket_path.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISSOCK(current.st_mode) and (current.st_dev, current.st_ino) == identity:
            self._socket_path.unlink()

    async def serve_forever(self) -> None:
        """Serve until cancelled after ``start`` has completed."""

        if self._server is None:
            raise RuntimeError("RPC server is not started.")
        await self._server.serve_forever()

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                request_id: JsonValue = None
                try:
                    line = await _read_frame(reader)
                    if line is None:
                        return
                    request = _decode_request(line)
                    request_id = request["request_id"]
                    method = cast(str, request["method"])
                    params = cast(JsonObject, request["params"])
                    if method == "watch":
                        await self._stream_watch(writer, request_id, params)
                        return
                    result = await self._dispatch(method, params)
                    await _write_frame(writer, _success(request_id, result))
                except SupervisorError as error:
                    await _write_frame(
                        writer,
                        _failure(request_id, error.code, error.message),
                    )
                except (TypeError, ValueError) as error:
                    await _write_frame(
                        writer,
                        _failure(request_id, "invalid_request", str(error)),
                    )
                except _FrameTooLargeError as error:
                    await _write_frame(
                        writer,
                        _failure(
                            error.request_id if error.request_id is not None else request_id,
                            "frame_too_large",
                            _frame_limit_message(),
                        ),
                    )
                    return
                except Exception:  # noqa: BLE001 -- isolate one RPC connection
                    await _write_frame(
                        writer,
                        _failure(
                            request_id,
                            "sdk_protocol_error",
                            "Supervisor request failed.",
                        ),
                    )
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            writer.close()
            with suppress(BrokenPipeError, ConnectionResetError):
                await writer.wait_closed()

    async def _dispatch(self, method: str, params: JsonObject) -> JsonValue:
        if method == "spawn":
            _validate_fields(
                params,
                required=("role", "cwd", "objective"),
                optional=(
                    "model",
                    "context",
                    "constraints",
                    "acceptance_criteria",
                ),
            )
            role = _string(params, "role")
            if role != "auditor":
                raise ValueError("Task 6 supports role 'auditor'.")
            contract = AuditContract(
                objective=_string(params, "objective"),
                cwd=Path(_string(params, "cwd")),
                requested_model=_optional_string(
                    params, "model", default="qwen-auditor"
                ),
                context=_string_tuple(params, "context"),
                constraints=_string_tuple(params, "constraints"),
                acceptance_criteria=_string_tuple(params, "acceptance_criteria"),
            )
            return await self._supervisor.spawn(contract)
        if method == "status":
            _validate_fields(params, required=("worker_id",))
            return await self._supervisor.status(_string(params, "worker_id"))
        if method == "result":
            _validate_fields(params, required=("worker_id",))
            return await self._supervisor.result(_string(params, "worker_id"))
        if method == "steer":
            _validate_fields(
                params,
                required=("worker_id", "message"),
                optional=("priority", "agent_id"),
            )
            priority = _optional_string(params, "priority", default="next")
            if priority not in ("now", "next", "later"):
                raise ValueError("priority must be now, next, or later.")
            agent_id = (
                _string(params, "agent_id") if "agent_id" in params else None
            )
            return await self._supervisor.steer(
                _string(params, "worker_id"),
                _string(params, "message"),
                priority=cast(SteeringPriority, priority),
                agent_id=agent_id,
            )
        if method == "cancel_message":
            _validate_fields(params, required=("worker_id", "message_id"))
            return await self._supervisor.cancel_message(
                _string(params, "worker_id"),
                _string(params, "message_id"),
            )
        if method == "stop":
            _validate_fields(
                params,
                required=("worker_id",),
                optional=("force", "agent_id"),
            )
            agent_id = _string(params, "agent_id") if "agent_id" in params else None
            return await self._supervisor.stop(
                _string(params, "worker_id"),
                force=_boolean(params, "force", default=False),
                agent_id=agent_id,
            )
        if method == "respond":
            _validate_fields(
                params,
                required=("worker_id", "request_id", "response"),
            )
            response = params["response"]
            if not isinstance(response, dict):
                raise TypeError("response must be a JSON object.")
            return await self._supervisor.respond(
                _string(params, "worker_id"),
                _string(params, "request_id"),
                response,
            )
        raise ValueError(f"Unknown RPC method: {method}")

    async def _stream_watch(
        self, writer: asyncio.StreamWriter, request_id: JsonValue, params: JsonObject
    ) -> None:
        _validate_fields(
            params,
            required=("worker_id",),
            optional=("since", "follow"),
        )
        worker_id = _string(params, "worker_id")
        since = _non_negative_integer(params, "since", default=0)
        follow = _boolean(params, "follow", default=False)
        await self._supervisor.status(worker_id)
        await _write_frame(
            writer,
            _success(
                request_id,
                {
                    "worker_id": worker_id,
                    "since": since,
                    "follow": follow,
                },
            ),
        )
        async for event in self._supervisor.watch(
            worker_id,
            since=since,
            follow=follow,
        ):
            await _write_frame(writer, _success(request_id, {"event": event}))
        status = await self._supervisor.status(worker_id)
        state = status["state"]
        cursor = status["event_cursor"]
        if not isinstance(state, str) or not isinstance(cursor, int):
            raise SupervisorError(
                "sdk_protocol_error", "Worker status has invalid watch metadata."
            )
        if follow and state not in _TERMINAL_STATES:
            raise SupervisorError(
                "worker_not_live", "Follow watch ended before worker termination."
            )
        reason = "terminal" if follow else "replay_complete"
        await _write_frame(
            writer,
            _success(
                request_id,
                {"end": {"reason": reason, "cursor": cursor, "state": state}},
            ),
        )


class RPCClientError(Exception):
    """Structured local connection, remote, or protocol failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


async def call(
    socket_path: Path,
    method: str,
    params: JsonObject,
    *,
    request_id: str | None = None,
) -> JsonValue:
    """Perform one finite correlated RPC call."""

    correlation_id = str(uuid.uuid4()) if request_id is None else request_id
    _validate_outgoing_request_id(correlation_id)
    reader, writer = await _open_connection(socket_path)
    try:
        try:
            await _write_frame(
                writer,
                {
                    "request_id": correlation_id,
                    "method": method,
                    "params": params,
                },
            )
        except _FrameTooLargeError:
            raise RPCClientError("frame_too_large", _frame_limit_message()) from None
        response = await _read_response(reader, correlation_id)
        return response["result"]
    finally:
        writer.close()
        with suppress(BrokenPipeError, ConnectionResetError):
            await writer.wait_closed()


async def watch(
    socket_path: Path,
    params: JsonObject,
    *,
    request_id: str | None = None,
) -> AsyncIterator[JsonObject]:
    """Yield correlated event objects from one upgraded watch connection."""

    correlation_id = str(uuid.uuid4()) if request_id is None else request_id
    _validate_outgoing_request_id(correlation_id)
    reader, writer = await _open_connection(socket_path)
    try:
        try:
            await _write_frame(
                writer,
                {
                    "request_id": correlation_id,
                    "method": "watch",
                    "params": params,
                },
            )
        except _FrameTooLargeError:
            raise RPCClientError("frame_too_large", _frame_limit_message()) from None
        await _read_response(reader, correlation_id)
        follow = params.get("follow") is True
        while True:
            try:
                line = await _read_frame(reader, request_id=correlation_id)
            except _FrameTooLargeError:
                raise RPCClientError(
                    "frame_too_large", _frame_limit_message()
                ) from None
            except (ConnectionError, OSError):
                raise RPCClientError(
                    "supervisor_unavailable",
                    "qworker watch connection ended before a terminal frame.",
                ) from None
            if line is None:
                raise RPCClientError(
                    "supervisor_unavailable",
                    "qworker watch connection ended before a terminal frame.",
                )
            response = _validated_response(line, correlation_id)
            result = response["result"]
            if not isinstance(result, dict):
                raise RPCClientError(
                    "sdk_protocol_error", "Supervisor returned an invalid watch frame."
                )
            if set(result) == {"event"}:
                event = result["event"]
                if not isinstance(event, dict):
                    raise RPCClientError(
                        "sdk_protocol_error", "Supervisor returned an invalid event."
                    )
                yield event
                continue
            if set(result) != {"end"} or not isinstance(result["end"], dict):
                raise RPCClientError(
                    "sdk_protocol_error", "Supervisor returned an invalid watch frame."
                )
            end = result["end"]
            if set(end) != {"reason", "cursor", "state"}:
                raise RPCClientError(
                    "sdk_protocol_error", "Supervisor returned an invalid watch end."
                )
            reason = end["reason"]
            cursor = end["cursor"]
            state = end["state"]
            expected_reason = "terminal" if follow else "replay_complete"
            if (
                reason != expected_reason
                or not isinstance(cursor, int)
                or isinstance(cursor, bool)
                or cursor < 0
                or not isinstance(state, str)
                or (follow and state not in _TERMINAL_STATES)
            ):
                raise RPCClientError(
                    "sdk_protocol_error", "Supervisor returned an invalid watch end."
                )
            return
    finally:
        writer.close()
        with suppress(BrokenPipeError, ConnectionResetError):
            await writer.wait_closed()


async def _open_connection(
    socket_path: Path,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    try:
        return await asyncio.open_unix_connection(
            socket_path, limit=_MAX_FRAME_BYTES
        )
    except OSError:
        raise RPCClientError(
            "supervisor_unavailable", "qworker supervisor is unavailable."
        ) from None


async def _socket_is_active(socket_path: Path) -> bool:
    try:
        _reader, writer = await asyncio.open_unix_connection(socket_path)
    except (ConnectionRefusedError, FileNotFoundError):
        return False
    except OSError as error:
        raise RuntimeError("Unable to verify existing RPC socket ownership.") from error
    writer.close()
    with suppress(BrokenPipeError, ConnectionResetError):
        await writer.wait_closed()
    return True


async def _read_response(reader: asyncio.StreamReader, request_id: str) -> JsonObject:
    try:
        line = await _read_frame(reader, request_id=request_id)
    except _FrameTooLargeError:
        raise RPCClientError("frame_too_large", _frame_limit_message()) from None
    except (ConnectionError, OSError):
        raise RPCClientError(
            "supervisor_unavailable", "Supervisor connection was interrupted."
        ) from None
    if not line:
        raise RPCClientError(
            "sdk_protocol_error", "Supervisor closed without a response."
        )
    return _validated_response(line, request_id)


def _validated_response(line: bytes, request_id: str) -> JsonObject:
    try:
        decoded: object = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RPCClientError(
            "sdk_protocol_error", "Supervisor returned invalid JSON."
        ) from None
    if not isinstance(decoded, dict) or not all(
        isinstance(key, str) for key in decoded
    ):
        raise RPCClientError(
            "sdk_protocol_error", "Supervisor returned an invalid response."
        )
    response = cast(JsonObject, decoded)
    if response.get("request_id") != request_id or not isinstance(
        response.get("ok"), bool
    ):
        raise RPCClientError(
            "sdk_protocol_error", "Supervisor response correlation failed."
        )
    if response["ok"] is False:
        error = response.get("error")
        if not isinstance(error, dict):
            raise RPCClientError(
                "sdk_protocol_error", "Supervisor returned an invalid error."
            )
        code = error.get("code")
        message = error.get("message")
        if not isinstance(code, str) or not isinstance(message, str):
            raise RPCClientError(
                "sdk_protocol_error", "Supervisor returned an invalid error."
            )
        raise RPCClientError(code, message)
    if "result" not in response:
        raise RPCClientError(
            "sdk_protocol_error", "Supervisor response omitted its result."
        )
    return response


def _decode_request(line: bytes) -> JsonObject:
    try:
        decoded: object = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Request must be one UTF-8 JSON object.") from error
    if not isinstance(decoded, dict) or not all(
        isinstance(key, str) for key in decoded
    ):
        raise ValueError("Request must be a JSON object.")
    request = cast(JsonObject, decoded)
    _validate_fields(
        request,
        required=("request_id", "method", "params"),
    )
    if not _valid_request_id(request["request_id"]):
        raise ValueError(_request_id_validation_message())
    if not isinstance(request["method"], str) or not request["method"]:
        raise ValueError("method must be a non-empty string.")
    if not isinstance(request["params"], dict):
        raise TypeError("params must be a JSON object.")
    return request


def _validate_fields(
    value: Mapping[str, JsonValue],
    *,
    required: tuple[str, ...],
    optional: tuple[str, ...] = (),
) -> None:
    missing = set(required).difference(value)
    if missing:
        raise ValueError(f"Missing field: {', '.join(sorted(missing))}")
    unexpected = set(value).difference((*required, *optional))
    if unexpected:
        raise ValueError(f"Unexpected field: {', '.join(sorted(unexpected))}")


def _string(value: Mapping[str, JsonValue], field: str) -> str:
    item = value[field]
    if not isinstance(item, str) or not item:
        raise ValueError(f"{field} must be a non-empty string.")
    return item


def _optional_string(
    value: Mapping[str, JsonValue], field: str, *, default: str
) -> str:
    if field not in value:
        return default
    return _string(value, field)


def _string_tuple(value: Mapping[str, JsonValue], field: str) -> tuple[str, ...]:
    if field not in value:
        return ()
    item = value[field]
    if not isinstance(item, list) or not all(isinstance(entry, str) for entry in item):
        raise ValueError(f"{field} must be an array of strings.")
    return tuple(cast(list[str], item))


def _non_negative_integer(
    value: Mapping[str, JsonValue], field: str, *, default: int
) -> int:
    if field not in value:
        return default
    item = value[field]
    if not isinstance(item, int) or isinstance(item, bool) or item < 0:
        raise ValueError(f"{field} must be a non-negative integer.")
    return item


def _boolean(value: Mapping[str, JsonValue], field: str, *, default: bool) -> bool:
    if field not in value:
        return default
    item = value[field]
    if not isinstance(item, bool):
        raise TypeError(f"{field} must be a boolean.")
    return item


def _success(request_id: JsonValue, result: JsonValue) -> JsonObject:
    return {"request_id": request_id, "ok": True, "result": result}


def _failure(request_id: JsonValue, code: str, message: str) -> JsonObject:
    return {
        "request_id": request_id,
        "ok": False,
        "error": {"code": code, "message": message},
    }


async def _write_frame(writer: asyncio.StreamWriter, frame: JsonObject) -> None:
    encoded = json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"
    if len(encoded) > _MAX_FRAME_BYTES:
        raise _FrameTooLargeError(frame.get("request_id"))
    writer.write(encoded)
    await writer.drain()


async def _read_frame(
    reader: asyncio.StreamReader, *, request_id: JsonValue = None
) -> bytes | None:
    try:
        frame = await reader.readuntil(b"\n")
    except asyncio.IncompleteReadError as error:
        if not error.partial:
            return None
        frame = error.partial
    except asyncio.LimitOverrunError as error:
        prefix_size = min(error.consumed, _MAX_FRAME_BYTES)
        prefix = await reader.readexactly(prefix_size)
        raise _FrameTooLargeError(
            request_id if request_id is not None else _request_id_from_prefix(prefix)
        ) from None
    if len(frame) > _MAX_FRAME_BYTES:
        raise _FrameTooLargeError(
            request_id if request_id is not None else _request_id_from_prefix(frame)
        )
    return frame


def _request_id_from_prefix(prefix: bytes) -> JsonValue:
    match = _REQUEST_ID_PATTERN.search(prefix)
    if match is None:
        return None
    if match.end(1) - match.start(1) > _MAX_ENCODED_REQUEST_ID_BYTES:
        return None
    try:
        decoded: object = json.loads(match.group(1))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return cast(str, decoded) if _valid_request_id(decoded) else None


def _validate_outgoing_request_id(request_id: str) -> None:
    if not _valid_request_id(request_id):
        raise RPCClientError("invalid_request", _request_id_validation_message())


def _valid_request_id(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= _MAX_REQUEST_ID_CHARS


def _request_id_validation_message() -> str:
    return (
        "request_id must be a non-empty string of at most "
        f"{_MAX_REQUEST_ID_CHARS} characters."
    )


def _frame_limit_message() -> str:
    return f"RPC frame exceeds the {_MAX_FRAME_BYTES}-byte limit."


def default_state_dir() -> Path:
    """Return the documented private durable-state directory."""

    state_home = os.environ.get("XDG_STATE_HOME")
    if state_home:
        return Path(state_home) / "qworker"
    return Path.home() / ".local" / "state" / "qworker"


async def run_server(socket_path: Path, state_dir: Path) -> None:
    """Run the production supervisor service until a termination signal arrives."""

    from qworker.qoder_sdk import create_default_transport
    from qworker.store import WorkerStore

    supervisor = Supervisor(
        WorkerStore(state_dir),
        create_default_transport,
        sdk_version=version("qoder-agent-sdk"),
    )
    server = RPCServer(supervisor, socket_path)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signum, stopped.set)
            registered_signals.append(signum)
    try:
        await server.start()
        await stopped.wait()
    finally:
        for signum in registered_signals:
            loop.remove_signal_handler(signum)
        await server.close()
        await supervisor.close()


def main(argv: list[str] | None = None) -> int:
    """Internal foreground service entry point used by CLI auto-start."""

    import argparse

    parser = argparse.ArgumentParser(prog="python -m qworker.rpc")
    parser.add_argument("--serve", action="store_true", required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    arguments = parser.parse_args(sys.argv[1:] if argv is None else argv)
    asyncio.run(run_server(arguments.socket, arguments.state_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
