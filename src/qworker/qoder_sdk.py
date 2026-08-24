"""Qoder Agent SDK 1.0.13 adapter."""

import asyncio
import json
import os
import platform
import re
import tempfile
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from importlib.metadata import version
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from qoder_agent_sdk import (
    AccessTokenAuthOptions,
    AccessTokenEnvVar,
    AssistantMessage,
    AuthAccessTokenEnvVarError,
    AuthNotConfiguredError,
    AuthOptions,
    AuthServiceAccountEnvVarError,
    CLINotFoundError,
    PermissionResultAllow,
    PermissionResultDeny,
    ProcessError,
    QoderAgentOptions,
    QoderCLIAuthOptions,
    QoderSDKClient,
    ResultMessage,
    SDKPermissionDeniedMessage,
    ServiceAccountAuthOptions,
    ServiceAccountEnvVar,
    SystemMessage,
    TaskNotificationMessage,
    TaskProgressMessage,
    TaskStartedMessage,
    TextBlock,
    ToolPermissionContext,
    ToolUseBlock,
    access_token_from_env,
    qodercli_auth,
    service_account_from_env,
)
from qoder_agent_sdk import (
    ElicitationRequest as SDKElicitationRequest,
)
from qoder_agent_sdk import (
    ElicitationResult as SDKElicitationResult,
)

from qworker.auditor_policy import AuditorToolPolicy
from qworker.config import ConfigError, load_config
from qworker.control import (
    ControlCallbacks,
    ElicitationDecision,
    ElicitationRequest,
    PermissionDecision,
    PermissionRequest,
    SteeringPriority,
)
from qworker.events import (
    AdapterEvent,
    AssistantEvent,
    ResultEvent,
    SystemEvent,
    TaskProgressEvent,
    TaskStartedEvent,
    TaskTerminalEvent,
)
from qworker.model_policy import AvailableModel
from qworker.preflight import (
    AuthSelection,
    PreflightFailure,
    RuntimeInfo,
    normalize_server_capabilities,
    safe_preflight_failure,
    select_auth,
)

_CREDENTIAL_ENV_VARS = (
    "QODER_PERSONAL_ACCESS_TOKEN",
    "QODER_SERVICE_ACCOUNT_KEY",
    "QODERCN_PERSONAL_ACCESS_TOKEN",
    "QODERCN_SERVICE_ACCOUNT_KEY",
)
_MAX_SYSTEM_DIAGNOSTIC_CHARS = 512
_MISSING = object()
_MINIMUM_RUNTIME_VERSION = (0, 2, 0)
_RUNTIME_VERSION = re.compile(r"^\s*([0-9]+)\.([0-9]+)\.([0-9]+)")

type AdapterDiagnosticCode = Literal[
    "auth_required",
    "initialize_timeout",
    "invalid_request",
    "runtime_not_found",
    "runtime_incompatible",
    "sdk_protocol_error",
]


class AdapterDiagnostic(RuntimeError):
    """Structured, display-safe failure at the SDK adapter boundary."""

    def __init__(self, code: AdapterDiagnosticCode, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def build_auditor_options(
    cwd: str | Path,
    *,
    auth: AuthOptions | None = None,
    cli_path: Path | None = None,
    resume: str | None = None,
) -> QoderAgentOptions:
    """Build isolated SDK options enforcing the auditor tool policy."""

    if auth is not None and not isinstance(
        auth,
        (AccessTokenAuthOptions, QoderCLIAuthOptions, ServiceAccountAuthOptions),
    ):
        raise TypeError("Use a qoder_agent_sdk SDK auth helper.")

    policy = AuditorToolPolicy()

    async def can_use_tool(
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        decision = policy.decide(tool_name, agent_id=context.agent_id)
        if decision.allowed:
            return PermissionResultAllow(updated_input=tool_input)
        return PermissionResultDeny(message=decision.reason)

    return QoderAgentOptions(
        auth=auth,
        cwd=cwd,
        cli_path=cli_path,
        resume=resume,
        setting_sources=[],
        tools=list(policy.visible_tools),
        allowed_tools=list(policy.visible_tools),
        disallowed_tools=list(policy.denied_tools),
        permission_mode="dontAsk",
        max_turns=24,
        control_request_timeout_ms=30_000,
        load_timeout_ms=30_000,
        max_buffer_size=16 * 1024 * 1024,
        can_use_tool=can_use_tool,
    )


def create_default_transport(cwd: Path) -> "QoderSDKTransport":
    """Create production transport from the same config/auth policy as preflight."""

    options = build_configured_auditor_options(cwd)
    return QoderSDKTransport(QoderSDKClient(options=options))


def create_resumed_transport(cwd: Path, session_id: str) -> "QoderSDKTransport":
    """Create a fresh production transport resumed from stored conversation history."""

    options = build_configured_auditor_options(cwd, resume=session_id)
    return QoderSDKTransport(QoderSDKClient(options=options))


def build_configured_auditor_options(
    cwd: Path,
    *,
    user_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
    resume: str | None = None,
) -> QoderAgentOptions:
    """Build worker options from validated config without reading credential values."""

    selected_environment = os.environ if environ is None else environ
    try:
        config = load_config(cwd, user_path=user_path)
        auth = select_auth(config, selected_environment)
        if config.runtime_path is None:
            runtime_path = _bundled_runtime_path()
        else:
            try:
                runtime_path = config.runtime_path.resolve(strict=True)
            except OSError:
                raise PreflightFailure(
                    "runtime_not_found", "Configured Qoder runtime was not found."
                ) from None
            if not runtime_path.is_file() or not os.access(runtime_path, os.X_OK):
                raise PreflightFailure(
                    "runtime_not_found",
                    "Configured Qoder runtime is not executable.",
                )
    except ConfigError as error:
        raise AdapterDiagnostic("invalid_request", error.message) from None
    except PreflightFailure as error:
        if error.code not in {
            "auth_required",
            "runtime_not_found",
            "runtime_incompatible",
        }:
            raise AdapterDiagnostic("sdk_protocol_error", error.message) from None
        raise AdapterDiagnostic(
            cast(AdapterDiagnosticCode, error.code), error.message
        ) from None
    return build_auditor_options(
        cwd,
        auth=_sdk_auth(auth),
        cli_path=runtime_path,
        resume=resume,
    )


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Bounded subprocess observation used for runtime diagnostics."""

    returncode: int
    stdout: str = ""


class _ControlClient(Protocol):
    async def connect(self, prompt: None = None) -> None: ...

    async def get_server_info(self) -> Mapping[str, object] | None: ...

    async def disconnect(self) -> None: ...


type ControlClientFactory = Callable[[QoderAgentOptions], object]
type CommandRunner = Callable[[tuple[str, ...]], Awaitable[CommandResult]]


class QoderPreflightBackend:
    """Pinned public-SDK backend for runtime and control preflight."""

    def __init__(
        self,
        *,
        client_factory: ControlClientFactory | None = None,
        command_runner: CommandRunner | None = None,
    ) -> None:
        self._client_factory = client_factory or _new_control_client
        self._command_runner = command_runner or _run_runtime_command

    def sdk_version(self) -> str:
        return version("qoder-agent-sdk")

    async def resolve_runtime(self, explicit_path: Path | None) -> RuntimeInfo:
        recorded_path = "bundled"
        if explicit_path is None:
            executable = _bundled_runtime_path()
        else:
            try:
                executable = explicit_path.resolve(strict=True)
            except OSError:
                raise PreflightFailure(
                    "runtime_not_found", "Configured Qoder runtime was not found."
                ) from None
            recorded_path = str(executable)
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise PreflightFailure(
                "runtime_not_found", "Configured Qoder runtime is not executable."
            )
        try:
            result = await self._command_runner((str(executable), "-v"))
        except PreflightFailure:
            raise
        except Exception:  # noqa: BLE001 -- injected runner is a system boundary
            raise PreflightFailure(
                "runtime_incompatible", "Unable to read Qoder runtime version."
            ) from None
        if result.returncode != 0:
            raise PreflightFailure(
                "runtime_incompatible", "Unable to read Qoder runtime version."
            )
        match = _RUNTIME_VERSION.match(result.stdout)
        if match is None:
            raise PreflightFailure(
                "runtime_incompatible", "Qoder runtime returned an invalid version."
            )
        version_parts = tuple(int(part) for part in match.groups())
        if version_parts < _MINIMUM_RUNTIME_VERSION:
            raise PreflightFailure(
                "runtime_incompatible",
                "Qoder runtime does not satisfy the SDK minimum version.",
            )
        runtime_version = ".".join(match.groups())
        return RuntimeInfo(recorded_path, executable, runtime_version)

    async def initialize(
        self,
        cwd: Path,
        runtime: RuntimeInfo,
        auth: AuthSelection,
    ) -> tuple[str, ...]:
        sdk_auth = _sdk_auth(auth)
        options = build_auditor_options(
            cwd,
            auth=sdk_auth,
            cli_path=runtime.executable,
        )
        client = cast(_ControlClient, self._client_factory(options))
        credential_values = _direct_credential_values(client)
        primary_error: Exception | None = None
        server_info: Mapping[str, object] | None = None
        try:
            await client.connect(None)
            server_info = await client.get_server_info()
            if server_info is None:
                raise AdapterDiagnostic(
                    "sdk_protocol_error",
                    "Qoder runtime returned no initialization information.",
                )
        except Exception as error:  # noqa: BLE001 -- classified below
            primary_error = error
        finally:
            try:
                await client.disconnect()
            except Exception as error:  # noqa: BLE001 -- classified below
                if primary_error is None:
                    primary_error = error
        if primary_error is not None:
            diagnostic = classify_sdk_error(
                primary_error,
                operation="initialize",
                credential_values=credential_values,
            )
            safe_diagnostic = safe_preflight_failure(diagnostic.code)
            raise PreflightFailure(
                safe_diagnostic.code, safe_diagnostic.message
            ) from None
        assert server_info is not None
        raw_capabilities = server_info.get("capabilities", {})
        if not isinstance(raw_capabilities, Mapping):
            return ()
        return normalize_server_capabilities(
            tuple(key for key in raw_capabilities if isinstance(key, str))
        )

    async def local_login_status(self, runtime: RuntimeInfo) -> bool:
        try:
            result = await self._command_runner((str(runtime.executable), "status"))
        except Exception:  # noqa: BLE001 -- command output is deliberately discarded
            return False
        return result.returncode == 0


def _new_control_client(options: QoderAgentOptions) -> object:
    return QoderSDKClient(options=options)


def _bundled_runtime_path() -> Path:
    runtime_name = "qodercli.exe" if platform.system() == "Windows" else "qodercli"
    bundled = Path(str(files("qoder_agent_sdk").joinpath("_bundled", runtime_name)))
    try:
        return bundled.resolve(strict=True)
    except OSError:
        raise PreflightFailure(
            "runtime_not_found", "Bundled Qoder runtime was not found."
        ) from None


async def _run_runtime_command(command: tuple[str, ...]) -> CommandResult:
    process: asyncio.subprocess.Process | None = None
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        async with asyncio.timeout(5):
            stdout, _ = await process.communicate()
    except FileNotFoundError:
        raise PreflightFailure(
            "runtime_not_found", "Qoder runtime was not found."
        ) from None
    except TimeoutError:
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()
        raise PreflightFailure(
            "runtime_incompatible", "Qoder runtime command timed out."
        ) from None
    return CommandResult(
        returncode=process.returncode or 0,
        stdout=stdout[:4096].decode("utf-8", errors="replace"),
    )


def _sdk_auth(auth: AuthSelection) -> AuthOptions:
    if auth.source == "personal_access_token":
        if auth.env_var is None:
            raise PreflightFailure(
                "auth_required", "Personal access-token environment is missing."
            )
        return access_token_from_env(auth.env_var)
    if auth.source == "service_account":
        if auth.env_var is None:
            raise PreflightFailure(
                "auth_required", "Service-account environment is missing."
            )
        return service_account_from_env(auth.env_var)
    return qodercli_auth()


type SDKOperation = Literal[
    "initialize",
    "runtime_construction",
    "model_discovery",
    "model_selection",
    "query",
    "steer",
    "cancel_message",
    "interrupt",
    "stream",
    "disconnect",
]


def classify_sdk_error(
    error: BaseException,
    *,
    operation: SDKOperation,
    credential_values: tuple[str, ...] = (),
) -> AdapterDiagnostic:
    """Classify an SDK failure without exposing credential values."""

    raw_message = str(error) or type(error).__name__
    message = _safe_bounded_text(raw_message, credential_values)
    if isinstance(error, AdapterDiagnostic):
        return AdapterDiagnostic(error.code, message)
    if isinstance(
        error,
        (
            AuthAccessTokenEnvVarError,
            AuthNotConfiguredError,
            AuthServiceAccountEnvVarError,
        ),
    ):
        return AdapterDiagnostic("auth_required", message)
    if operation in {"initialize", "runtime_construction"}:
        if (
            isinstance(error, TimeoutError)
            or "control request timeout: initialize" in raw_message.casefold()
        ):
            return AdapterDiagnostic("initialize_timeout", message)
        if isinstance(error, (CLINotFoundError, FileNotFoundError)):
            return AdapterDiagnostic("runtime_not_found", message)
    return AdapterDiagnostic("sdk_protocol_error", message)


class _SDKClient(Protocol):
    async def connect(
        self, prompt: str | AsyncIterable[dict[str, Any]] | None = None
    ) -> None: ...

    async def get_available_models(self) -> Sequence[Mapping[str, object]]: ...

    async def set_model(self, model: str | None = None) -> None: ...

    async def query(
        self,
        prompt: str,
        session_id: str = "default",
        *,
        priority: SteeringPriority = "next",
        message_uuid: str | None = None,
        should_query: bool = True,
    ) -> None: ...

    async def cancel_async_message(self, message_uuid: str) -> bool: ...

    async def interrupt(self) -> None: ...

    def receive_messages(self) -> AsyncIterator[object]: ...

    async def disconnect(self) -> None: ...


class _PinnedSDKSubprocessTransport(Protocol):
    _process: object | None
    _auth_payload_path: Path | None


class QoderSDKTransport:
    """Adapt one injected SDK client to the stable worker transport seam."""

    def __init__(self, client: object) -> None:
        self._client = cast(_SDKClient, client)
        self._credential_values = _direct_credential_values(client)
        self._model_identifiers: dict[str, str] = {}
        self._control_callbacks: ControlCallbacks | None = None
        self._disconnect_lock = asyncio.Lock()
        self._disconnected = False

    async def connect(self) -> None:
        await _call_sdk(
            lambda: self._client.connect(None),
            operation="initialize",
            credential_values=self._credential_values,
        )

    async def available_models(self) -> tuple[AvailableModel, ...]:
        async def load_catalog() -> tuple[AvailableModel, ...]:
            catalog = await self._client.get_available_models()
            models: list[AvailableModel] = []
            identifiers: dict[str, str] = {}
            for item in catalog:
                identifier = item.get("value")
                display_name = item.get("displayName")
                enabled = item.get("isEnabled")
                if isinstance(identifier, str) and isinstance(enabled, bool):
                    safe_identifier = _redact(identifier, self._credential_values)
                    value = (
                        display_name if isinstance(display_name, str) else identifier
                    )
                    safe_value = _redact(value, self._credential_values)
                    identifiers[safe_value] = safe_identifier
                    models.append(
                        AvailableModel(
                            value=safe_value,
                            enabled=enabled,
                        )
                    )
            self._model_identifiers = identifiers
            return tuple(models)

        return await _call_sdk(
            load_catalog,
            operation="model_discovery",
            credential_values=self._credential_values,
        )

    async def select_model(self, model: str) -> None:
        identifier = self._model_identifiers.get(model, model)
        await _call_sdk(
            lambda: self._client.set_model(identifier),
            operation="model_selection",
            credential_values=self._credential_values,
        )

    async def send(self, prompt: str) -> None:
        await _call_sdk(
            lambda: self._client.query(prompt),
            operation="query",
            credential_values=self._credential_values,
        )

    def bind_control(self, callbacks: ControlCallbacks) -> None:
        """Bind supervisor callbacks before the SDK client connects."""

        self._control_callbacks = callbacks
        options = getattr(self._client, "options", None)
        if not isinstance(options, QoderAgentOptions):
            raise AdapterDiagnostic(
                "sdk_protocol_error",
                "SDK client does not expose mutable control options.",
            )
        policy_callback = options.can_use_tool

        async def can_use_tool(
            tool_name: str,
            tool_input: dict[str, Any],
            context: ToolPermissionContext,
        ) -> PermissionResultAllow | PermissionResultDeny:
            if policy_callback is None:
                return PermissionResultDeny(
                    message="Tool permission policy is unavailable."
                )
            policy_decision = await policy_callback(tool_name, tool_input, context)
            if isinstance(policy_decision, PermissionResultDeny):
                return policy_decision
            display_message = (
                context.title
                or context.display_name
                or context.description
                or f"Permission requested for {tool_name}."
            )
            try:
                decision = await callbacks.request_permission(
                    PermissionRequest(
                        tool_name=tool_name,
                        agent_id=context.agent_id,
                        display_message=display_message,
                    )
                )
            except Exception:  # noqa: BLE001 -- callback failures deny permission
                decision = PermissionDecision("deny")
            if decision.action == "allow":
                return PermissionResultAllow(updated_input=tool_input)
            return PermissionResultDeny(
                message="Permission denied by supervisor response."
            )

        async def on_elicitation(
            request: SDKElicitationRequest,
        ) -> SDKElicitationResult:
            mode = request.get("mode", "form")
            if mode not in ("form", "url"):
                return {"action": "cancel"}
            requested_schema = request.get("requestedSchema")
            try:
                decision = await callbacks.request_elicitation(
                    ElicitationRequest(
                        server_name=request["serverName"],
                        mode=mode,
                        display_message=request["message"],
                        requested_schema=(
                            requested_schema
                            if isinstance(requested_schema, dict)
                            else None
                        ),
                    )
                )
            except Exception:  # noqa: BLE001 -- callback failures cancel elicitation
                decision = ElicitationDecision("cancel")
            result: SDKElicitationResult = {"action": decision.action}
            if decision.action == "accept" and decision.content is not None:
                result["content"] = cast(dict[str, Any], decision.content)
            return result

        options.can_use_tool = can_use_tool
        options.on_elicitation = on_elicitation

    async def steer(
        self,
        message: str,
        *,
        priority: SteeringPriority,
        message_id: str,
    ) -> None:
        await _call_sdk(
            lambda: self._client.query(
                message,
                priority=priority,
                message_uuid=message_id,
            ),
            operation="steer",
            credential_values=self._credential_values,
        )

    async def cancel_message(self, message_id: str) -> bool:
        return await _call_sdk(
            lambda: self._client.cancel_async_message(message_id),
            operation="cancel_message",
            credential_values=self._credential_values,
        )

    async def interrupt(self) -> None:
        await _call_sdk(
            self._client.interrupt,
            operation="interrupt",
            credential_values=self._credential_values,
        )

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        try:
            async for message in self._client.receive_messages():
                event = _map_sdk_message(message, self._credential_values)
                if event is not None:
                    yield event
        except ProcessError:
            raise EOFError("QoderCLI process exited.") from None
        # SDK 1.0.13 raises plain Exception for initialize control timeouts.
        except Exception as error:  # noqa: BLE001
            diagnostic = classify_sdk_error(
                error,
                operation="stream",
                credential_values=self._credential_values,
            )
        else:
            return
        raise diagnostic

    async def disconnect(self) -> None:
        async with self._disconnect_lock:
            if self._disconnected:
                return
            await _call_sdk(
                self._client.disconnect,
                operation="disconnect",
                credential_values=self._credential_values,
            )
            self._disconnected = True

    def abort(self) -> None:
        """Safely own pinned-SDK subprocess and credential fallback cleanup."""

        transport = _sdk_subprocess_transport(self._client)
        if transport is None:
            return
        process_handled = _abort_sdk_process(transport)
        credentials_handled = _cleanup_sdk_auth_payload(transport)
        if process_handled and credentials_handled:
            self._disconnected = True


def _sdk_subprocess_transport(
    client: object,
) -> _PinnedSDKSubprocessTransport | None:
    """Locate SDK 1.0.13 transport after or during query construction."""

    query = getattr(client, "_query", None)
    candidates = (
        getattr(query, "transport", None),
        getattr(client, "_transport", None),
    )
    seen: set[int] = set()
    for transport in candidates:
        if transport is None or id(transport) in seen:
            continue
        seen.add(id(transport))
        if (
            getattr(transport, "_process", _MISSING) is not _MISSING
            and getattr(transport, "_auth_payload_path", _MISSING) is not _MISSING
        ):
            return cast(_PinnedSDKSubprocessTransport, transport)
    return None


def _abort_sdk_process(transport: _PinnedSDKSubprocessTransport) -> bool:
    """Kill only a process whose pinned live-state marker is available."""

    process = transport._process
    if process is None:
        return True
    returncode = getattr(process, "returncode", _MISSING)
    if returncode is _MISSING:
        return False
    if returncode is not None:
        return True
    kill = getattr(process, "kill", None)
    if not callable(kill):
        return False
    try:
        kill()
    except ProcessLookupError:
        return True
    except Exception:  # noqa: BLE001 -- normal SDK cleanup remains retryable
        return False
    return True


def _cleanup_sdk_auth_payload(transport: _PinnedSDKSubprocessTransport) -> bool:
    """Remove only pinned SDK payload path and its empty temporary directory."""

    raw_path = transport._auth_payload_path
    if raw_path is None:
        return True
    if not isinstance(raw_path, Path) or not raw_path.is_absolute():
        return False
    auth_dir = raw_path.parent
    try:
        temp_root = Path(tempfile.gettempdir()).resolve(strict=True)
        if (
            raw_path.name != "payload.json"
            or not auth_dir.name.startswith("qoder-sdk-auth-")
            or auth_dir.is_symlink()
            or auth_dir.resolve(strict=False).parent != temp_root
            or (auth_dir.exists() and not auth_dir.is_dir())
            or raw_path.is_symlink()
            or (raw_path.exists() and not raw_path.is_file())
        ):
            return False
        raw_path.unlink(missing_ok=True)
        transport._auth_payload_path = None
        try:
            auth_dir.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            pass
    except OSError:
        return False
    return True


async def _call_sdk[ResultT](
    call: Callable[[], Awaitable[ResultT]],
    *,
    operation: SDKOperation,
    credential_values: tuple[str, ...],
) -> ResultT:
    try:
        return await call()
    # SDK 1.0.13 raises plain Exception for initialize control timeouts.
    except Exception as error:  # noqa: BLE001
        diagnostic = classify_sdk_error(
            error,
            operation=operation,
            credential_values=credential_values,
        )
    raise diagnostic


def _map_sdk_message(
    message: object,
    credential_values: tuple[str, ...],
) -> AdapterEvent | None:
    if isinstance(message, AssistantMessage):
        return AssistantEvent(
            text=tuple(
                _redact(block.text, credential_values)
                for block in message.content
                if isinstance(block, TextBlock)
            ),
            tools=tuple(
                _redact(block.name, credential_values)
                for block in message.content
                if isinstance(block, ToolUseBlock)
            ),
            model=_redact(message.model, credential_values),
        )
    if isinstance(message, TaskStartedMessage):
        return TaskStartedEvent(
            task_id=_redact(message.task_id, credential_values),
            description=_redact(message.description, credential_values),
        )
    if isinstance(message, TaskProgressMessage):
        return TaskProgressEvent(
            task_id=_redact(message.task_id, credential_values),
            description=_redact(message.description, credential_values),
            last_tool_name=(
                _redact(message.last_tool_name, credential_values)
                if message.last_tool_name is not None
                else None
            ),
        )
    if isinstance(message, TaskNotificationMessage):
        return TaskTerminalEvent(
            task_id=_redact(message.task_id, credential_values),
            status=message.status,
        )
    if isinstance(message, SDKPermissionDeniedMessage):
        return SystemEvent(
            subtype="permission_denied",
            message=_permission_summary(
                message.tool_name,
                message.message,
                credential_values,
            ),
        )
    if isinstance(message, SystemMessage):
        return SystemEvent(
            subtype=_safe_bounded_text(message.subtype, credential_values),
            message=_bounded_diagnostic(message.data, credential_values),
        )
    if isinstance(message, ResultMessage):
        return ResultEvent(
            session_id=_redact(message.session_id, credential_values),
            is_error=message.is_error,
            result=(
                _redact(message.result, credential_values)
                if message.result is not None
                else None
            ),
            model_usage=tuple(
                _redact(model, credential_values)
                for model in (message.model_usage or {})
            ),
            permission_denials=tuple(
                _permission_summary(
                    denial.get("tool_name", "unknown"),
                    denial.get("message", "denied"),
                    credential_values,
                )
                for denial in (message.permission_denials or ())
            ),
            errors=tuple(
                _redact(error, credential_values) for error in (message.errors or ())
            ),
        )
    return None


def _permission_summary(
    tool_name: str,
    message: str,
    credential_values: tuple[str, ...],
) -> str:
    return _safe_bounded_text(f"{tool_name}: {message}", credential_values)


def _bounded_diagnostic(data: object, credential_values: tuple[str, ...]) -> str:
    rendered = json.dumps(data, ensure_ascii=False, sort_keys=True, default=repr)
    return _safe_bounded_text(rendered, credential_values)


def _safe_bounded_text(value: str, credential_values: tuple[str, ...]) -> str:
    return _bounded_text(_redact(value, credential_values))


def _bounded_text(safe: str) -> str:
    if len(safe) <= _MAX_SYSTEM_DIAGNOSTIC_CHARS:
        return safe
    return safe[: _MAX_SYSTEM_DIAGNOSTIC_CHARS - 3] + "..."


def _redact(value: str, credential_values: tuple[str, ...] = ()) -> str:
    safe = value
    for direct_secret in credential_values:
        if direct_secret:
            safe = safe.replace(direct_secret, "[REDACTED]")
    for variable in _CREDENTIAL_ENV_VARS:
        environment_secret = os.environ.get(variable)
        if environment_secret:
            safe = safe.replace(environment_secret, "[REDACTED]")
    return safe


def _direct_credential_values(client: object) -> tuple[str, ...]:
    options = getattr(client, "options", None)
    if not isinstance(options, QoderAgentOptions):
        return ()
    auth = options.auth
    if isinstance(auth, AccessTokenAuthOptions):
        value = _auth_input_value(auth.access_token)
        return (value,) if value else ()
    if isinstance(auth, ServiceAccountAuthOptions):
        value = _auth_input_value(auth.service_account_key)
        return (value,) if value else ()
    return ()


def _auth_input_value(
    credential: str | AccessTokenEnvVar | ServiceAccountEnvVar | None,
) -> str | None:
    if isinstance(credential, str):
        return credential
    if isinstance(credential, (AccessTokenEnvVar, ServiceAccountEnvVar)):
        return os.environ.get(credential.env_var)
    return None
