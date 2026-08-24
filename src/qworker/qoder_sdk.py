"""Qoder Agent SDK 1.0.13 adapter."""

import asyncio
import json
import os
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
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
)
from qoder_agent_sdk import (
    ElicitationRequest as SDKElicitationRequest,
)
from qoder_agent_sdk import (
    ElicitationResult as SDKElicitationResult,
)

from qworker.auditor_policy import AuditorToolPolicy
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

_CREDENTIAL_ENV_VARS = (
    "QODER_PERSONAL_ACCESS_TOKEN",
    "QODER_SERVICE_ACCOUNT_KEY",
    "QODERCN_PERSONAL_ACCESS_TOKEN",
    "QODERCN_SERVICE_ACCOUNT_KEY",
)
_MAX_SYSTEM_DIAGNOSTIC_CHARS = 512

type AdapterDiagnosticCode = Literal[
    "auth_required",
    "initialize_timeout",
    "runtime_not_found",
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
    """Create the production transport using unattended PAT authentication."""

    if not os.environ.get("QODER_PERSONAL_ACCESS_TOKEN"):
        raise AdapterDiagnostic(
            "auth_required",
            "Set QODER_PERSONAL_ACCESS_TOKEN before starting a Qoder audit.",
        )
    options = build_auditor_options(cwd, auth=access_token_from_env())
    return QoderSDKTransport(QoderSDKClient(options=options))


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
