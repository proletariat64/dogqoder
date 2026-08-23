"""Qoder Agent SDK 1.0.13 adapter."""

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
    AssistantMessage,
    AuthAccessTokenEnvVarError,
    AuthNotConfiguredError,
    AuthServiceAccountEnvVarError,
    CLINotFoundError,
    InternalAuthOptions,
    PermissionResultAllow,
    PermissionResultDeny,
    QoderAgentOptions,
    QoderSDKClient,
    ResultMessage,
    SDKPermissionDeniedMessage,
    SystemMessage,
    TaskNotificationMessage,
    TaskProgressMessage,
    TaskStartedMessage,
    TextBlock,
    ToolPermissionContext,
    ToolUseBlock,
    access_token_from_env,
)

from qworker.auditor_policy import AuditorToolPolicy
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

_VISIBLE_TOOLS = ["Read", "Glob", "Grep", "WebFetch", "WebSearch", "Agent"]
_DISALLOWED_TOOLS = ["Write", "Edit", "Bash", "NotebookEdit"]
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
    auth: InternalAuthOptions | dict[str, Any] | None = None,
) -> QoderAgentOptions:
    """Build isolated SDK options enforcing the auditor tool policy."""

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
        tools=list(_VISIBLE_TOOLS),
        allowed_tools=list(_VISIBLE_TOOLS),
        disallowed_tools=list(_DISALLOWED_TOOLS),
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


def classify_sdk_error(error: BaseException) -> AdapterDiagnostic:
    """Classify an SDK failure without exposing credential values."""

    raw_message = str(error) or type(error).__name__
    message = _bounded_text(_redact(raw_message))
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

    async def query(self, prompt: str) -> None: ...

    def receive_messages(self) -> AsyncIterator[object]: ...

    async def disconnect(self) -> None: ...


class QoderSDKTransport:
    """Adapt one injected SDK client to the stable worker transport seam."""

    def __init__(self, client: object) -> None:
        self._client = cast(_SDKClient, client)

    async def connect(self) -> None:
        await _call_sdk(lambda: self._client.connect(None))

    async def available_models(self) -> tuple[AvailableModel, ...]:
        async def load_catalog() -> tuple[AvailableModel, ...]:
            catalog = await self._client.get_available_models()
            models: list[AvailableModel] = []
            for item in catalog:
                value = item.get("value")
                enabled = item.get("isEnabled")
                if isinstance(value, str) and isinstance(enabled, bool):
                    models.append(AvailableModel(value=_redact(value), enabled=enabled))
            return tuple(models)

        return await _call_sdk(load_catalog)

    async def select_model(self, model: str) -> None:
        await _call_sdk(lambda: self._client.set_model(model))

    async def send(self, prompt: str) -> None:
        await _call_sdk(lambda: self._client.query(prompt))

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        try:
            async for message in self._client.receive_messages():
                event = _map_sdk_message(message)
                if event is not None:
                    yield event
        # SDK 1.0.13 raises plain Exception for initialize control timeouts.
        except Exception as error:  # noqa: BLE001
            diagnostic = classify_sdk_error(error)
        else:
            return
        raise diagnostic

    async def disconnect(self) -> None:
        await _call_sdk(self._client.disconnect)


async def _call_sdk[ResultT](operation: Callable[[], Awaitable[ResultT]]) -> ResultT:
    try:
        return await operation()
    # SDK 1.0.13 raises plain Exception for initialize control timeouts.
    except Exception as error:  # noqa: BLE001
        diagnostic = classify_sdk_error(error)
    raise diagnostic


def _map_sdk_message(message: object) -> AdapterEvent | None:
    if isinstance(message, AssistantMessage):
        return AssistantEvent(
            text=tuple(
                _redact(block.text)
                for block in message.content
                if isinstance(block, TextBlock)
            ),
            tools=tuple(
                _redact(block.name)
                for block in message.content
                if isinstance(block, ToolUseBlock)
            ),
            model=_redact(message.model),
        )
    if isinstance(message, TaskStartedMessage):
        return TaskStartedEvent(
            task_id=_redact(message.task_id),
            description=_redact(message.description),
        )
    if isinstance(message, TaskProgressMessage):
        return TaskProgressEvent(
            task_id=_redact(message.task_id),
            description=_redact(message.description),
            last_tool_name=(
                _redact(message.last_tool_name)
                if message.last_tool_name is not None
                else None
            ),
        )
    if isinstance(message, TaskNotificationMessage):
        return TaskTerminalEvent(
            task_id=_redact(message.task_id),
            status=message.status,
        )
    if isinstance(message, SDKPermissionDeniedMessage):
        return SystemEvent(
            subtype="permission_denied",
            message=_permission_summary(message.tool_name, message.message),
        )
    if isinstance(message, SystemMessage):
        return SystemEvent(
            subtype=_redact(message.subtype),
            message=_bounded_diagnostic(message.data),
        )
    if isinstance(message, ResultMessage):
        return ResultEvent(
            session_id=_redact(message.session_id),
            is_error=message.is_error,
            result=_redact(message.result) if message.result is not None else None,
            model_usage=tuple(_redact(model) for model in (message.model_usage or {})),
            permission_denials=tuple(
                _permission_summary(
                    denial.get("tool_name", "unknown"),
                    denial.get("message", "denied"),
                )
                for denial in (message.permission_denials or ())
            ),
            errors=tuple(_redact(error) for error in (message.errors or ())),
        )
    return None


def _permission_summary(tool_name: str, message: str) -> str:
    return _redact(f"{tool_name}: {message}")


def _bounded_diagnostic(data: object) -> str:
    rendered = json.dumps(data, ensure_ascii=False, sort_keys=True, default=repr)
    return _bounded_text(_redact(rendered))


def _bounded_text(safe: str) -> str:
    if len(safe) <= _MAX_SYSTEM_DIAGNOSTIC_CHARS:
        return safe
    return safe[: _MAX_SYSTEM_DIAGNOSTIC_CHARS - 3] + "..."


def _redact(value: str) -> str:
    safe = value
    for variable in _CREDENTIAL_ENV_VARS:
        secret = os.environ.get(variable)
        if secret:
            safe = safe.replace(secret, "[REDACTED]")
    return safe
