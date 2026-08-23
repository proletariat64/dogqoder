from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Literal, cast

import pytest
from qoder_agent_sdk import (
    AssistantMessage,
    AuthOptions,
    ModelUsage,
    PermissionResultDeny,
    QoderAgentOptions,
    ResultMessage,
    SDKPermissionDenial,
    SDKPermissionDeniedMessage,
    SystemMessage,
    TaskNotificationMessage,
    TaskProgressMessage,
    TaskStartedMessage,
    TextBlock,
    ToolPermissionContext,
    ToolUseBlock,
    access_token,
    service_account,
)

from qworker.events import (
    AssistantEvent,
    ResultEvent,
    SystemEvent,
    TaskProgressEvent,
    TaskStartedEvent,
    TaskTerminalEvent,
)
from qworker.qoder_sdk import (
    AdapterDiagnostic,
    QoderSDKTransport,
    build_auditor_options,
)

type FailingOperation = Literal[
    "initialize",
    "model_discovery",
    "model_selection",
    "query",
    "stream",
    "disconnect",
]


class FakeSDKClient:
    def __init__(
        self,
        models: list[dict[str, Any]],
        sdk_messages: list[object] | None = None,
        options: QoderAgentOptions | None = None,
    ) -> None:
        self.models = models
        self.sdk_messages = sdk_messages or []
        self.options = options
        self.calls: list[str] = []

    async def connect(self, prompt: None) -> None:
        self.calls.append("connect:none")

    async def get_available_models(self) -> list[dict[str, Any]]:
        self.calls.append("models")
        return self.models

    async def set_model(self, model: str) -> None:
        self.calls.append(f"model:{model}")

    async def query(self, prompt: str) -> None:
        self.calls.append(f"query:{prompt}")

    async def receive_messages(self) -> AsyncIterator[object]:
        self.calls.append("messages")
        for message in self.sdk_messages:
            yield message

    async def disconnect(self) -> None:
        self.calls.append("disconnect")


class FailingSDKClient(FakeSDKClient):
    def __init__(self, error_message: str, options: QoderAgentOptions | None = None):
        super().__init__(models=[], options=options)
        self.error_message = error_message

    async def connect(self, prompt: None) -> None:
        del prompt
        raise RuntimeError(self.error_message)


class TimeoutSDKClient(FakeSDKClient):
    def __init__(self, failing_operation: FailingOperation) -> None:
        super().__init__(models=[])
        self.failing_operation = failing_operation

    async def connect(self, prompt: None) -> None:
        if self.failing_operation == "initialize":
            raise TimeoutError("timed out")
        await super().connect(prompt)

    async def get_available_models(self) -> list[dict[str, Any]]:
        if self.failing_operation == "model_discovery":
            raise TimeoutError("timed out")
        return await super().get_available_models()

    async def set_model(self, model: str) -> None:
        if self.failing_operation == "model_selection":
            raise TimeoutError("timed out")
        await super().set_model(model)

    async def query(self, prompt: str) -> None:
        if self.failing_operation == "query":
            raise TimeoutError("timed out")
        await super().query(prompt)

    async def receive_messages(self) -> AsyncIterator[object]:
        if self.failing_operation == "stream":
            raise TimeoutError("timed out")
        async for message in super().receive_messages():
            yield message

    async def disconnect(self) -> None:
        if self.failing_operation == "disconnect":
            raise TimeoutError("timed out")
        await super().disconnect()


async def test_transport_initializes_selects_model_then_sends() -> None:
    client = FakeSDKClient(models=[{"value": "Qwen3.8-Max", "isEnabled": True}])
    transport = QoderSDKTransport(client)

    await transport.connect()
    models = await transport.available_models()
    await transport.select_model(models[0].value)
    await transport.send("audit this")
    assert [event async for event in transport.messages()] == []
    await transport.disconnect()

    assert client.calls == [
        "connect:none",
        "models",
        "model:Qwen3.8-Max",
        "query:audit this",
        "messages",
        "disconnect",
    ]


async def test_nested_mutation_is_denied_at_callback_boundary(
    tmp_path: Path,
) -> None:
    options = build_auditor_options(tmp_path)
    context = ToolPermissionContext(agent_id="nested-1")

    assert options.can_use_tool is not None
    decision = await options.can_use_tool("Write", {"file_path": "x"}, context)

    assert isinstance(decision, PermissionResultDeny)


def test_auditor_options_are_layered_and_isolated(tmp_path: Path) -> None:
    auth = access_token("opaque")
    options = build_auditor_options(
        tmp_path,
        auth=auth,
    )

    assert options.tools == ["Read", "Glob", "Grep", "WebFetch", "WebSearch", "Agent"]
    assert options.allowed_tools == [
        "Read",
        "Glob",
        "Grep",
        "WebFetch",
        "WebSearch",
        "Agent",
    ]
    assert options.disallowed_tools == ["Write", "Edit", "Bash", "NotebookEdit"]
    assert options.permission_mode == "dontAsk"
    assert options.setting_sources == []
    assert options.max_turns == 24
    assert options.cwd == tmp_path
    assert options.auth is auth
    assert options.cli_path is None


def test_invalid_raw_auth_shape_is_rejected(tmp_path: Path) -> None:
    invalid_auth = cast(
        AuthOptions,
        {"type": "access_token", "access_token": "opaque"},
    )

    with pytest.raises(TypeError, match="SDK auth helper"):
        build_auditor_options(tmp_path, auth=invalid_auth)


@pytest.mark.parametrize(
    "auth",
    [
        access_token("direct-secret"),
        service_account(service_account_key="direct-secret"),
    ],
)
async def test_injected_sdk_auth_is_valid_and_its_secret_never_crosses_seam(
    tmp_path: Path,
    auth: AuthOptions,
) -> None:
    options = build_auditor_options(tmp_path, auth=auth)
    client = FakeSDKClient(
        models=[],
        options=options,
        sdk_messages=[
            SystemMessage(
                subtype="credential_status",
                data={"detail": "direct-secret leaked"},
            ),
            ResultMessage(
                subtype="error",
                duration_ms=1,
                duration_api_ms=1,
                is_error=True,
                num_turns=1,
                session_id="session-1",
                result="direct-secret result",
                errors=["direct-secret error"],
            ),
        ],
    )

    assert options.auth is auth
    events = [event async for event in QoderSDKTransport(client).messages()]
    assert "direct-secret" not in repr(events)

    failing_transport = QoderSDKTransport(
        FailingSDKClient("direct-secret failed", options=options)
    )
    with pytest.raises(AdapterDiagnostic) as caught:
        await failing_transport.connect()
    assert "direct-secret" not in caught.value.message


async def test_sdk_messages_are_normalized_bounded_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "secret-value"
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", secret)
    usage = ModelUsage(
        inputTokens=1,
        outputTokens=2,
        cacheReadInputTokens=0,
        cacheCreationInputTokens=0,
        webSearchRequests=0,
        costUSD=0.0,
        contextWindow=100,
        maxOutputTokens=50,
    )
    permission_denial = SDKPermissionDenial(
        tool_name="Write",
        tool_use_id="tool-1",
        message=f"denied {secret}",
        uuid="permission-1",
        session_id="session-1",
    )
    client = FakeSDKClient(
        models=[],
        sdk_messages=[
            AssistantMessage(
                content=[
                    TextBlock(text=f"answer {secret}"),
                    ToolUseBlock(id="tool-1", name="Read", input={}),
                ],
                model="Qwen3.8-Max",
            ),
            TaskStartedMessage(
                subtype="task_started",
                data={},
                task_id="task-1",
                description=f"inspect {secret}",
                uuid="started-1",
                session_id="session-1",
            ),
            TaskProgressMessage(
                subtype="task_progress",
                data={},
                task_id="task-1",
                description="still inspecting",
                usage={"total_tokens": 1, "tool_uses": 1, "duration_ms": 1},
                uuid="progress-1",
                session_id="session-1",
                last_tool_name="Read",
            ),
            TaskNotificationMessage(
                subtype="task_notification",
                data={},
                task_id="task-1",
                status="completed",
                output_file="",
                summary="done",
                uuid="terminal-1",
                session_id="session-1",
            ),
            SDKPermissionDeniedMessage(
                subtype="permission_denied",
                data={},
                tool_name="Write",
                tool_use_id="tool-1",
                message=f"denied {secret}",
                uuid="permission-1",
                session_id="session-1",
            ),
            SystemMessage(
                subtype="status",
                data={"detail": secret, "noise": "x" * 1_000},
            ),
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="session-1",
                result=f"report {secret}",
                model_usage={"Qwen3.8-Max": usage},
                permission_denials=[permission_denial],
                errors=[f"error {secret}"],
            ),
        ],
    )

    events = [event async for event in QoderSDKTransport(client).messages()]

    assert events[:5] == [
        AssistantEvent(
            text=("answer [REDACTED]",),
            tools=("Read",),
            model="Qwen3.8-Max",
        ),
        TaskStartedEvent(task_id="task-1", description="inspect [REDACTED]"),
        TaskProgressEvent(
            task_id="task-1",
            description="still inspecting",
            last_tool_name="Read",
        ),
        TaskTerminalEvent(task_id="task-1", status="completed"),
        SystemEvent(subtype="permission_denied", message="Write: denied [REDACTED]"),
    ]
    generic_system = events[5]
    assert isinstance(generic_system, SystemEvent)
    assert secret not in generic_system.message
    assert len(generic_system.message) <= 512
    assert events[6] == ResultEvent(
        session_id="session-1",
        is_error=False,
        result="report [REDACTED]",
        model_usage=("Qwen3.8-Max",),
        permission_denials=("Write: denied [REDACTED]",),
        errors=("error [REDACTED]",),
    )


async def test_sdk_operation_errors_are_classified_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", "secret-value")
    transport = QoderSDKTransport(FailingSDKClient("secret-value failed"))

    with pytest.raises(AdapterDiagnostic) as caught:
        await transport.connect()

    assert caught.value.code == "sdk_protocol_error"
    assert caught.value.message == "[REDACTED] failed"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


async def test_every_system_event_field_is_bounded_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", "secret-value")
    client = FakeSDKClient(
        models=[],
        sdk_messages=[
            SDKPermissionDeniedMessage(
                subtype="permission_denied",
                data={},
                tool_name="Write" * 200,
                tool_use_id="tool-1",
                message="secret-value" + "x" * 1_000,
                uuid="permission-1",
                session_id="session-1",
            ),
            SystemMessage(
                subtype="secret-value" + "s" * 1_000,
                data={"detail": "secret-value" + "d" * 1_000},
            ),
        ],
    )

    events = [event async for event in QoderSDKTransport(client).messages()]

    assert all(isinstance(event, SystemEvent) for event in events)
    for event in events:
        assert isinstance(event, SystemEvent)
        assert len(event.subtype) <= 512
        assert len(event.message) <= 512
        assert "secret-value" not in event.subtype
        assert "secret-value" not in event.message


@pytest.mark.parametrize(
    ("operation", "expected_code"),
    [
        ("initialize", "initialize_timeout"),
        ("model_discovery", "sdk_protocol_error"),
        ("model_selection", "sdk_protocol_error"),
        ("query", "sdk_protocol_error"),
        ("stream", "sdk_protocol_error"),
        ("disconnect", "sdk_protocol_error"),
    ],
)
async def test_transport_classifies_timeouts_by_operation(
    operation: FailingOperation,
    expected_code: str,
) -> None:
    transport = QoderSDKTransport(TimeoutSDKClient(operation))

    with pytest.raises(AdapterDiagnostic) as caught:
        if operation == "initialize":
            await transport.connect()
        elif operation == "model_discovery":
            await transport.available_models()
        elif operation == "model_selection":
            await transport.select_model("Qwen3.8-Max")
        elif operation == "query":
            await transport.send("audit")
        elif operation == "stream":
            _ = [event async for event in transport.messages()]
        else:
            await transport.disconnect()

    assert caught.value.code == expected_code
