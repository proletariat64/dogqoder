from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from qoder_agent_sdk import (
    AssistantMessage,
    ModelUsage,
    PermissionResultDeny,
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


class FakeSDKClient:
    def __init__(
        self,
        models: list[dict[str, Any]],
        sdk_messages: list[object] | None = None,
    ) -> None:
        self.models = models
        self.sdk_messages = sdk_messages or []
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
    async def connect(self, prompt: None) -> None:
        del prompt
        raise RuntimeError("secret-value failed")


async def test_transport_initializes_selects_model_then_sends() -> None:
    client = FakeSDKClient(models=[{"value": "Qwen3.8-Max", "isEnabled": True}])
    transport = QoderSDKTransport(client)

    await transport.connect()
    models = await transport.available_models()
    await transport.select_model(models[0].value)
    await transport.send("audit this")

    assert client.calls == [
        "connect:none",
        "models",
        "model:Qwen3.8-Max",
        "query:audit this",
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
    options = build_auditor_options(
        tmp_path,
        auth={"type": "access_token", "access_token": "opaque"},
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
    assert options.auth == {"type": "access_token", "access_token": "opaque"}
    assert options.cli_path is None


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
    transport = QoderSDKTransport(FailingSDKClient(models=[]))

    with pytest.raises(AdapterDiagnostic) as caught:
        await transport.connect()

    assert caught.value.code == "sdk_protocol_error"
    assert caught.value.message == "[REDACTED] failed"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
