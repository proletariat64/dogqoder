from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from qoder_agent_sdk import AssistantMessage, ModelUsage, ResultMessage, TextBlock

from qworker.auditor import ForegroundAuditor
from qworker.domain import AuditContract
from qworker.events import ResultEvent, TaskStartedEvent, TaskTerminalEvent
from qworker.model_policy import AvailableModel
from qworker.qoder_sdk import AdapterDiagnostic, QoderSDKTransport
from tests.fakes import FakeQoderTransport


async def test_foreground_auditor_returns_structured_completion(
    tmp_path: Path,
) -> None:
    transport = FakeQoderTransport.successful_audit(model="Qwen3.8-Max")
    result = await ForegroundAuditor(lambda _: transport).run(
        AuditContract(objective="audit the design", cwd=tmp_path)
    )

    assert result.outcome == "completed"
    assert result.resolved_model == "Qwen3.8-Max"
    assert result.nested_state == "settled"
    assert transport.calls[:4] == [
        "connect",
        "models",
        "model:Qwen3.8-Max",
        "send",
    ]
    assert transport.disconnected is True


async def test_completed_run_distinguishes_requested_resolved_and_actual_models(
    tmp_path: Path,
) -> None:
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

    class ModelReportingSDKClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def connect(self, prompt: None) -> None:
            self.calls.append("connect")

        async def get_available_models(self) -> list[dict[str, object]]:
            self.calls.append("models")
            return [
                {
                    "value": "qmodel_38max",
                    "displayName": "Qwen3.8-Max",
                    "isEnabled": True,
                }
            ]

        async def set_model(self, model: str | None = None) -> None:
            self.calls.append(f"model:{model}")

        async def query(self, prompt: str) -> None:
            del prompt
            self.calls.append("send")

        async def receive_messages(self) -> AsyncIterator[object]:
            self.calls.append("messages")
            yield AssistantMessage(
                content=[TextBlock(text="audit complete")],
                model="Qwen3.8-Max",
            )
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="session-1",
                result=(
                    '{"outcome":"completed","summary":"safe","files":[],'
                    '"validation":[],"risks":[]}'
                ),
                model_usage={"Qwen3.7-Max": usage},
            )

        async def disconnect(self) -> None:
            self.calls.append("disconnect")

    client = ModelReportingSDKClient()

    result = await ForegroundAuditor(lambda _: QoderSDKTransport(client)).run(
        AuditContract(objective="audit the design", cwd=tmp_path)
    )

    assert result.outcome == "completed"
    assert result.requested_model == "qwen-auditor"
    assert result.resolved_model == "Qwen3.8-Max"
    assert result.actual_models == ("Qwen3.8-Max", "Qwen3.7-Max")
    assert client.calls[:4] == [
        "connect",
        "models",
        "model:qmodel_38max",
        "send",
    ]


async def test_foreground_auditor_waits_for_delayed_terminal_task(
    tmp_path: Path,
) -> None:
    transport = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(
            TaskStartedEvent(task_id="task-1", description="inspect design"),
            ResultEvent(session_id="session-1", is_error=False, result="done"),
            TaskTerminalEvent(task_id="task-1", status="completed"),
        ),
        event_delays=(0.0, 0.0, 0.001),
    )

    result = await ForegroundAuditor(
        lambda _: transport,
        settlement_timeout=0.05,
    ).run(AuditContract(objective="audit the design", cwd=tmp_path))

    assert result.nested_state == "settled"


async def test_foreground_auditor_times_out_missing_terminal_task(
    tmp_path: Path,
) -> None:
    transport = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(
            TaskStartedEvent(task_id="task-1", description="inspect design"),
            ResultEvent(session_id="session-1", is_error=False, result="done"),
        ),
        hang_after_events=True,
    )

    result = await ForegroundAuditor(
        lambda _: transport,
        settlement_timeout=0.001,
    ).run(AuditContract(objective="audit the design", cwd=tmp_path))

    assert result.nested_state == "unknown"
    assert "nested_terminal_event_missing" in result.warnings


async def test_foreground_auditor_renders_stable_prompt_envelope(
    tmp_path: Path,
) -> None:
    transport = FakeQoderTransport.successful_audit(model="Qwen3.8-Max")
    contract = AuditContract(
        objective="audit the design",
        cwd=tmp_path,
        context=("design.md is normative",),
        constraints=("Report only evidenced findings.",),
        acceptance_criteria=("Identify unsafe writes.",),
    )

    await ForegroundAuditor(lambda _: transport).run(contract)

    prompt = transport.sent_prompts[0]
    headings = (
        "ROLE",
        "OBJECTIVE",
        "WORKSPACE",
        "CONTEXT",
        "CONSTRAINTS",
        "ACCEPTANCE CRITERIA",
        "REPORT CONTRACT",
    )
    assert (
        tuple(section.splitlines()[0] for section in prompt.split("\n\n")) == headings
    )
    assert "audit the design" in prompt
    assert str(tmp_path) in prompt
    assert "design.md is normative" in prompt
    assert "Report only evidenced findings." in prompt
    assert "Identify unsafe writes." in prompt
    reasoning_prescriptions = (
        "chain of thought",
        "step-by-step reasoning",
        "show your reasoning",
        "reasoning process",
        "think aloud",
    )
    assert not any(
        prescription in prompt.casefold() for prescription in reasoning_prescriptions
    )


async def test_foreground_auditor_returns_redacted_transport_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "credential-value-that-must-not-escape"
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", secret)

    class FailingTransport(FakeQoderTransport):
        async def connect(self) -> None:
            raise AdapterDiagnostic("auth_required", f"auth failed for {secret}")

    transport = FailingTransport(models=(), events=())

    result = await ForegroundAuditor(lambda _: transport).run(
        AuditContract(objective="audit the design", cwd=tmp_path)
    )

    assert result.outcome == "failed"
    assert result.errors == ("auth_required",)
    assert secret not in result.summary
    assert transport.disconnected is True
