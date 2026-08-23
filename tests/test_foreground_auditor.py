from pathlib import Path

import pytest
from fakes import FakeQoderTransport

from qworker.auditor import ForegroundAuditor
from qworker.domain import AuditContract
from qworker.events import ResultEvent, TaskStartedEvent, TaskTerminalEvent
from qworker.model_policy import AvailableModel
from qworker.qoder_sdk import AdapterDiagnostic


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
    assert [prompt.index(heading) for heading in headings] == sorted(
        prompt.index(heading) for heading in headings
    )
    assert "audit the design" in prompt
    assert str(tmp_path) in prompt
    assert "design.md is normative" in prompt
    assert "Report only evidenced findings." in prompt
    assert "Identify unsafe writes." in prompt


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
