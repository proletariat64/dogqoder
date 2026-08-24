from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest
from qoder_agent_sdk import ResultMessage

from qworker.domain import AuditContract, AuditResult
from qworker.qoder_sdk import QoderSDKTransport
from tests.contract.test_qoder_sdk_transport import FakeSDKClient
from tests.fakes import SUCCESSFUL_AUDIT_REPORT
from tests.real_qoder import run_credential_gated_audit, run_resumed_adapter_audit


@pytest.mark.parametrize("credential", [None, ""])
async def test_missing_or_empty_credential_skips_before_runner_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    credential: str | None,
) -> None:
    if credential is None:
        monkeypatch.delenv("QODER_PERSONAL_ACCESS_TOKEN", raising=False)
    else:
        monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", credential)

    runner_constructed = False

    def construct_runner() -> Callable[[AuditContract], Awaitable[AuditResult]]:
        nonlocal runner_constructed
        runner_constructed = True
        raise AssertionError("SDK runner must not be constructed")

    with pytest.raises(pytest.skip.Exception, match="is not configured"):
        await run_credential_gated_audit(
            AuditContract(objective="audit", cwd=tmp_path),
            construct_runner,
        )

    assert runner_constructed is False


async def test_resumed_adapter_audit_passes_session_and_consumes_result(
    tmp_path: Path,
) -> None:
    client = FakeSDKClient(
        models=[{"value": "Qwen3.8-Max", "isEnabled": True}],
        sdk_messages=[
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="session-resumed",
                result=SUCCESSFUL_AUDIT_REPORT,
            )
        ],
    )
    constructions: list[tuple[Path, str]] = []

    def resume_factory(cwd: Path, session_id: str) -> QoderSDKTransport:
        constructions.append((cwd, session_id))
        return QoderSDKTransport(client)

    result = await run_resumed_adapter_audit(
        AuditContract(objective="recover", cwd=tmp_path),
        "session-stored",
        resume_transport_factory=resume_factory,
    )

    assert constructions == [(tmp_path, "session-stored")]
    assert result.outcome == "completed"
    assert result.session_id == "session-resumed"
    assert "messages" in client.calls
