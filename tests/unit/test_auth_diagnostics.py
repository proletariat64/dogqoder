from pathlib import Path

import pytest
from qoder_agent_sdk import CLINotFoundError

from qworker.qoder_sdk import (
    AdapterDiagnostic,
    SDKOperation,
    classify_sdk_error,
    create_default_transport,
)


def test_missing_token_raises_actionable_secret_free_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QODER_PERSONAL_ACCESS_TOKEN", raising=False)

    with pytest.raises(AdapterDiagnostic) as caught:
        create_default_transport(Path.cwd())

    diagnostic = caught.value
    assert diagnostic.code == "auth_required"
    assert "QODER_PERSONAL_ACCESS_TOKEN" in diagnostic.message
    assert "token=" not in diagnostic.message


@pytest.mark.parametrize(
    ("error", "operation", "expected_code"),
    [
        (TimeoutError("initialize timed out"), "initialize", "initialize_timeout"),
        (
            RuntimeError("Control request timeout: initialize"),
            "initialize",
            "initialize_timeout",
        ),
        (CLINotFoundError("missing runtime"), "initialize", "runtime_not_found"),
        (FileNotFoundError("missing runtime"), "initialize", "runtime_not_found"),
        (TimeoutError("model timeout"), "model_discovery", "sdk_protocol_error"),
        (CLINotFoundError("late failure"), "query", "sdk_protocol_error"),
        (FileNotFoundError("late failure"), "disconnect", "sdk_protocol_error"),
        (RuntimeError("malformed SDK response"), "stream", "sdk_protocol_error"),
    ],
)
def test_sdk_errors_are_classified(
    error: Exception,
    operation: SDKOperation,
    expected_code: str,
) -> None:
    assert classify_sdk_error(error, operation=operation).code == expected_code


def test_error_redaction_removes_known_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", "secret-value")

    diagnostic = classify_sdk_error(
        RuntimeError("secret-value failed"),
        operation="query",
    )

    assert "secret-value" not in diagnostic.message
    assert diagnostic.message == "[REDACTED] failed"
