from pathlib import Path

import pytest
from qoder_agent_sdk import CLINotFoundError

from qworker.qoder_sdk import (
    AdapterDiagnostic,
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
    ("error", "expected_code"),
    [
        (TimeoutError("initialize timed out"), "initialize_timeout"),
        (RuntimeError("Control request timeout: initialize"), "initialize_timeout"),
        (CLINotFoundError("missing runtime"), "runtime_not_found"),
        (RuntimeError("malformed SDK response"), "sdk_protocol_error"),
    ],
)
def test_sdk_errors_are_classified(
    error: Exception,
    expected_code: str,
) -> None:
    assert classify_sdk_error(error).code == expected_code


def test_error_redaction_removes_known_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", "secret-value")

    diagnostic = classify_sdk_error(RuntimeError("secret-value failed"))

    assert "secret-value" not in diagnostic.message
    assert diagnostic.message == "[REDACTED] failed"
