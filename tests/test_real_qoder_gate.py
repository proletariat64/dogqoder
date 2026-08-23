from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from qworker.domain import AuditContract, AuditResult
from tests.real_qoder import run_credential_gated_audit


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
