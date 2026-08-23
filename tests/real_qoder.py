"""Shared credential gate for opt-in real Qoder tests."""

import os
from collections.abc import Awaitable, Callable

import pytest

from qworker.domain import AuditContract, AuditResult

type AuditRunner = Callable[[AuditContract], Awaitable[AuditResult]]
type AuditRunnerFactory = Callable[[], AuditRunner]


def require_real_qoder_credentials() -> None:
    """Skip unless a nonempty PAT is available for a real SDK process."""

    if not os.environ.get("QODER_PERSONAL_ACCESS_TOKEN"):
        pytest.skip("QODER_PERSONAL_ACCESS_TOKEN is not configured")


async def run_credential_gated_audit(
    contract: AuditContract,
    runner_factory: AuditRunnerFactory,
) -> AuditResult:
    """Skip before constructing a real runner when no nonempty PAT exists."""

    require_real_qoder_credentials()
    return await runner_factory()(contract)
