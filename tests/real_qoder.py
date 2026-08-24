"""Shared credential gate for opt-in real Qoder tests."""

import os
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from qworker.auditor import ForegroundAuditor
from qworker.domain import AuditContract, AuditResult
from qworker.qoder_sdk import create_resumed_transport
from qworker.transport import QoderTransport

type AuditRunner = Callable[[AuditContract], Awaitable[AuditResult]]
type AuditRunnerFactory = Callable[[], AuditRunner]
type ResumeTransportFactory = Callable[[Path, str], QoderTransport]


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


async def run_resumed_adapter_audit(
    contract: AuditContract,
    session_id: str,
    *,
    resume_transport_factory: ResumeTransportFactory = create_resumed_transport,
) -> AuditResult:
    """Run one fresh adapter-backed process with stored conversation history."""

    def transport_factory(cwd: Path) -> QoderTransport:
        return resume_transport_factory(cwd, session_id)

    return await ForegroundAuditor(transport_factory).run(contract)
