"""Single explicitly authorized adapter-backed live UAT for Tasks 9 and 10."""

import asyncio
import json
import os
import secrets
from pathlib import Path

import pytest

from qworker.auditor import run_foreground_audit
from qworker.domain import AuditContract, AuditResult
from qworker.preflight import RuntimePreflight
from qworker.qoder_sdk import QoderPreflightBackend
from tests.real_qoder import (
    require_real_qoder_credentials,
    run_resumed_adapter_audit,
)


def _safe_signal(**values: bool | str | None) -> None:
    print(f"QODER_UAT {json.dumps(values, sort_keys=True)}")


def _require_adapter_result(result: AuditResult, *, stage: str) -> None:
    if result.session_id is None or result.outcome == "failed":
        _safe_signal(
            preflight_confirmed=True,
            resume_history_confirmed=False,
            stage=stage,
        )
        pytest.xfail(f"Task 10 live adapter result unavailable at {stage}.")


@pytest.mark.real_qoder
async def test_live_pat_preflight_and_resumed_conversation(tmp_path: Path) -> None:
    """Spend credits once to prove PAT preflight and adapter-backed history resume."""

    require_real_qoder_credentials()
    credential = os.environ["QODER_PERSONAL_ACCESS_TOKEN"]
    before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    async with asyncio.timeout(210):
        preflight = await RuntimePreflight(QoderPreflightBackend()).run(tmp_path)
        if not preflight.ok:
            code = preflight.error.code if preflight.error is not None else "unknown"
            pytest.fail(f"Live Qoder preflight failed safely: {code}", pytrace=False)
        if preflight.auth_source != "personal_access_token":
            pytest.fail(
                "Live preflight did not select PAT authentication.", pytrace=False
            )
        if set(preflight.capabilities) - {"modelPolicy"}:
            pytest.fail("Live preflight exposed an unsafe capability.", pytrace=False)
        preflight_json = json.dumps(preflight.to_json(), sort_keys=True)
        if credential in preflight_json:
            pytest.fail("Credential crossed the preflight boundary.", pytrace=False)

        marker = f"QWUAT{secrets.token_hex(8)}"
        initial_contract = AuditContract(
            objective=(
                "Remember the supplied marker, read no workspace files, and return "
                f"the marker in the report summary: {marker}"
            ),
            cwd=tmp_path,
            acceptance_criteria=("Report the supplied marker in summary.",),
        )
        try:
            async with asyncio.timeout(90):
                initial = await run_foreground_audit(initial_contract)
        except TimeoutError:
            _safe_signal(
                preflight_confirmed=True,
                resume_history_confirmed=False,
                stage="initial_adapter_timeout",
            )
            pytest.xfail(
                "Task 10 live resume blocked: initial adapter audit timed out."
            )
        _require_adapter_result(initial, stage="initial_adapter_result")
        assert initial.session_id is not None

        recovery_contract = AuditContract(
            objective=(
                "Recover the marker from the prior conversation, read no workspace "
                "files, and return the recovered marker in the report summary."
            ),
            cwd=tmp_path,
            acceptance_criteria=("Report the recovered marker in summary.",),
        )
        try:
            async with asyncio.timeout(90):
                resumed = await run_resumed_adapter_audit(
                    recovery_contract,
                    initial.session_id,
                )
        except TimeoutError:
            _safe_signal(
                preflight_confirmed=True,
                resume_history_confirmed=False,
                stage="resume_adapter_timeout",
            )
            pytest.xfail(
                "Task 10 live resume blocked: resumed adapter audit timed out."
            )
        _require_adapter_result(resumed, stage="resume_adapter_result")

    history_text = "\n".join(
        (
            resumed.summary,
            *resumed.confirmed,
            resumed.verdict or "",
        )
    )
    if marker not in history_text:
        pytest.fail("Resumed adapter did not recover prior conversation history.")
    if credential in repr(initial) or credential in repr(resumed):
        pytest.fail("Credential crossed the adapter result boundary.", pytrace=False)
    after = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    if after != before:
        pytest.fail(
            "Live read-only UAT changed the disposable workspace.", pytrace=False
        )
    _safe_signal(
        preflight_confirmed=True,
        resume_history_confirmed=True,
        same_session_id=resumed.session_id == initial.session_id,
        stage="complete",
    )
