"""Credit-consuming, opt-in checks against the real bundled Qoder runtime."""

import os
import shutil
from pathlib import Path

import pytest
from qoder_agent_sdk import (
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)

from qworker.auditor import run_foreground_audit
from qworker.domain import AuditContract
from qworker.qoder_sdk import build_auditor_options
from tests.real_qoder import (
    require_real_qoder_credentials,
    run_credential_gated_audit,
)


@pytest.mark.real_qoder
async def test_real_qwen_auditor_is_read_only(tmp_path: Path) -> None:
    """Run only by explicit marker selection; consumes Qoder credits."""

    require_real_qoder_credentials()
    fixture = Path(__file__).with_name("fixtures") / "audit_workspace.txt"
    shutil.copy2(fixture, tmp_path / "audit-target.txt")
    before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    options = build_auditor_options(tmp_path)
    assert options.cwd == tmp_path
    assert options.cli_path is None
    assert options.permission_mode == "dontAsk"
    assert options.can_use_tool is not None
    nested_context = ToolPermissionContext(agent_id="nested-real-test")
    assert isinstance(
        await options.can_use_tool(
            "Read", {"file_path": "audit-target.txt"}, nested_context
        ),
        PermissionResultAllow,
    )
    assert isinstance(
        await options.can_use_tool(
            "Write", {"file_path": "blocked.txt"}, nested_context
        ),
        PermissionResultDeny,
    )
    assert isinstance(
        await options.can_use_tool("Agent", {"prompt": "recurse"}, nested_context),
        PermissionResultDeny,
    )

    result = await run_credential_gated_audit(
        AuditContract(
            objective="Read the workspace and report its top-level files.",
            cwd=tmp_path,
        ),
        lambda: run_foreground_audit,
    )

    after = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    credential = os.environ["QODER_PERSONAL_ACCESS_TOKEN"]
    if credential in repr(result):
        pytest.fail("Qoder credential crossed the AuditResult boundary", pytrace=False)
    assert result.outcome == "completed", result
    assert result.resolved_model == "Qwen3.8-Max"
    assert result.verdict is not None
    assert after == before
