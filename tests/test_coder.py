import json
import os
from pathlib import Path

import pytest

from qworker.coder import CoderContract, ForegroundCoder, run_foreground_coder
from qworker.events import ResultEvent
from qworker.model_policy import AvailableModel
from qworker.qoder_sdk import build_coder_options, build_configured_coder_options
from tests.fakes import FakeQoderTransport


def test_coder_options_use_isolated_default_tool_policy(tmp_path: Path) -> None:
    options = build_coder_options(tmp_path)

    assert options.tools == {"type": "preset", "preset": "qodercli"}
    assert options.permission_mode == "acceptEdits"
    assert options.allow_dangerously_skip_permissions is False
    assert options.setting_sources == []
    assert options.disallowed_tools == []


def test_configured_coder_can_only_narrow_default_policy(tmp_path: Path) -> None:
    user_config = tmp_path / "config.toml"
    user_config.write_text(
        "[auth]\n"
        "reuse_qodercli = true\n"
        "[policy]\n"
        'coder_permission_mode = "dontAsk"\n'
        'coder_denied_tools = ["Bash", "Write"]\n',
        encoding="utf-8",
    )

    options = build_configured_coder_options(
        tmp_path,
        user_path=user_config,
        environ={},
    )

    assert options.permission_mode == "dontAsk"
    assert options.disallowed_tools == ["Bash", "Write"]
    assert options.allow_dangerously_skip_permissions is False
    assert options.setting_sources == []


async def test_fake_coder_reports_changed_files_and_validation(
    tmp_path: Path,
) -> None:
    report = json.dumps(
        {
            "outcome": "completed",
            "summary": "implemented the requested change",
            "files": ["src/example.py", "tests/test_example.py"],
            "validation": ["pytest tests/test_example.py -q: passed"],
            "risks": [],
        }
    )
    transport = FakeQoderTransport(
        models=(AvailableModel(value="Qwen3.8-Max", enabled=True),),
        events=(
            ResultEvent(
                session_id="coder-session-1",
                is_error=False,
                result=report,
                model_usage=("Qwen3.8-Max",),
            ),
        ),
    )
    contract = CoderContract(
        objective="Implement example behavior",
        cwd=tmp_path,
        context=("The current tests define the public API.",),
        constraints=("Preserve unrelated caller changes.",),
        acceptance_criteria=("Focused tests pass.",),
    )

    result = await ForegroundCoder(lambda _: transport).run(contract)

    assert result.outcome == "completed"
    assert result.files == ("src/example.py", "tests/test_example.py")
    assert result.validation == ("pytest tests/test_example.py -q: passed",)
    assert result.session_id == "coder-session-1"
    assert result.actual_models == ("Qwen3.8-Max",)
    assert transport.disconnected is True

    prompt = transport.sent_prompts[0]
    assert tuple(section.splitlines()[0] for section in prompt.split("\n\n")) == (
        "ROLE",
        "OBJECTIVE",
        "WORKSPACE",
        "CONTEXT",
        "CONSTRAINTS",
        "ACCEPTANCE CRITERIA",
        "REPORT CONTRACT",
    )
    assert "shared-workspace coder" in prompt
    assert "files" in prompt
    assert "validation" in prompt
    assert "Do not commit, push, publish, deploy" in prompt


@pytest.mark.real_qoder
async def test_real_coder_edits_only_a_disposable_workspace(tmp_path: Path) -> None:
    if os.environ.get("QWORKER_RUN_REAL_CODER") != "1":
        pytest.skip("set QWORKER_RUN_REAL_CODER=1 to spend Qoder credits")
    target = tmp_path / "message.txt"
    target.write_text("before\n", encoding="utf-8")

    result = await run_foreground_coder(
        CoderContract(
            objective="Change message.txt to contain exactly: after",
            cwd=tmp_path,
            constraints=("Modify no other files.",),
            acceptance_criteria=("message.txt contains exactly one line: after",),
        )
    )

    assert result.outcome == "completed"
    assert target.read_text(encoding="utf-8") == "after\n"
    assert result.files == ("message.txt",)
