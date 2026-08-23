import pytest

from qworker.auditor_policy import AuditorToolPolicy


@pytest.mark.parametrize("tool", ["Read", "Glob", "Grep", "WebFetch", "WebSearch"])
def test_nested_auditor_can_use_read_only_tools(tool: str) -> None:
    assert AuditorToolPolicy().decide(tool, agent_id="nested-1").allowed is True


@pytest.mark.parametrize("tool", ["Write", "Edit", "Bash", "NotebookEdit", "Unknown"])
def test_auditor_denies_mutating_and_unknown_tools(tool: str) -> None:
    decision = AuditorToolPolicy().decide(tool, agent_id="nested-1")

    assert decision.allowed is False
    assert decision.reason == "auditor_tool_denied"


def test_nested_auditor_cannot_spawn_another_agent() -> None:
    assert AuditorToolPolicy().decide("Agent", agent_id="nested-1").allowed is False


def test_top_level_auditor_can_spawn_one_agent() -> None:
    assert AuditorToolPolicy().decide("Agent", agent_id=None).allowed is True
