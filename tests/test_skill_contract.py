from pathlib import Path

SKILL_PATH = Path("skills/qoder-worker/SKILL.md")
CONTRACT_PATH = Path("skills/qoder-worker/references/contract.md")


def test_role_inference_prefers_explicit_intent_and_defaults_read_only() -> None:
    skill = SKILL_PATH.read_text(encoding="utf-8")
    rules = (
        "1. An explicit role from the user wins.",
        (
            "2. Review, audit, verify, verification, cross-check, challenge, or "
            "second-opinion intent selects the read-only `auditor`."
        ),
        "3. Implementation, fix, refactor, test, or code intent selects `coder`.",
        "4. Ambiguous intent defaults to the read-only `auditor`.",
    )

    positions = [skill.index(rule) for rule in rules]

    assert positions == sorted(positions)


def test_proactive_activation_allows_audits_but_gates_coding() -> None:
    skill = SKILL_PATH.read_text(encoding="utf-8")

    assert (
        "Proactively start an `auditor` when an independent perspective materially "
        "improves review, risk analysis, or design validation."
    ) in skill
    assert (
        "Proactively start a `coder` only after project configuration explicitly "
        "enables proactive Qoder coding."
    ) in skill


def test_fallback_changes_after_workspace_mutation_is_possible() -> None:
    skill = SKILL_PATH.read_text(encoding="utf-8")

    assert (
        "Before possible workspace mutation, a failed worker creation may fall back "
        "to Codex only when active policy permits."
    ) in skill
    assert (
        "After a coder may have modified the workspace, report its partial state "
        "before Codex continues or another writer starts; never silently replace "
        "the writer."
    ) in skill


def test_workflow_uses_json_only_and_discloses_exact_cli_contract() -> None:
    skill = SKILL_PATH.read_text(encoding="utf-8")
    contract = CONTRACT_PATH.read_text(encoding="utf-8")
    frontmatter = skill.split("---", maxsplit=2)[1]

    assert "name: qoder-worker" in frontmatter
    assert "description:" in frontmatter
    assert "disable-model-invocation" not in frontmatter
    assert (
        "For exact commands, response fields, lifecycle states, and recovery limits, "
        "read [the CLI contract](references/contract.md) before the first `qworker` "
        "call."
    ) in skill
    assert (
        "Use files or stdin for prompts, specifications, steering messages, and "
        "approval responses; never place large content in command arguments."
    ) in skill
    for command in ("spawn", "status", "watch", "result", "stop", "resume"):
        assert f"qworker {command}" in contract
    assert "Treat stdout as JSON or JSON Lines only" in contract
