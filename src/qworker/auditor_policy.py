"""Read-only tool policy for foreground auditors and their nested agents."""

from dataclasses import dataclass

from qworker.audit_report import SUBMIT_AUDIT_TOOL


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    reason: str


class AuditorToolPolicy:
    """Allow only auditor-visible tools and fail closed for nested agent spawning."""

    visible_tools = ("Read", "Glob", "Grep", "WebFetch", "WebSearch", "Agent")
    denied_tools = ("Write", "Edit", "Bash", "NotebookEdit")

    def decide(self, tool_name: str, *, agent_id: str | None) -> PolicyDecision:
        if tool_name == SUBMIT_AUDIT_TOOL:
            allowed = agent_id is None
        else:
            allowed = tool_name in self.visible_tools and not (
                agent_id is not None and tool_name == "Agent"
            )
        return PolicyDecision(
            allowed,
            "auditor_tool_allowed" if allowed else "auditor_tool_denied",
        )
