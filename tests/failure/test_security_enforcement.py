from pathlib import Path
from typing import Any

import pytest
from qoder_agent_sdk import (
    PermissionResultAllow,
    PermissionResultDeny,
    QoderAgentOptions,
    ToolPermissionContext,
)

from qworker.control import (
    ControlCallbacks,
    ElicitationDecision,
    ElicitationRequest,
    PermissionDecision,
    PermissionRequest,
)
from qworker.qoder_sdk import QoderSDKTransport, build_auditor_options


class _CallbackSDKClient:
    def __init__(self, options: QoderAgentOptions) -> None:
        self.options = options


async def _deny_permission(_request: PermissionRequest) -> PermissionDecision:
    return PermissionDecision("deny")


async def _cancel_elicitation(_request: ElicitationRequest) -> ElicitationDecision:
    return ElicitationDecision("cancel")


async def test_policy_callback_failure_denies_at_actual_sdk_boundary(
    tmp_path: Path,
) -> None:
    options = build_auditor_options(tmp_path)

    async def failing_policy(
        _tool_name: str,
        _tool_input: dict[str, Any],
        _context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        raise RuntimeError("policy-callback-secret-must-not-escape")

    options.can_use_tool = failing_policy
    transport = QoderSDKTransport(_CallbackSDKClient(options))
    transport.bind_control(
        ControlCallbacks(
            request_permission=_deny_permission,
            request_elicitation=_cancel_elicitation,
        )
    )

    assert options.can_use_tool is not None
    decision = await options.can_use_tool(
        "Read",
        {"file_path": "tool-input-secret-must-not-escape"},
        ToolPermissionContext(agent_id=None),
    )

    assert isinstance(decision, PermissionResultDeny)
    assert decision.message == "Tool permission policy failed closed."


@pytest.mark.parametrize(
    ("tool_name", "agent_id"),
    (
        ("Write", None),
        ("Edit", None),
        ("NotebookEdit", None),
        ("Bash", None),
        ("UnknownTool", None),
        ("mcp__extension__mutate", None),
        ("Agent", "nested-agent"),
    ),
)
async def test_adversarial_tools_are_denied_before_supervisor_approval(
    tmp_path: Path,
    tool_name: str,
    agent_id: str | None,
) -> None:
    options = build_auditor_options(tmp_path)
    transport = QoderSDKTransport(_CallbackSDKClient(options))
    approval_requests: list[PermissionRequest] = []

    async def record_permission(request: PermissionRequest) -> PermissionDecision:
        approval_requests.append(request)
        return PermissionDecision("allow")

    transport.bind_control(
        ControlCallbacks(
            request_permission=record_permission,
            request_elicitation=_cancel_elicitation,
        )
    )

    assert options.can_use_tool is not None
    decision = await options.can_use_tool(
        tool_name,
        {"payload": f"tool-input-sentinel-for-{tool_name}"},
        ToolPermissionContext(agent_id=agent_id),
    )

    assert isinstance(decision, PermissionResultDeny)
    assert approval_requests == []


async def test_supervisor_callback_failures_deny_without_logging_details(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sentinel = "callback-failure-secret-must-not-escape"
    options = build_auditor_options(tmp_path)
    transport = QoderSDKTransport(_CallbackSDKClient(options))

    async def failing_permission(
        _request: PermissionRequest,
    ) -> PermissionDecision:
        raise RuntimeError(sentinel)

    async def failing_elicitation(
        _request: ElicitationRequest,
    ) -> ElicitationDecision:
        raise RuntimeError(sentinel)

    transport.bind_control(
        ControlCallbacks(
            request_permission=failing_permission,
            request_elicitation=failing_elicitation,
        )
    )

    assert options.can_use_tool is not None
    permission = await options.can_use_tool(
        "Read",
        {"file_path": "safe-path"},
        ToolPermissionContext(agent_id=None),
    )
    assert options.on_elicitation is not None
    elicitation = await options.on_elicitation(
        {
            "serverName": "safe-server",
            "message": "safe prompt",
            "mode": "form",
        }
    )

    assert isinstance(permission, PermissionResultDeny)
    assert elicitation == {"action": "cancel"}
    assert sentinel not in caplog.text
