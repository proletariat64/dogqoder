import asyncio
import json
import logging
from dataclasses import asdict, is_dataclass
from pathlib import Path

import qoder_agent_sdk
from qoder_agent_sdk import (
    AssistantMessage,
    QoderAgentOptions,
    QoderSDKClient,
    ResultMessage,
    SystemMessage,
    TaskNotificationMessage,
    TaskProgressMessage,
    TaskStartedMessage,
    TextBlock,
    ToolUseBlock,
    access_token_from_env,
    get_subagent_messages,
    list_subagents,
)


logging.basicConfig(level=logging.INFO)
REPO_ROOT = Path(__file__).resolve().parents[1]


PROMPT = rf"""
Act as an independent, skeptical architecture auditor. Do not assume the proposed
design is sound. This is a READ-ONLY audit: do not write or edit any file.

First delegate one focused feasibility check to a Qoder internal subagent using
the Agent tool. Then independently inspect the installed SDK source and official
Qoder SDK documentation where useful, and synthesize both views.

Verified environment facts from the original audit:
- external qodercli 1.1.28 was present; the SDK used its bundled runtime.
- qoder-agent-sdk Python 1.0.13 is installed at
  {qoder_agent_sdk.__file__}
- Qwen3.8-Max is in this account's live `qodercli --list-models` output.
- Official docs entry: https://docs.qoder.com/cli/sdk/overview

Proposed design to audit:
1. Codex invokes a `$qoder-worker` skill. A local `qworker` CLI talks to a
   persistent supervisor. The supervisor owns one QoderSDKClient process/session
   per top-level worker. ACP is not used initially.
2. Top-level roles are `coder` and `auditor`. Qoder may freely spawn and harness
   its own nested subagents. Codex observes nested health/task telemetry but does
   not guide or restrict nested agents.
3. Workers use the caller's shared workspace by default; no automatic worktree
   and no exclusive write lock. Codex coordinates concurrent writers.
4. Coder can write and must run relevant lint/typecheck/tests. Auditor is enforced
   read-only by a tool allowlist (Read, Glob, Grep, web-read tools, Agent) plus a
   write/edit/shell denylist.
5. Model aliases resolve against the current Qoder model list, preferring
   Qwen3.8-Max for auditor. Resolved model and fallback are reported.
6. Desired commands: spawn, list, status, watch, steer, respond, stop, result.
   `steer` maps to SDK message priorities now/next/later; queued messages have UUIDs
   and can be cancelled. `stop` should preserve shared-workspace edits.
7. Desired persisted lifecycle:
   starting -> running <-> requires_action -> completed, plus failed/cancelled/
   lost/stalled. Health is tracked separately. Events persist across CLI clients.
   Supervisor restart should resume a Qoder session if possible. Stalled workers
   are not killed automatically.
8. Completion means the main ResultMessage arrived and no nested task is still
   active. Nested task_started/progress/task_notification and session/subagent
   inspection are used for visibility.

Audit against what is ACTUALLY available in Python SDK 1.0.13 and qodercli 1.1.28.
Pay special attention to:
- whether a durable supervisor and one-process-per-worker are realistic;
- whether reconnect/resume after supervisor or CLI-process death is actually
  supported, and what state can/cannot be reconstructed;
- exact visibility into nested agents and whether task events are sufficient;
- whether a specific nested agent can be steered or stopped;
- whether Python exposes backgroundTasks/stopTask or only task events;
- whether the proposed read-only policy is enforceable, including nested agents;
- permission/elicitation handling needed for `requires_action` and `respond`;
- race conditions in defining completion and preserving event order;
- model discovery/resolution and fallback behavior.

Return a concise report with these exact sections:
VERDICT
CONFIRMED
PARTIAL_OR_UNPROVEN
INVALID
REQUIRED_DESIGN_CHANGES
MINIMUM_REALISTIC_V1

For each material claim, distinguish direct SDK/API evidence from inference.
"""


def compact(value):
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        keep = {
            key: compact(item)
            for key, item in value.items()
            if key
            in {
                "subtype",
                "task_id",
                "description",
                "status",
                "summary",
                "session_id",
                "tool_use_id",
                "task_type",
                "last_tool_name",
                "model",
                "name",
            }
        }
        return keep or {"keys": sorted(value.keys())}
    if isinstance(value, list):
        return [compact(item) for item in value]
    return value


async def main():
    result_text = []
    result_session_id = None
    task_events = []
    options = QoderAgentOptions(
        auth=access_token_from_env(),
        cwd=str(REPO_ROOT),
        model="Qwen3.8-Max",
        setting_sources=[],
        tools=["Read", "Glob", "Grep", "WebFetch", "WebSearch", "Agent"],
        allowed_tools=["Read", "Glob", "Grep", "WebFetch", "WebSearch", "Agent"],
        disallowed_tools=["Write", "Edit", "Bash", "NotebookEdit"],
        permission_mode="dontAsk",
        max_turns=24,
        control_request_timeout_ms=30000,
        load_timeout_ms=30000,
        max_buffer_size=16 * 1024 * 1024,
        stderr=lambda line: print("QODER_STDERR", line, flush=True),
    )

    client = QoderSDKClient(options=options)
    try:
        await client.connect(PROMPT)
        server = await client.get_server_info()
        print("SERVER_INFO", json.dumps(compact(server), ensure_ascii=False), flush=True)

        async for message in client.receive_response():
            if isinstance(message, TaskStartedMessage):
                event = {
                    "kind": "task_started",
                    "task_id": message.task_id,
                    "description": message.description,
                    "task_type": message.task_type,
                    "session_id": message.session_id,
                }
                task_events.append(event)
                print("TASK_EVENT", json.dumps(event, ensure_ascii=False), flush=True)
            elif isinstance(message, TaskProgressMessage):
                event = {
                    "kind": "task_progress",
                    "task_id": message.task_id,
                    "description": message.description,
                    "last_tool_name": message.last_tool_name,
                    "usage": message.usage,
                }
                task_events.append(event)
                print("TASK_EVENT", json.dumps(event, ensure_ascii=False), flush=True)
            elif isinstance(message, TaskNotificationMessage):
                event = {
                    "kind": "task_notification",
                    "task_id": message.task_id,
                    "status": message.status,
                    "summary": message.summary,
                    "usage": message.usage,
                }
                task_events.append(event)
                print("TASK_EVENT", json.dumps(event, ensure_ascii=False), flush=True)
            elif isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, ToolUseBlock):
                        print(
                            "TOOL_USE",
                            json.dumps(
                                {"name": block.name, "id": block.id},
                                ensure_ascii=False,
                            ),
                            flush=True,
                        )
                    elif isinstance(block, TextBlock):
                        result_text.append(block.text)
                        print("AUDITOR_TEXT", block.text, flush=True)
            elif isinstance(message, ResultMessage):
                result_session_id = message.session_id
                if message.result:
                    result_text.append(message.result)
                print(
                    "RESULT_META",
                    json.dumps(
                        {
                            "session_id": message.session_id,
                            "is_error": message.is_error,
                            "num_turns": message.num_turns,
                            "stop_reason": message.stop_reason,
                            "terminal_reason": message.terminal_reason,
                            "errors": message.errors,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            elif isinstance(message, SystemMessage):
                print(
                    "SYSTEM_EVENT",
                    json.dumps(compact(message), ensure_ascii=False),
                    flush=True,
                )

        if result_session_id:
            agent_ids = list_subagents(result_session_id, directory=str(REPO_ROOT))
            print("SUBAGENT_IDS", json.dumps(agent_ids), flush=True)
            for agent_id in agent_ids:
                messages = get_subagent_messages(
                    result_session_id,
                    agent_id,
                    directory=str(REPO_ROOT),
                    limit=5,
                )
                print(
                    "SUBAGENT_MESSAGES",
                    json.dumps(
                        {"agent_id": agent_id, "count": len(messages)},
                        ensure_ascii=False,
                    ),
                    flush=True,
                )

        print(
            "PROBE_SUMMARY",
            json.dumps(
                {
                    "session_id": result_session_id,
                    "task_event_count": len(task_events),
                    "report_characters": sum(len(text) for text in result_text),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
