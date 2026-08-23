# Qoder Agent SDK feasibility audit

Date: 2026-08-24
Verdict: feasible with explicit limitations and recovery semantics

## Probe environment

- Python: 3.12
- `qoder-agent-sdk`: 1.0.13
- SDK-bundled QoderCLI: 1.1.23
- External QoderCLI observed: 1.1.28
- Model used by the successful auditor: `Qwen3.8-Max`
- Authentication used successfully: `access_token_from_env()` with
  `QODER_PERSONAL_ACCESS_TOKEN`

The successful read-only SDK session delegated a focused feasibility check to
one direct Qoder subagent, streamed task telemetry, inspected the installed SDK
and official documentation, and returned a structured architecture audit.

## Confirmed

- One persistent `QoderSDKClient` owns one stdio QoderCLI subprocess.
- `query()` accepts `now`, `next`, and `later` priorities plus stable message
  UUIDs.
- `cancel_async_message()` and global `interrupt()` are public APIs.
- Python exposes task-started, task-progress, and task-notification messages.
- Qoder persists session and direct-subagent transcripts for later inspection.
- Conversation history can be resumed in a newly spawned process by session ID.
- Model discovery, `set_model()`, and reported model usage are available.
- Permission, elicitation, and MCP OAuth callbacks can be bridged while the
  supervisor remains alive.

## Corrections forced by the audit

1. Recovery is respawn-with-history, not reattachment. In-flight tools, queued
   messages, and callback futures can be lost with the process.
2. Python SDK 1.0.13 exposes no public `backgroundTasks` or `stopTask` method.
3. A selected nested agent cannot be directly steered or stopped; interruption
   applies to the top-level active turn.
4. Qoder subagents cannot recursively spawn further subagents.
5. Task telemetry is useful but not authoritative enough for a race-free strict
   completion gate. In the real run, the main result arrived without a promoted
   terminal task-notification event. V1 must settle briefly, then expose an
   unknown nested state instead of hanging forever.
6. A tool allowlist alone is insufficient evidence of nested read-only safety.
   V1 needs CLI restrictions plus a fail-closed permission callback that checks
   nested `agent_id` calls.
7. Permission and elicitation requests are live process state. Persisting their
   display record does not make the callback durable.
8. Model fallback must be explicit policy. The SDK does not provide a reliable
   automatic-fallback report.

## Authentication finding

`qodercli_auth()` timed out during SDK initialization in this environment even
though interactive `qodercli status` reported an active login. A manual SDK-mode
handshake revealed an `auth_required` result that SDK 1.0.13 did not surface
before its initialize timeout. `access_token_from_env()` succeeded immediately.

Production preflight must prefer unattended token or service-account auth and
turn local-login reuse timeouts into an actionable diagnostic. Credential values
must never enter logs, events, the database, or process arguments.

## Probe telemetry observed

- successful SDK initialization and server-capability response;
- `Agent`, `Read`, `Glob`, `Grep`, and `WebFetch` tool calls;
- one `task_started` event for a `local_agent`;
- repeated `task_progress` events;
- raw `background_tasks_changed` and `task_updated` system events;
- a successful `ResultMessage`;
- one persisted direct-subagent ID and readable transcript messages;
- no write, edit, notebook, or shell tool invocation.

The absence of a write-capable invocation demonstrates this probe's behavior,
not a universal inheritance guarantee. The normative enforcement requirements
are in the design specification.

## Reproduction

The probe is preserved at `spikes/qoder_audit_probe.py`. It makes a real Qoder
request and may consume credits:

```bash
uv sync
uv run python spikes/qoder_audit_probe.py
```

## Official references

- <https://docs.qoder.com/cli/sdk/overview>
- <https://docs.qoder.com/cli/sdk/how-it-works>
- <https://docs.qoder.com/cli/sdk/input-modes>
- <https://docs.qoder.com/cli/sdk/references-python>
- <https://docs.qoder.com/cli/subagent>
