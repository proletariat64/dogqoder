# Fresh-session implementation handoff

Copy the prompt below into a new Codex session started from this repository.

```text
Continue the dogqoder project in the current repository.

The architecture is approved. Do not restart product discovery or replace the
design with ACP, print mode, worktrees, exclusive workspace locks, or a generic
multi-provider abstraction.

Read these sources in order:
1. README.md
2. docs/superpowers/specs/2026-08-24-qoder-worker-design.md
3. docs/research/2026-08-24-qoder-sdk-feasibility.md
4. spikes/qoder_audit_probe.py

The normative source is the design specification. The research note records
real SDK 1.0.13 behavior and must constrain the implementation where the public
SDK is weaker than the desired interface.

First use the writing-plans skill to create the detailed TDD implementation
plan under docs/superpowers/plans/. Preserve the seven tracer bullets in the
spec, with small commits and explicit verification after every bullet. Once the
plan is ready, begin implementation according to that plan unless a genuine
design contradiction requires user direction.

Implementation constraints:
- Python 3.12, asyncio, uv, and qoder-agent-sdk==1.0.13.
- Use one QoderSDKClient and one asyncio context per live top-level worker.
- Use receive_messages(), not receive_response(), for the production reducer.
- Default to the SDK-bundled QoderCLI. Treat an external cli_path as an explicit
  compatibility-tested override.
- Prefer access_token_from_env() for unattended auth. Never print or persist the
  token. Diagnose qodercli_auth initialization timeouts explicitly.
- Recovery means a new process with resume=<session_id>; it is not reattachment.
- Python 1.0.13 has no public backgroundTasks/stopTask and cannot steer or stop a
  selected nested agent. Unsupported operations must fail honestly.
- Auditor enforcement is layered: visible tools, denylist, dontAsk, and a
  fail-closed can_use_tool callback covering nested agent_id calls.
- The shared workspace is intentional. Warn about overlapping writers; do not
  create a lock or worktree by default.
- Preserve user changes. Do not reset, clean, stash, commit, push, or publish
  unless the user explicitly requests that action.

Start with tracer bullet 1: a tested foreground single-worker auditor adapter.
It must initialize with no prompt, resolve the model through
get_available_models()/set_model(), send the work prompt, reduce semantic
events, settle missing nested terminal telemetry, and return a structured
result. Use an injected fake transport for deterministic tests; keep real Qoder
tests credential-gated and confined to a disposable directory.

Before claiming tracer bullet 1 complete, demonstrate:
- unit tests for model resolution and the result/task settlement reducer;
- a read-only policy test covering a nested agent_id;
- an authentication failure diagnostic that exposes no credential value;
- one opt-in real Qwen3.8-Max audit when credentials are available;
- a clean git diff containing only planned files.
```
