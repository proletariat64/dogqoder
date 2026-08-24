# qworker V1 acceptance evidence

Date: 2026-08-24

## Verdict

V1 is **not yet fully accepted**. Credential-free public-interface coverage now
demonstrates most lifecycle, control, persistence, recovery, and security
contracts. Acceptance criterion 7 remains blocked because the public supervisor
does not expose direct-subagent task events or a post-run transcript-discovery
API. Criteria 5, 6, and 11 have deterministic seam evidence but still lack the
corresponding bounded real-Qoder evidence required by design section 17.3.

This report distinguishes deterministic guarantees from live observations. A
fake transport proves qworker behavior at its stable CLI/RPC/transport seams; it
does not prove that the installed Qoder runtime emitted a particular live event
or exited a particular OS process.

## Environment

| Component | Observed version |
| --- | --- |
| Python | 3.12.3 |
| `qoder-agent-sdk` | 1.0.13 (project pin) |
| bundled QoderCLI runtime | 1.1.23 |
| pytest | 9.1.1 |
| Ruff | 0.16.4 |
| mypy | 2.3.1 |
| platform | Linux, local Unix-domain RPC socket |

Version commands:

```bash
uv run python -c 'import platform; from importlib.metadata import version; print(platform.python_version()); print(version("qoder-agent-sdk")); print(version("pytest")); print(version("ruff")); print(version("mypy"))'
uv run python -c 'import asyncio; from qworker.qoder_sdk import QoderPreflightBackend; result=asyncio.run(QoderPreflightBackend().resolve_runtime(None)); print(result.version)'
```

## Acceptance matrix

Status meanings:

- **Demonstrated**: exercised through a default credential-free public
  interface, with supporting lower-level coverage where needed.
- **Live-confirmed**: deterministic evidence plus a bounded observation from the
  installed credentialed Qoder runtime.
- **Partial**: deterministic behavior is proven, but a required live-runtime or
  OS-process observation is absent.
- **Blocked**: required public capability or trustworthy evidence is absent.

| AC | Status | Evidence and observed boundary |
| --- | --- | --- |
| 1 | Demonstrated | `test_public_cli_controls_two_workers_and_replays_each_cursor` launches two concurrent `spawn` CLI processes. Both return immediately with distinct stable IDs and remain `running` together. |
| 2 | Demonstrated | Same test uses fresh CLI processes for `status`, `watch`, `steer`, `cancel-message`, `respond`, and `result` while one supervisor owns both attempts. |
| 3 | Demonstrated | Same test replays from zero, reconnects with the last cursor, follows to terminal state, and asserts contiguous per-worker sequences without cross-worker events. |
| 4 | Live-confirmed | The deterministic lifecycle test proves requested `qwen-auditor`, resolved `Qwen3.8-Max`, and actual `Qwen3.8-Max` fields at the public result boundary. The authorized live rerun confirmed accepted/starting/running/result/completed lifecycle, no requires-action/lost/failed state, bundled runtime selection, and resolved plus actual `Qwen3.8-Max` reporting. |
| 5 | Partial | `test_public_coder_edits_disposable_workspace_without_leaking_credentials` drives `spawn --role coder`, watch, and result against a disposable workspace; the transport edit and validation report cross the public boundary with no `.git` worktree. A real-Qoder coder was intentionally not run in this bounded UAT. |
| 6 | Partial | `test_auditor_callback_denies_top_level_and_direct_helper_mutation` invokes the actual configured SDK callback for top-level and nested contexts. Write, edit, notebook, shell, unknown, mutation-capable extension, and nested recursion requests are denied and the workspace byte snapshot is unchanged. No live agent was prompted to attempt every denial. |
| 7 | Blocked | Adapter contract coverage normalizes Qoder task-start/progress/terminal messages, and the new lifecycle fixture emits direct-helper task events. The public supervisor persists neither those task events nor a transcript locator. Context7 exposed cloud session endpoints, not a local Python transcript API. No post-run helper transcript can therefore be discovered through qworker's public CLI. |
| 8 | Demonstrated | The lifecycle test emits task start/progress without terminal telemetry. Both public results finish with `nested_state=unknown` and warning `nested_terminal_event_missing`; neither worker stays running. |
| 9 | Demonstrated | The lifecycle test sends `now`, `next`, and `later` through separate CLI processes, validates UUIDs and priorities, observes queued/delivered durable events, and cancels one UUID-stamped message. Cancellation is asserted as the injected transport observation, not a universal guarantee. |
| 10 | Demonstrated | The lifecycle test answers a currently live permission request through `respond`. `test_public_loss_expires_live_approval_and_rejects_response` then proves an approval expires on loss and later response fails with `approval_not_pending`. |
| 11 | Partial | `test_public_stop_preserves_edits_and_disconnects_live_child` rejects nested stop, performs normal top-level stop, preserves an existing workspace edit, reaches cancelled/exited, and observes its transport child as disconnected. It does not inspect a real QoderCLI OS child. |
| 12 | Demonstrated | The loss test observes `lost`/`exited`. `test_public_resume_uses_same_id_cwd_session_and_new_attempt` uses a later CLI process to keep the stable ID and canonical cwd, pass the stored session to a fresh transport factory, increment attempt 1 to 2, send recovery text, and preserve partial edits. Prior live adapter resume evidence is recorded separately in `docs/research/2026-08-24-qoder-task-9-10-uat.md`; it was not rerun here. |
| 13 | Demonstrated | The public coder security test sends a credential-bearing objective only over stdin, asserts it is absent from process arguments, stdout/stderr, watch events, result JSON, transport call categories, SQLite/WAL files, and workspace files. Failure-suite redaction tests cover recursive diagnostic/log/store boundaries. |
| 14 | Demonstrated | The lifecycle test rejects selected nested-agent steering and the stop test rejects selected nested-agent stop with explicit `unsupported_operation`; neither path reports success. |

## Default verification

Focused public-interface checks completed before any live test:

```bash
uv run pytest tests/integration/test_real_worker_lifecycle.py -m 'not real_qoder' -q
# 2 passed, 1 deselected

uv run pytest tests/integration/test_real_auditor_security.py -m 'not real_qoder' -q
# 2 passed

uv run pytest tests/integration/test_real_resume.py -m 'not real_qoder' -q
# 3 passed
```

Final repository verification commands and their exact outcomes are recorded
after the suite completes:

```bash
uv run pytest -m 'not real_qoder' -q
# 259 passed, 4 deselected in 33.21s

uv run ruff format --check src tests
# Failed: 7 pre-existing/shared files outside Task 14 scope would be reformatted:
# src/qworker/control.py, src/qworker/store.py, tests/test_cli.py,
# tests/test_resume.py, tests/test_stop_and_loss.py,
# tests/test_supervisor_rpc.py, tests/test_worker_control.py

uv run ruff format --check tests/integration/test_real_worker_lifecycle.py tests/integration/test_real_auditor_security.py tests/integration/test_real_resume.py
# 3 files already formatted

uv run ruff check src tests
# All checks passed

uv run mypy src tests
# Success: no issues found in 48 source files
```

## Bounded real-Qoder marker UAT

Only the newly added Task 14 marker test may run. Existing `real_qoder` tests
and earlier Tasks 9–10 UATs are excluded. The test uses a pytest disposable
directory, sends the objective over stdin, captures child stdout/stderr, applies
per-process and outer timeouts, performs no write request, and emits no token,
session ID, prompt, marker, or model response.

Initial one-shot diagnostic:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_worker_lifecycle.py::test_real_public_lifecycle_reports_model_and_keeps_workspace_read_only -q
```

Observed one-shot outcome: `1 failed in 16.51s`. The credential gate passed,
the public worker returned a structured result, the private marker was present
in its summary, and the credential was absent from captured spawn/watch/result/
status JSON. The result outcome was not `completed`, so the test stopped with
the generic message `Live public worker did not complete.` No token, session
ID, prompt, marker, or model response appeared in test output.

The command was not retried and no other `real_qoder` test ran. Assertions for
resolved/actual model and the final workspace byte snapshot followed the
completion gate and therefore did not execute; they remain unconfirmed by this
initial run. The SDK does not expose cost through this result, so the credit
cost is unknown and may be nonzero.

### Diagnosis and authorized rerun

The initial assertion incorrectly used structured audit outcome as a proxy for
worker lifecycle state. A deterministic repro proves why that is invalid: a
non-error result that does not satisfy the strict report schema is intentionally
reduced to `outcome=partial` with `report_contract_unparseable`, while worker
lifecycle reaches `completed`. The regression locks down that distinction
without claiming the first live run exposed a diagnostic it did not capture:

```bash
uv run pytest tests/integration/test_real_worker_lifecycle.py::test_public_lifecycle_distinguishes_partial_result_from_completed_worker -q
# 1 passed in 0.59s
```

Test-only instrumentation reduces all public frames to fixed booleans. It can
distinguish accepted, starting, running, requires-action, result, lost, failed,
and completed without retaining or emitting a worker ID, prompt, token, session
ID, transcript, marker, response text, or raw diagnostic.

After the user explicitly authorized continuing UAT, exactly one bounded rerun
of the same live node executed:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_worker_lifecycle.py::test_real_public_lifecycle_reports_model_and_keeps_workspace_read_only -q
# 1 passed in 16.62s
```

The passing fixed-boolean gates confirm `accepted=true`, `starting=true`,
`running=true`, `result=true`, and `completed=true`; they also confirm
`requires_action=false`, `lost=false`, `failed=false`, resolved and actual
`Qwen3.8-Max`, and bundled runtime selection. Credential exclusion, recovery of
the private marker in the structured summary, and an unchanged disposable
workspace byte snapshot also passed. Captured output contained only pytest's
pass summary. No other real test ran and the authorized rerun was not retried.

## Remaining acceptance work

1. Expose bounded direct-subagent task telemetry through public watch/status and
   persist a safe transcript locator that can be resolved after completion.
2. Add a bounded real-Qoder coder edit in a disposable directory when separate
   credit authorization is available.
3. Add a live adversarial auditor/helper run that observes callback denials for
   write and shell requests rather than inferring them from a cooperative prompt.
4. During a live normal stop, identify the owned QoderCLI child and verify it no
   longer exists without inspecting or exposing unrelated process arguments.
