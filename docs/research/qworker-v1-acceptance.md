# qworker V1 acceptance evidence

Date: 2026-08-24
Updated: 2026-08-25

## Verdict

V1 is **accepted under one documented waiver**. Credential-free
public-interface coverage demonstrates the lifecycle, control, persistence,
recovery, and security contracts. Criteria 5, 6, and 11 are live-confirmed by
bounded authorized markers. Acceptance criterion 7 is **Waived — QoderCLI
limitation** by explicit user decision; the waiver records the unsupported
boundary and is not a demonstration.

Authentication scope uses `QODER_PERSONAL_ACCESS_TOKEN`. On 2026-08-25 the
user explicitly excluded OAuth approval flows from V1 because this deployment
authenticates with PAT rather than OAuth. Issue acceptance is evaluated against
that superseding scope decision; qworker does not claim an OAuth approval
workflow.

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
  OS-process observation is absent or did not satisfy every required gate.
- **Blocked**: required public capability or trustworthy evidence is absent.
- **Waived — QoderCLI limitation**: the user accepted that the installed runtime
  boundary cannot supply the required evidence; this does not mean the criterion
  passed.

| AC | Status | Evidence and observed boundary |
| --- | --- | --- |
| 1 | Demonstrated | `test_public_cli_controls_two_workers_and_replays_each_cursor` launches two concurrent `spawn` CLI processes. Both return immediately with distinct stable IDs and remain `running` together. |
| 2 | Demonstrated | Same test uses fresh CLI processes for `status`, `watch`, `steer`, `cancel-message`, `respond`, and `result` while one supervisor owns both attempts. |
| 3 | Demonstrated | Same test replays from zero, reconnects with the last cursor, follows to terminal state, and asserts contiguous per-worker sequences without cross-worker events. |
| 4 | Live-confirmed | The deterministic lifecycle test proves requested `qwen-auditor`, resolved `Qwen3.8-Max`, and actual `Qwen3.8-Max` fields at the public result boundary. The authorized live rerun confirmed accepted/starting/running/result/completed lifecycle, no requires-action/lost/failed state, bundled runtime selection, and resolved plus actual `Qwen3.8-Max` reporting. |
| 5 | Live-confirmed | The public replay now separates worker lifecycle and host-verified workspace facts from the optional model report: a non-error result may remain `partial` with `report_contract_unparseable` while the worker reaches `completed`. One authorized live marker then confirmed result receipt, completed lifecycle, exact marker bytes, disposable non-worktree isolation, resolved/actual `Qwen3.8-Max`, and credential exclusion. Model-authored JSON, reported files, and self-reported validation are no longer evidence gates for whether the coder performed the requested work. |
| 6 | Live-confirmed | Deterministic callback tests exercise the host enforcement boundary directly and deny top-level plus direct-helper Write/Edit/NotebookEdit/Bash, unknown tools, MCP writes, and nested-agent recursion. The authorized live containment marker then completed through the strict auditor report channel with a byte-identical workspace and credential-free output. Model-generated forbidden calls remain optional telemetry because their absence cannot disprove host enforcement. |
| 7 | Waived — QoderCLI limitation | Adapter fixtures can normalize task messages, but the installed live QoderCLI/SDK path did not provide a trustworthy public correlation from worker task telemetry to post-run direct-helper discovery. The SDK's transcript readers are standalone local functions keyed by a session UUID; qworker's public result intentionally does not expose that locator or persist direct-helper task events. Context7 returned cloud-session material rather than a supported local worker correlation flow. The user explicitly waived AC7; it is not accepted as demonstrated. |
| 8 | Demonstrated | The lifecycle test emits task start/progress without terminal telemetry. Both public results finish with `nested_state=unknown` and warning `nested_terminal_event_missing`; neither worker stays running. |
| 9 | Demonstrated | The lifecycle test sends `now`, `next`, and `later` through separate CLI processes, validates UUIDs and priorities, observes queued/delivered durable events, and cancels one UUID-stamped message. Cancellation is asserted as the injected transport observation, not a universal guarantee. |
| 10 | Demonstrated | The lifecycle test answers a currently live permission request through `respond`. `test_public_loss_expires_live_approval_and_rejects_response` then proves an approval expires on loss and later response fails with `approval_not_pending`. |
| 11 | Live-confirmed | Deterministic race coverage now records whether the adapter accepted ResultEvent or stop first: result-before-stop preserves completed/failed, while stop-before-interrupt-result persists the summary but terminates cancelled/exited. The authorized live marker observed running, returned cancelled after non-forced stop, preserved the disposable edit, and verified that the exact SDK-owned PID/start-time identity disappeared using only `/proc/<pid>/stat`; credential exclusion also passed. |
| 12 | Demonstrated | The loss test observes `lost`/`exited`. `test_public_resume_uses_same_id_cwd_session_and_new_attempt` uses a later CLI process to keep the stable ID and canonical cwd, pass the stored session to a fresh transport factory, increment attempt 1 to 2, send recovery text, and preserve partial edits. Prior live adapter resume evidence is recorded separately in `docs/research/2026-08-24-qoder-task-9-10-uat.md`; it was not rerun here. |
| 13 | Demonstrated | The public coder security test sends a credential-bearing objective only over stdin, asserts it is absent from process arguments, stdout/stderr, watch events, result JSON, transport call categories, SQLite/WAL files, and workspace files. Failure-suite redaction tests cover recursive diagnostic/log/store boundaries. |
| 14 | Demonstrated | The lifecycle test rejects selected nested-agent steering and the stop test rejects selected nested-agent stop with explicit `unsupported_operation`; neither path reports success. |

## Default verification

Focused public-interface checks completed before the AC5/AC6/AC11 live tests:

```bash
uv run pytest tests/integration/test_real_worker_lifecycle.py tests/integration/test_real_auditor_security.py tests/integration/test_real_resume.py -m 'not real_qoder' -q
# 10 passed, 4 deselected in 16.89s
```

The first full default-suite run encountered one timeout in the unrelated
`test_rpc_over_limit_request_with_near_limit_id_stays_structured` test. That
node passed immediately in isolation, and subsequent complete default-suite
runs passed; the final run was:

```bash
uv run pytest tests/test_supervisor_rpc.py::test_rpc_over_limit_request_with_near_limit_id_stays_structured -q
# 1 passed in 0.73s

uv run pytest tests/test_coder.py tests/integration/test_real_auditor_security.py -m 'not real_qoder' -q
# 8 passed, 3 deselected in 3.08s

uv run pytest -q
# 263 passed, 7 deselected in 23.25s

uv run ruff format --check src tests
# Failed: 7 shared files outside Task 14 scope would be reformatted

uv run ruff format --check src/qworker/coder.py tests/integration/test_real_worker_lifecycle.py tests/integration/test_real_auditor_security.py tests/integration/test_real_resume.py
# 4 files already formatted

uv run ruff check src tests
# All checks passed

uv run mypy src tests
# Success: no issues found in 48 source files

git diff --check
# clean
```

The 2026-08-25 AC5 evidence change then passed the two focused public tests,
Ruff, full mypy, scoped formatting, and `git diff --check`. A parallel full
suite run hit the same unrelated one-second large-RPC timeout; that node passed
alone, and the serial confirmation was clean:

```bash
uv run pytest -m 'not real_qoder' -q
# 263 passed, 7 deselected in 25.26s
```

## Bounded real-Qoder marker UAT

Only the specifically authorized Task 14 marker nodes ran. Existing
`real_qoder` tests and earlier Tasks 9–10 UATs were excluded. Each test used a
pytest disposable directory, sent its objective over stdin, captured child
stdout/stderr, applied per-process plus 190-second in-test and 210-second outer
timeouts, and emitted no token, session ID, prompt, marker, transcript, or model
response. The initial AC5/AC6/AC11 nodes ran serially exactly once each. A later
AC5-only diagnosis phase received separate explicit authorization for exactly
one post-fix rerun; AC6, AC11, and all other real nodes remained excluded.

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

### Authorized AC5, AC6, and AC11 one-shot markers

After deterministic safe-evidence regressions and the credential-free matrix
passed, the user authorized exactly one live node for each remaining criterion.
The commands and complete safe outcomes were:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_auditor_security.py::test_real_coder_edit_and_structured_validation_ac5 -q
# 1 failed in 28.15s
```

AC5 fixed-boolean evidence: exact marker file `true`, disposable workspace had
no `.git` directory `true`, resolved model `Qwen3.8-Max` `true`, actual model
`Qwen3.8-Max` `true`, and credential exclusion `true`; completed result,
reported `marker.txt`, and nonempty structured validation were all `false`.
The coder therefore demonstrated a real edit but not the required validation
report. The node was not retried under that authorization.

### AC5 diagnosis, fix, and authorized post-fix rerun

A credential-free public-boundary replay reproduced the exact split: the coder
applied its edit and emitted an exact report in an assistant event, but an empty
terminal SDK result reduced to partial with empty files and validation. Before
the fix, the minimized command failed deterministically; after commit `d0b9bd7`
(`fix: recover coder report contract`), the same command passed:

```bash
uv run pytest tests/integration/test_real_auditor_security.py::test_ac5_replay_requires_contract_after_successful_coder_edit -q
# pre-fix: 1 failed in 1.65s
# post-fix: 1 passed in 1.60s
```

The minimal fix keeps a valid terminal result authoritative and otherwise uses
the newest assistant text that independently satisfies the unchanged exact
five-key coder contract. It does not accept fenced JSON, extra keys, or a
missing contract.

The user then authorized exactly one bounded AC5-only real marker from the
committed fix. It ran once and was not retried:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_auditor_security.py::test_real_coder_edit_and_structured_validation_ac5 -q
# 1 failed in 30.71s
```

The fixed-boolean signature was unchanged: exact marker file, no worktree,
resolved/actual `Qwen3.8-Max`, and credential exclusion were `true`; completed
result, reported marker file, and nonempty validation were `false`. This safely
rules out “exact contract existed in a prior assistant event” for this run; it
does not reveal or retain the prompt, marker, session, transcript, or response.
No AC6, AC11, earlier UAT, or other real test ran in this diagnosis phase.

### AC5 verified-work reclassification and authorized one-shot

On 2026-08-25 the user explicitly changed AC5's success definition: objective
evidence that the worker performed the requested edit is authoritative;
model-authored JSON is optional reporting. qworker's structured result remains
unchanged and still exposes `report_contract_unparseable` when appropriate.

The credential-free public CLI replay first failed after removing the synthetic
assistant report. Its safe evidence showed exact marker bytes, expected model,
and non-worktree isolation as `true`; only the three report-derived gates were
`false`. The revised replay then passed with a deliberately partial result and
the warning preserved:

```bash
uv run pytest tests/integration/test_real_auditor_security.py::test_coder_work_evidence_uses_lifecycle_and_host_validation tests/integration/test_real_auditor_security.py::test_ac5_replay_accepts_verified_edit_without_report_contract -q
# 2 passed in 1.65s
```

After the complete credential-free suite passed, exactly one revised real AC5
marker ran:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_auditor_security.py::test_real_coder_completes_host_verified_edit_ac5 -q
# 1 passed in 34.05s
```

The fixed-boolean gates confirm a public result event, worker lifecycle
`completed`, exact marker bytes, no `.git` directory, resolved and actual
`Qwen3.8-Max`, and credential exclusion. The host's exact-byte check is the
validation authority. The test neither requires nor exposes model-authored
JSON, reported files, self-reported validation, prompt, marker, session, or
response text. No other real node ran, and this marker was not retried. AC5 is
therefore **Live-confirmed** under the revised evidence definition.

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_auditor_security.py::test_real_auditor_and_direct_helper_denials_ac6 -q
# 1 failed in 91.35s
```

AC6 fixed-boolean evidence: workspace byte snapshot unchanged `true` and
credential exclusion `true`. Observed denial callbacks for top-level and helper
Write/Edit/Bash were all `false`; top-level Agent allow was also `false`, and
the structured result did not complete. Because no callback was observed, the
unchanged workspace cannot be presented as proof that the live auditor/helper
were denied. The node was not retried.

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_resume.py::test_real_normal_stop_preserves_edit_and_reaps_owned_child_ac11 -q
# 1 failed in 9.09s
```

AC11 fixed-boolean evidence: running observed, non-forced stop, exited health,
preserved workspace bytes, disappearance of the exact SDK-owned child identity,
and credential exclusion were all `true`. The returned state was not
`cancelled`, so the test's normal-cancel gate was `false`. No process scan or
command-line read occurred: identity was limited to the SDK-owned PID, its
parent PID, and start-time ticks from `/proc/<pid>/stat`. The node was not
retried.

### Auditor `submit_audit` MCP reporting channel

On 2026-08-25 the auditor report contract moved from best-effort parsing of the
model's terminal prose to a host-owned in-process MCP tool. The production
auditor registers `mcp__qworker_audit__submit_audit` with an exact nine-field
JSON Schema, permits it only for the top-level auditor, and captures the latest
valid submission without routing this host-internal action through human
approval. The transport substitutes the captured, redacted canonical JSON only
when the SDK terminal result arrives. If the tool is unavailable or is never
called, the existing strict terminal-result parser remains the compatibility
fallback and still emits `report_contract_unparseable` for invalid prose.

Credential-free verification passed:

```bash
uv run ruff check src tests
# All checks passed!

uv run mypy src
# Success: no issues found in 20 source files

uv run pytest -q
# 269 passed, 7 deselected in 24.68s
```

An explicitly authorized real auditor UAT first passed against the unchanged
default runtime:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_qoder_auditor.py::test_real_qwen_auditor_is_read_only -q
# 1 passed in 24.61s
```

Review then removed `submit_audit` from the SDK auto-approval list so a nested
agent could not bypass the top-level-only permission callback. After fresh user
authorization, the final hardened path ran exactly once and passed:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_qoder_auditor.py::test_real_qwen_auditor_is_read_only -q
# 1 passed in 28.25s

timeout 90s uv run qworker doctor --json
# ok=true, SDK 1.0.13, runtime path=bundled, runtime version=1.1.23
```

The real test confirmed a completed structured audit, expected model selection,
credential exclusion, and an unchanged disposable workspace. It was not
retried after the final hardening change. This resolves the auditor
reporting-format decision without weakening finding/verdict semantics and
without switching qworker to the external 1.1.28 runtime.

### AC6 and AC11 closure

AC6 now separates two independent claims. The deterministic permission-boundary
test is authoritative for top-level and direct-helper denial decisions. The
real marker is authoritative for runtime containment and completion; it does not
require the model to choose to issue every prohibited call. Credential-free
verification passed before any live marker:

```bash
uv run ruff check src tests
# All checks passed!

uv run mypy src
# Success: no issues found in 20 source files

uv run pytest -q
# 270 passed, 7 deselected in 25.51s
```

The full repository `ruff format --check src tests` still identifies seven
pre-existing unformatted files. All changed source files and the changed AC6
integration test pass their scoped format check; no unrelated formatting rewrite
was made.

Exactly one revised AC6 marker ran against SDK 1.0.13 and bundled QoderCLI
1.1.23:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_auditor_security.py::test_real_auditor_containment_ac6 -q
# 1 passed in 105.19s
```

Its fixed gates were completed structured result, byte-identical workspace, and
credential exclusion. The adversarial callback probe remained active and
retained only fixed denial-category booleans, but callback occurrence was not a
success condition.

AC11's failure was qworker terminal arbitration, not failure to terminate the
SDK child. The foreground adapter now records when its first ResultEvent is
accepted. A stop already registered at that point wins the lifecycle state as
`cancelled`; a result accepted before stop retains `completed` or `failed`.
Either path durably preserves an accepted result summary. Focused deterministic
race coverage passed for both auditor and coder plus 10 result-before-stop
cases, including normal/force/close overlap and paused result persistence.

Exactly one revised AC11 marker then ran:

```bash
timeout 210s uv run pytest -m real_qoder tests/integration/test_real_resume.py::test_real_normal_stop_preserves_edit_and_reaps_owned_child_ac11 -q
# 1 passed in 14.26s
```

Its fixed gates all passed: running observed, normal cancelled state, exited
health, non-forced stop, workspace preserved, exact owned child gone, and
credential exclusion. Neither live marker was retried.

## Remaining acceptance work

1. Revisit AC7 only if a later QoderCLI/SDK boundary supplies correlatable live
   direct-helper telemetry and transcript discovery; its current waiver is not
   acceptance evidence.
