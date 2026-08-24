# Task 10 Report: Resume as a New Attempt

## Outcome

Implemented Task 10 only:

- `Supervisor.resume()` accepts only `lost`, `failed`, and `cancelled` workers;
- live states, `completed`, missing-session, missing-cwd, and non-canonical-cwd cases return `resume_not_possible` before attempt mutation or transport construction;
- accepted resume captures the prior session ID, durably increments the same worker to the next attempt, and schedules a distinct owner task;
- a dedicated resume factory creates a fresh SDK client/process with public `QoderAgentOptions.resume=<stored session_id>` and the original canonical cwd;
- resumed execution sends one fixed recovery objective stating that the prior Qoder process ended, current workspace state must be rechecked, and interrupted work must not be assumed complete;
- earlier objective text, approval display, callback future, and steering body are neither fabricated nor replayed into the new attempt;
- prior pending approval resolves fail-closed before resume, and resumed status exposes no prior pending approval;
- old attempt completion callbacks remove only their own task identity, so they cannot evict a newer resumed owner task;
- RPC exposes the closed-schema `resume` method and CLI exposes `qworker resume WORKER_ID --json`;
- production RPC server supplies `create_resumed_transport` separately from ordinary spawn construction.

## TDD evidence

1. SDK option RED: `uv run pytest tests/test_resume.py::test_resume_transport_sets_public_sdk_option -q` failed because `build_auditor_options()` rejected the `resume` keyword.
2. SDK option GREEN: the same test passed after wiring the public `QoderAgentOptions.resume` field.
3. Supervisor RED: `uv run pytest tests/test_resume.py::test_failed_worker_resumes_as_new_attempt -q` failed because `Supervisor.__init__()` lacked the resume transport seam.
4. Supervisor GREEN: failed worker acceptance returned durable attempt 2 in `starting` without constructing the transport before the owner task ran.
5. Eligibility GREEN: parameterized coverage accepted `lost`, `failed`, and `cancelled`; rejected `starting`, `running`, `requires_action`, `completed`, and missing-session cases with `resume_not_possible` and attempt 1 preserved.
6. Recovery/non-replay GREEN: stored cwd/session reached the new factory exactly once; one resumed prompt contained all recovery statements and none of the prior objective, approval body, or steering body; the prior approval resolved deny.
7. Cwd GREEN: renaming the original working directory caused `resume_not_possible`, no factory construction, and no attempt increment.
8. RPC/CLI RED: JSON CLI returned exit 2 because `resume` was not a recognized command.
9. RPC/CLI GREEN: CLI returned attempt 2 through local RPC; an unexpected RPC parameter returned `invalid_request`.

Final Task 10 suite: `13 passed`.

## Verification

- Focused suite: `13 passed`.
- Full default suite: `208 passed, 1 deselected`.
- `uv run ruff check src tests`: passed.
- `uv run mypy src tests`: passed in strict mode across 36 source files.
- `git diff --check`: passed.
- `code-review-graph update --brief`: refreshed 7 changed files.
- `code-review-graph detect-changes --base main --brief`: completed; no affected execution flows reported.

## SDK documentation evidence

Context7 library resolution selected high-reputation `/websites/qoder`, but the docs query returned Qoder Cloud REST session APIs rather than the `qoder-agent-sdk` Python client surface needed here. Continued with allowed local inspection of pinned `qoder-agent-sdk==1.0.13`, verifying public `QoderAgentOptions.resume: str | None`, `QoderSDKClient(options=...)`, `connect()`, `query()`, and `receive_messages()` signatures. Resume construction uses only the public options field; no private initialization or process-attachment API is used.

## Self-review

- Lifecycle: validation occurs under the control lock; `WorkerStore.start_attempt()` supplies the only terminal-to-starting transition and attempt increment.
- Durability: session ID is captured before `start_attempt()` clears prior-attempt session/result fields.
- Concurrency: stale task callbacks compare task identity before removing `_tasks[worker_id]`.
- Isolation: resumed contract is reconstructed only from fixed recovery text plus durable cwd/requested model; prior events and ephemeral controller queues are not read.
- SDK boundary: resume factory constructs a new `QoderSDKClient`; no existing transport/client/process is reused.
- RPC: request schema accepts exactly `worker_id`; CLI sends no prompt, credential, session, or cwd body.

## Caveats

- No real credentials, Qoder network request, credit-consuming call, or live QoderCLI process ran.
- Default pytest correctly deselected credential-gated real-Qoder coverage.
- Repository-wide `ruff check .` still reports one pre-existing import-order finding in unchanged `spikes/qoder_audit_probe.py`; scoped production/test Ruff check is clean. Task 10 did not alter that spike.
