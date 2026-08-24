# Qoder Worker Task 10 Resume Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven development. This task is being executed inline because the parent worker explicitly prohibited subagents. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resume an eligible terminal Qoder worker as a new durable attempt backed by a fresh SDK process and the prior Qoder session history.

**Architecture:** Supervisor validates durable state, stored session ID, and the original canonical working directory before atomically starting the next attempt. A dedicated two-argument resume transport factory keeps ordinary spawn construction unchanged while production maps the stored session to public `QoderAgentOptions.resume`; resumed execution sends one recovery-only objective through the existing foreground auditor lifecycle. RPC and CLI remain thin JSON surfaces over this supervisor operation.

**Tech Stack:** Python 3.12, asyncio, SQLite, qoder-agent-sdk 1.0.13, pytest, Ruff, strict mypy.

**Spec:** `docs/superpowers/specs/2026-08-24-qoder-worker-design.md` sections 7, 8, and 12.7; `.superpowers/sdd/2026-08-24-qoder-worker-v1/task-10-brief.md`

## Global Constraints

- Resume accepts only `lost`, `failed`, or `cancelled`; all other states return `resume_not_possible`.
- Resume requires a stored session ID and the original canonical existing directory.
- Resume increments the same durable worker's attempt and starts a new SDK process with `resume=<session_id>`.
- Resume sends a concise recovery message that says the prior process ended, requires workspace-state rechecking, and does not claim interrupted work completed.
- Pending approvals, callback futures, steering queues, and earlier messages are never replayed or fabricated.
- Context7 exposes Qoder Cloud REST sessions but no matching `qoder-agent-sdk` Python surface; implementation is constrained to the pinned 1.0.13 public `QoderAgentOptions.resume` field inspected locally.
- Tests use fakes only; no real Qoder, credentials, network, credits, destructive Git, or push operations.

---

### Task 1: Resume SDK construction

**Files:**
- Modify: `src/qworker/qoder_sdk.py`
- Test: `tests/test_resume.py`

**Interfaces:**
- Consumes: canonical `cwd` and validated stored `session_id`.
- Produces: `build_auditor_options(..., resume=...)`, `build_configured_auditor_options(..., resume=...)`, and `create_resumed_transport(cwd, session_id)`.

- [ ] **Step 1: Write the failing public-options test**

```python
options = build_auditor_options(tmp_path, resume="session-resume")
assert options.cwd == tmp_path
assert options.resume == "session-resume"
```

- [ ] **Step 2: Run test and verify RED**

Run: `uv run pytest tests/test_resume.py::test_resume_transport_sets_public_sdk_option -q`

Expected: `build_auditor_options()` rejects the unknown `resume` keyword.

- [ ] **Step 3: Implement minimal public-SDK wiring**

Pass `resume` only to `QoderAgentOptions`; make `create_resumed_transport()` construct a new `QoderSDKClient` from configured resume options. Do not attach to or reuse an existing client/process.

- [ ] **Step 4: Run test and verify GREEN**

Run: `uv run pytest tests/test_resume.py::test_resume_transport_sets_public_sdk_option -q`

Expected: test passes without constructing or connecting a live SDK client.

### Task 2: Supervisor eligibility and new-attempt behavior

**Files:**
- Modify: `src/qworker/supervisor.py`
- Test: `tests/test_resume.py`

**Interfaces:**
- Consumes: `ResumeTransportFactory = Callable[[Path, str], QoderTransport]`, durable `WorkerRecord`, and stored attempt/session/cwd.
- Produces: `Supervisor.resume(worker_id) -> dict[str, JsonValue]` accepting one new attempt before execution begins.

- [ ] **Step 1: Write terminal eligibility and rejection tests**

```python
accepted = await supervisor.resume(worker_id)
assert accepted["attempt"] == 2
assert accepted["state"] == "starting"

with pytest.raises(SupervisorError, match="resume") as rejected:
    await supervisor.resume(ineligible_worker_id)
assert rejected.value.code == "resume_not_possible"
```

Cover `lost`, `failed`, and `cancelled`; reject `starting`, `running`, `requires_action`, `completed`, and eligible states missing a session.

- [ ] **Step 2: Run cases and verify RED**

Run: `uv run pytest tests/test_resume.py -q`

Expected: `Supervisor` lacks resume construction and `resume()`.

- [ ] **Step 3: Implement validated new-attempt acceptance**

Under the supervisor control lock: reject a closed supervisor, load the durable worker, check state/session, strictly resolve the stored cwd and require the same directory, capture the old session, call `WorkerStore.start_attempt()`, and schedule `_run_worker()` with the dedicated resume transport factory. Return worker ID, state, role, canonical cwd, attempt, and event cursor.

- [ ] **Step 4: Add recovery and non-replay test**

Assert resume factory receives exactly stored cwd/session, the new transport receives only one prompt containing these independent literals:

```text
prior Qoder process ended
recheck the current workspace state
do not assume interrupted work completed
```

Assert no prior steering body, approval answer, callback future, or original objective appears in the new transport's prompt/calls.

- [ ] **Step 5: Implement recovery-only execution and verify GREEN**

Use a fixed recovery objective with the existing prompt envelope and a contract built only from durable cwd/requested model. Do not inspect prior events for replay. Ensure an old task callback removes only its own task identity, never a newer resumed owner task.

Run: `uv run pytest tests/test_resume.py -q`

Expected: eligibility, cwd/session capture, attempt increment, recovery, and non-replay cases pass.

### Task 3: RPC and JSON CLI resume

**Files:**
- Modify: `src/qworker/rpc.py`
- Modify: `src/qworker/cli.py`
- Test: `tests/test_resume.py`

**Interfaces:**
- RPC consumes: method `resume`, params `{ "worker_id": string }`.
- CLI produces: `qworker resume WORKER_ID --json`, forwarding only the worker ID and printing one JSON response.

- [ ] **Step 1: Write failing RPC/CLI test**

Start the local fake-backed RPC server, call CLI `resume`, and assert exit zero plus the accepted new attempt. Send an unexpected RPC parameter and assert `invalid_request` to retain the closed request schema.

- [ ] **Step 2: Run case and verify RED**

Run: `uv run pytest tests/test_resume.py -q`

Expected: CLI parser or RPC dispatch rejects unknown resume command/method.

- [ ] **Step 3: Implement thin dispatch and production factory wiring**

Add CLI parser/dispatch, RPC field validation/dispatch, and supply `create_resumed_transport` when the production server creates `Supervisor`.

- [ ] **Step 4: Run focused suite and verify GREEN**

Run: `uv run pytest tests/test_resume.py -q`

Expected: all Task 10 tests pass.

### Task 4: Verification, report, and commit

**Files:**
- Create: `.superpowers/sdd/2026-08-24-qoder-worker-v1/task-10-report.md`

- [ ] **Step 1: Run focused and default tests**

Run: `uv run pytest tests/test_resume.py -q`

Run: `uv run pytest -q`

- [ ] **Step 2: Run static and diff checks**

Run: `uv run ruff check .`

Run: `uv run mypy src tests/test_resume.py`

Run: `git diff --check`

- [ ] **Step 3: Refresh graph and review scope**

Run: `code-review-graph update --brief`

Run: `code-review-graph detect-changes --base main --brief`

- [ ] **Step 4: Record evidence and caveats in report**

Document RED/GREEN commands, passing verification, Context7 mismatch and pinned public-API evidence, explicit resume rejection matrix, recovery/non-replay guarantees, and skipped real-Qoder checks.

- [ ] **Step 5: Commit**

```bash
git add src/qworker/supervisor.py src/qworker/qoder_sdk.py src/qworker/rpc.py src/qworker/cli.py tests/test_resume.py docs/superpowers/plans/2026-08-24-qoder-worker-task-10.md .superpowers/sdd/2026-08-24-qoder-worker-v1/task-10-report.md
git commit -m "feat: resume qoder conversation in new attempt"
```
