# Qoder Worker V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver persistent, observable, steerable Qoder auditor and coder workers through a stable local CLI and Codex skill.

**Architecture:** Build seven end-to-end tracer bullets in the normative order. Keep Qoder SDK details behind one transport adapter, represent controller state with SDK-independent domain types, persist semantic events in SQLite, and expose the supervisor through local NDJSON RPC and a thin CLI.

**Tech Stack:** Python 3.12, asyncio, qoder-agent-sdk 1.0.13, SQLite, uv, pytest, pytest-asyncio, mypy, and Ruff.

**Spec:** `docs/superpowers/specs/2026-08-24-qoder-worker-design.md`

## Global Constraints

- Python is `>=3.12,<3.13`; `qoder-agent-sdk` remains pinned to `1.0.13`.
- One live top-level worker owns one `QoderSDKClient`, one QoderCLI subprocess, and one asyncio context.
- Production consumes `receive_messages()`; `receive_response()` is probe-only.
- The SDK-bundled QoderCLI is the default; `cli_path` is an explicit compatibility-tested override.
- Unattended authentication prefers `access_token_from_env()`; credential values never enter logs, events, database rows, results, or process arguments.
- Resume starts a new process with `resume=<session_id>`; it is never described as reattachment.
- Python SDK 1.0.13 cannot steer or stop a selected nested agent; unsupported operations return explicit errors.
- Auditor enforcement combines visible tools, mutation denylist, `dontAsk`, and a fail-closed callback that evaluates nested `agent_id` calls.
- Shared workspaces are intentional; overlapping writers produce warnings, not automatic locks or worktrees.
- Production code preserves user changes and never resets, cleans, stashes, commits, pushes, publishes, deploys, or rolls back a workspace.
- Unit and contract tests never require credentials. Real Qoder tests are opt-in, credit-aware, and use disposable directories.

## Confirmed Test Seams

Before Task 1 tests begin, confirm these public seams with the user:

1. `resolve_model()` maps an exact request or configured alias plus a live catalog to one explicit resolution.
2. `AuditorToolPolicy.decide()` returns an SDK-independent allow/deny decision for a tool and optional nested agent ID.
3. `ResultReducer` consumes normalized semantic events and returns one structured result with task settlement confidence.
4. `ForegroundAuditor.run()` exercises the full worker behavior through an injected `QoderTransport`; the real SDK transport is the second adapter at this seam.

---

## Tracer Bullet 1: Foreground Single-Worker Auditor

### Task 1: Establish domain contracts, model policy, and auditor policy

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `src/qworker/__init__.py`
- Create: `src/qworker/domain.py`
- Create: `src/qworker/model_policy.py`
- Create: `src/qworker/auditor_policy.py`
- Create: `tests/unit/test_model_policy.py`
- Create: `tests/unit/test_auditor_policy.py`

**Interfaces:**
- Consumes: enabled model catalog entries from the SDK transport and auditor tool requests.
- Produces: `AuditContract`, `AuditResult`, `AvailableModel`, `ModelResolution`, `PolicyDecision`, `resolve_model()`, and `AuditorToolPolicy.decide()`.

- [ ] **Step 1: Add the test toolchain and package layout**

Add pytest, pytest-asyncio, mypy, and Ruff as uv development dependencies. Configure pytest with `asyncio_mode = "auto"`, a `real_qoder` marker, and `src` on the import path; configure strict mypy for Python 3.12 and Ruff for `py312`.

- [ ] **Step 2: Write the failing model-resolution tests**

```python
def test_exact_model_requires_an_enabled_catalog_entry() -> None:
    catalog = [AvailableModel(value="Qwen3.8-Max", enabled=True)]
    assert resolve_model("Qwen3.8-Max", catalog).resolved == "Qwen3.8-Max"


def test_alias_selects_first_enabled_candidate() -> None:
    catalog = [
        AvailableModel(value="Qwen3.8-Max", enabled=False),
        AvailableModel(value="Qwen3.7-Max", enabled=True),
    ]
    resolution = resolve_model("qwen-auditor", catalog)
    assert resolution.resolved == "Qwen3.7-Max"
    assert resolution.used_fallback is True


def test_exact_model_never_falls_back() -> None:
    with pytest.raises(ModelUnavailableError):
        resolve_model(
            "Qwen3.8-Max",
            [AvailableModel(value="Auto", enabled=True)],
        )
```

- [ ] **Step 3: Run the model tests red**

Run: `uv run pytest tests/unit/test_model_policy.py -q`

Expected: collection fails because the model-policy interface does not exist.

- [ ] **Step 4: Implement the minimal model contract**

```python
@dataclass(frozen=True, slots=True)
class AvailableModel:
    value: str
    enabled: bool


@dataclass(frozen=True, slots=True)
class ModelResolution:
    requested: str
    resolved: str
    used_fallback: bool


MODEL_ALIASES: Final = {
    "qwen-auditor": ("Qwen3.8-Max", "Qwen3.7-Max", "Auto"),
    "qwen-coder": ("Qwen3.8-Max", "Qwen3.7-Max", "Auto"),
}
```

Implement `resolve_model()` so exact names validate one enabled catalog entry and aliases select only from their declared ordered candidates.

- [ ] **Step 5: Run the model tests green and typecheck**

Run: `uv run pytest tests/unit/test_model_policy.py -q`

Run: `uv run mypy src/qworker tests/unit/test_model_policy.py`

Expected: both commands pass.

- [ ] **Step 6: Write the failing auditor-policy tests**

```python
@pytest.mark.parametrize("tool", ["Read", "Glob", "Grep", "WebFetch", "WebSearch"])
def test_nested_auditor_can_use_read_only_tools(tool: str) -> None:
    assert AuditorToolPolicy().decide(tool, agent_id="nested-1").allowed is True


@pytest.mark.parametrize("tool", ["Write", "Edit", "Bash", "NotebookEdit", "Unknown"])
def test_auditor_denies_mutating_and_unknown_tools(tool: str) -> None:
    decision = AuditorToolPolicy().decide(tool, agent_id="nested-1")
    assert decision.allowed is False
    assert decision.reason == "auditor_tool_denied"


def test_nested_auditor_cannot_spawn_another_agent() -> None:
    assert AuditorToolPolicy().decide("Agent", agent_id="nested-1").allowed is False
```

- [ ] **Step 7: Run the policy tests red**

Run: `uv run pytest tests/unit/test_auditor_policy.py -q`

Expected: collection fails because `AuditorToolPolicy` does not exist.

- [ ] **Step 8: Implement the minimal policy interface**

```python
@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    reason: str


class AuditorToolPolicy:
    visible_tools = frozenset({"Read", "Glob", "Grep", "WebFetch", "WebSearch", "Agent"})
    denied_tools = frozenset({"Write", "Edit", "Bash", "NotebookEdit"})

    def decide(self, tool_name: str, *, agent_id: str | None) -> PolicyDecision:
        allowed = tool_name in self.visible_tools and not (
            agent_id is not None and tool_name == "Agent"
        )
        return PolicyDecision(allowed, "auditor_tool_allowed" if allowed else "auditor_tool_denied")
```

- [ ] **Step 9: Run focused tests, formatting, lint, and typechecking**

Run: `uv run pytest tests/unit/test_model_policy.py tests/unit/test_auditor_policy.py -q`

Run: `uv run ruff format --check src tests`

Run: `uv run ruff check src tests`

Run: `uv run mypy src/qworker tests/unit`

Expected: all commands pass.

- [ ] **Step 10: Commit Task 1**

```bash
git add pyproject.toml uv.lock src/qworker tests/unit
git commit -m "feat: define auditor worker contracts"
```

### Task 2: Reduce semantic messages and settle nested tasks

**Files:**
- Create: `src/qworker/events.py`
- Create: `src/qworker/reducer.py`
- Create: `tests/unit/test_result_reducer.py`

**Interfaces:**
- Consumes: normalized `AssistantEvent`, `TaskStartedEvent`, `TaskProgressEvent`, `TaskTerminalEvent`, `SystemEvent`, and `ResultEvent` values.
- Produces: `ResultReducer.apply(event)`, `ResultReducer.needs_settlement`, and `ResultReducer.finish(settlement_expired)` returning `AuditResult`.

- [ ] **Step 1: Write a failing successful-result test**

```python
def test_result_with_no_active_tasks_completes_immediately() -> None:
    reducer = ResultReducer(requested_model="qwen-auditor", resolved_model="Qwen3.8-Max")
    reducer.apply(AssistantEvent(text=("checked code",), tools=("Read",), model="Qwen3.8-Max"))
    reducer.apply(ResultEvent(session_id="session-1", is_error=False, result='{"outcome":"completed","summary":"safe","files":["README.md"],"validation":[],"risks":[]}'))

    result = reducer.finish(settlement_expired=False)

    assert result.outcome == "completed"
    assert result.nested_state == "settled"
    assert result.actual_models == ("Qwen3.8-Max",)
```

- [ ] **Step 2: Run the successful-result test red**

Run: `uv run pytest tests/unit/test_result_reducer.py::test_result_with_no_active_tasks_completes_immediately -q`

Expected: collection fails because event and reducer types do not exist.

- [ ] **Step 3: Implement normalized event types and minimal reducer**

```python
@dataclass(frozen=True, slots=True)
class TaskStartedEvent:
    task_id: str
    description: str


@dataclass(frozen=True, slots=True)
class TaskTerminalEvent:
    task_id: str
    status: Literal["completed", "failed", "stopped"]


@dataclass(frozen=True, slots=True)
class ResultEvent:
    session_id: str
    is_error: bool
    result: str | None
    model_usage: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
```

`ResultReducer.apply()` records assistant models, tool uses, task ledger changes, bounded semantic events, the main result, permission denials, and errors. `finish()` parses the report-contract JSON when possible and otherwise returns the result text as summary with warning `report_contract_unparseable`.

- [ ] **Step 4: Run the successful-result test green**

Run: `uv run pytest tests/unit/test_result_reducer.py::test_result_with_no_active_tasks_completes_immediately -q`

Expected: pass.

- [ ] **Step 5: Write failing nested-settlement tests**

```python
def test_terminal_task_after_result_settles_completion() -> None:
    reducer = started_reducer()
    reducer.apply(ResultEvent(session_id="session-1", is_error=False, result="done"))
    assert reducer.needs_settlement is True
    reducer.apply(TaskTerminalEvent(task_id="task-1", status="completed"))
    assert reducer.finish(settlement_expired=False).nested_state == "settled"


def test_missing_terminal_task_degrades_without_hanging() -> None:
    reducer = started_reducer()
    reducer.apply(ResultEvent(session_id="session-1", is_error=False, result="done"))
    result = reducer.finish(settlement_expired=True)
    assert result.outcome == "completed"
    assert result.nested_state == "unknown"
    assert "nested_terminal_event_missing" in result.warnings


def test_error_result_fails_even_when_nested_state_is_unknown() -> None:
    reducer = started_reducer()
    reducer.apply(ResultEvent(session_id="session-1", is_error=True, result=None, errors=("tool failed",)))
    assert reducer.finish(settlement_expired=True).outcome == "failed"
```

- [ ] **Step 6: Run the settlement tests red**

Run: `uv run pytest tests/unit/test_result_reducer.py -q`

Expected: the new settlement assertions fail.

- [ ] **Step 7: Implement task-ledger settlement**

Maintain active task IDs from start to terminal notification. A result sets the target outcome; an empty ledger is immediately settled, while a non-empty ledger requires the caller to keep reading until it empties or the settlement window expires.

- [ ] **Step 8: Run reducer tests and typechecking green**

Run: `uv run pytest tests/unit/test_result_reducer.py -q`

Run: `uv run mypy src/qworker tests/unit/test_result_reducer.py`

Expected: both commands pass.

- [ ] **Step 9: Commit Task 2**

```bash
git add src/qworker/events.py src/qworker/reducer.py tests/unit/test_result_reducer.py
git commit -m "feat: reduce auditor results and nested tasks"
```

### Task 3: Adapt SDK 1.0.13 behind the transport seam

**Files:**
- Create: `src/qworker/transport.py`
- Create: `src/qworker/qoder_sdk.py`
- Create: `tests/contract/test_qoder_sdk_transport.py`
- Create: `tests/unit/test_auth_diagnostics.py`

**Interfaces:**
- Consumes: `AuditContract`, `AuditorToolPolicy`, SDK 1.0.13 client/messages, and an optional injected SDK client.
- Produces: `QoderTransport` protocol and `QoderSDKTransport` implementation with `connect()`, `available_models()`, `select_model()`, `send()`, `messages()`, and `disconnect()`.

- [ ] **Step 1: Write the failing transport-order contract test**

```python
async def test_transport_initializes_selects_model_then_sends() -> None:
    client = FakeSDKClient(models=[{"value": "Qwen3.8-Max", "isEnabled": True}])
    transport = QoderSDKTransport(client)

    await transport.connect()
    models = await transport.available_models()
    await transport.select_model(models[0].value)
    await transport.send("audit this")

    assert client.calls == ["connect:none", "models", "model:Qwen3.8-Max", "query:audit this"]
```

- [ ] **Step 2: Run the transport-order test red**

Run: `uv run pytest tests/contract/test_qoder_sdk_transport.py::test_transport_initializes_selects_model_then_sends -q`

Expected: collection fails because the transport seam does not exist.

- [ ] **Step 3: Define the transport protocol and SDK adapter**

```python
class QoderTransport(Protocol):
    async def connect(self) -> None: ...
    async def available_models(self) -> tuple[AvailableModel, ...]: ...
    async def select_model(self, model: str) -> None: ...
    async def send(self, prompt: str) -> None: ...
    def messages(self) -> AsyncIterator[AdapterEvent]: ...
    async def disconnect(self) -> None: ...
```

`QoderSDKTransport` calls `connect(None)`, `get_available_models()`, `set_model()`, `query()`, `receive_messages()`, and `disconnect()` in that order. It is the only production module importing `qoder_agent_sdk`; it maps SDK messages to normalized events before yielding them.

- [ ] **Step 4: Run the transport-order test green**

Run: `uv run pytest tests/contract/test_qoder_sdk_transport.py::test_transport_initializes_selects_model_then_sends -q`

Expected: pass.

- [ ] **Step 5: Write failing option and permission-callback tests**

```python
async def test_nested_mutation_is_denied_at_callback_boundary(tmp_path: Path) -> None:
    options = build_auditor_options(tmp_path)
    context = ToolPermissionContext(agent_id="nested-1")
    decision = await options.can_use_tool("Write", {"file_path": "x"}, context)
    assert isinstance(decision, PermissionResultDeny)


def test_auditor_options_are_layered_and_isolated(tmp_path: Path) -> None:
    options = build_auditor_options(tmp_path, auth={"type": "access_token", "access_token": "opaque"})
    assert options.tools == ["Read", "Glob", "Grep", "WebFetch", "WebSearch", "Agent"]
    assert options.disallowed_tools == ["Write", "Edit", "Bash", "NotebookEdit"]
    assert options.permission_mode == "dontAsk"
    assert options.setting_sources == []
    assert options.cli_path is None
```

- [ ] **Step 6: Run the option tests red**

Run: `uv run pytest tests/contract/test_qoder_sdk_transport.py -q`

Expected: option construction and callback assertions fail.

- [ ] **Step 7: Implement layered options and message mapping**

Build options with the visible tools, mutation denylist, `dontAsk`, isolated setting sources, bounded turns, fail-closed callback, repo working directory, bundled runtime default, and injected authentication. Map text/tool blocks, task start/progress/notification, result metadata, permission denials, assistant models, and bounded system diagnostics.

- [ ] **Step 8: Write the failing authentication diagnostic tests**

```python
def test_missing_token_is_actionable_and_contains_no_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QODER_PERSONAL_ACCESS_TOKEN", raising=False)
    diagnostic = create_default_transport(Path.cwd()).diagnostic
    assert diagnostic.code == "auth_required"
    assert "QODER_PERSONAL_ACCESS_TOKEN" in diagnostic.message
    assert "token=" not in diagnostic.message


def test_error_redaction_removes_known_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QODER_PERSONAL_ACCESS_TOKEN", "secret-value")
    assert "secret-value" not in classify_sdk_error(RuntimeError("secret-value failed")).message
```

- [ ] **Step 9: Run auth tests red, then implement classification and redaction**

Run: `uv run pytest tests/unit/test_auth_diagnostics.py -q`

Classify missing token as `auth_required`, initialization timeout as `initialize_timeout`, missing runtime as `runtime_not_found`, and other SDK/protocol failures as `sdk_protocol_error`. Redact known credential environment values before exposing any message.

- [ ] **Step 10: Run all transport/auth tests green**

Run: `uv run pytest tests/contract/test_qoder_sdk_transport.py tests/unit/test_auth_diagnostics.py -q`

Run: `uv run mypy src/qworker tests/contract tests/unit`

Expected: both commands pass.

- [ ] **Step 11: Commit Task 3**

```bash
git add src/qworker/transport.py src/qworker/qoder_sdk.py tests/contract tests/unit/test_auth_diagnostics.py
git commit -m "feat: adapt qoder sdk for read-only audits"
```

### Task 4: Run the foreground auditor end to end

**Files:**
- Create: `src/qworker/auditor.py`
- Create: `tests/fakes.py`
- Create: `tests/test_foreground_auditor.py`
- Create: `tests/integration/test_real_qoder_auditor.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: `AuditContract` and a `QoderTransport` factory.
- Produces: `ForegroundAuditor.run(contract) -> AuditResult` and `run_foreground_audit(contract) -> AuditResult` using the real SDK transport by default.

- [ ] **Step 1: Write the failing end-to-end fake-transport test**

```python
async def test_foreground_auditor_returns_structured_completion(tmp_path: Path) -> None:
    transport = FakeQoderTransport.successful_audit(model="Qwen3.8-Max")
    result = await ForegroundAuditor(lambda _: transport).run(
        AuditContract(objective="audit the design", cwd=tmp_path)
    )

    assert result.outcome == "completed"
    assert result.resolved_model == "Qwen3.8-Max"
    assert result.nested_state == "settled"
    assert transport.calls[:4] == ["connect", "models", "model:Qwen3.8-Max", "send"]
    assert transport.disconnected is True
```

- [ ] **Step 2: Run the end-to-end test red**

Run: `uv run pytest tests/test_foreground_auditor.py::test_foreground_auditor_returns_structured_completion -q`

Expected: collection fails because the foreground runner does not exist.

- [ ] **Step 3: Implement prompt rendering and the run loop**

Render the stable role/objective/workspace/context/constraints/acceptance/report envelope. Connect, resolve, select, and send before consuming events. After the main result, return immediately when the task ledger is empty; otherwise keep reading for up to five seconds. Always disconnect in `finally`.

- [ ] **Step 4: Add delayed-terminal and timeout tests**

Use a fake transport that yields a result before the terminal task event and another that never yields the terminal event. Inject a short settlement timeout in tests and assert settled versus unknown nested state without sleeping for five seconds.

- [ ] **Step 5: Run foreground tests green**

Run: `uv run pytest tests/test_foreground_auditor.py -q`

Expected: pass.

- [ ] **Step 6: Add the credential-gated real integration test**

```python
@pytest.mark.real_qoder
async def test_real_qwen_auditor_is_read_only(tmp_path: Path) -> None:
    if "QODER_PERSONAL_ACCESS_TOKEN" not in os.environ:
        pytest.skip("QODER_PERSONAL_ACCESS_TOKEN is not configured")
    result = await run_foreground_audit(
        AuditContract(objective="Read the workspace and report its top-level files.", cwd=tmp_path)
    )
    assert result.outcome == "completed"
    assert result.resolved_model == "Qwen3.8-Max"
```

The test copies only a harmless fixture into `tmp_path`, remains excluded from the default suite, and documents that it consumes Qoder credits.

- [ ] **Step 7: Document opt-in execution and run the default suite**

Run: `uv run pytest -m 'not real_qoder' -q`

Run: `uv run ruff format --check src tests`

Run: `uv run ruff check src tests`

Run: `uv run mypy src tests`

Expected: all commands pass; the real test is not executed by default.

- [ ] **Step 8: Run the opt-in real audit only when credentials are present**

Run: `uv run pytest tests/integration/test_real_qoder_auditor.py -m real_qoder -q`

Expected with credentials: pass against Qwen3.8-Max. Expected without credentials: one explicit skip and no SDK process.

- [ ] **Step 9: Commit Tracer Bullet 1**

```bash
git add src/qworker/auditor.py tests/fakes.py tests/test_foreground_auditor.py tests/integration/test_real_qoder_auditor.py README.md
git commit -m "feat: run foreground qoder auditor"
```

## Tracer Bullet 2: Supervisor, Persistence, RPC, and Core CLI

### Task 5: Persist workers, attempts, and ordered semantic events

**Files:**
- Create: `src/qworker/lifecycle.py`
- Create: `src/qworker/store.py`
- Create: `tests/unit/test_lifecycle.py`
- Create: `tests/test_store.py`

**Interfaces:**
- Consumes: worker creation contracts and normalized semantic events.
- Produces: `WorkerRecord`, `AttemptRecord`, `WorkerStateReducer.transition()`, and async `WorkerStore` methods `create_worker`, `start_attempt`, `append_event`, `get_worker`, and `events_since`.

- [ ] Write one failing lifecycle transition test, run it red, implement the explicit transition table from spec section 8.1, and run it green.
- [ ] Write one failing SQLite round-trip test asserting WAL mode, attempt increments, monotonic per-worker sequences, and ordered cursor replay; run red, implement the store, and run green.
- [ ] Write permission-mode tests for `0700` state directory and owner-only database creation; run them green on POSIX.
- [ ] Run: `uv run pytest tests/unit/test_lifecycle.py tests/test_store.py -q`.
- [ ] Run: `uv run mypy src tests/unit/test_lifecycle.py tests/test_store.py`.
- [ ] Commit: `git commit -m "feat: persist worker lifecycle and events"`.

### Task 6: Expose spawn, status, watch, and result through the supervisor

**Files:**
- Create: `src/qworker/supervisor.py`
- Create: `src/qworker/rpc.py`
- Create: `src/qworker/cli.py`
- Create: `tests/test_supervisor_rpc.py`
- Create: `tests/test_cli.py`

**Interfaces:**
- Consumes: `WorkerStore`, worker transport factory, and NDJSON requests `{request_id, method, params}`.
- Produces: `Supervisor.spawn/status/watch/result`, ordered RPC responses, and `qworker` console entry point.

- [ ] Drive one worker from spawn acceptance through structured result using a fake transport; assert the ID returns before completion.
- [ ] Add watch replay and follow tests using persisted event cursors and two independent workers.
- [ ] Add JSON CLI tests for validation, connection failure, and accepted-versus-completed exit semantics.
- [ ] Implement the supervisor as sole owner of live worker tasks and clients; implement request correlation and streaming watch RPC.
- [ ] Run: `uv run pytest tests/test_supervisor_rpc.py tests/test_cli.py -q`.
- [ ] Demonstrate two concurrent fake workers and reconnecting watch cursors.
- [ ] Commit: `git commit -m "feat: supervise observable qoder workers"`.

## Tracer Bullet 3: Steering, Approvals, Stop, and Loss

### Task 7: Control live input and approvals

**Files:**
- Create: `src/qworker/control.py`
- Modify: `src/qworker/supervisor.py`
- Modify: `src/qworker/cli.py`
- Create: `tests/test_worker_control.py`

**Interfaces:**
- Consumes: live worker IDs, priorities, message UUIDs, and pending approval responses.
- Produces: `steer`, `cancel_message`, and `respond` supervisor/CLI operations plus persisted steering and approval events.

- [ ] Test each priority through a fake transport, including stable UUID creation and best-effort cancellation result preservation.
- [ ] Test live permission and elicitation callbacks entering `requires_action`, returning to `running`, and rejecting responses from lost attempts.
- [ ] Implement callback futures in supervisor memory and durable redacted display records in SQLite.
- [ ] Run: `uv run pytest tests/test_worker_control.py -q` and `uv run mypy src tests/test_worker_control.py`.
- [ ] Commit: `git commit -m "feat: steer workers and resolve approvals"`.

### Task 8: Stop workers and classify process loss

**Files:**
- Modify: `src/qworker/supervisor.py`
- Modify: `src/qworker/lifecycle.py`
- Modify: `src/qworker/cli.py`
- Create: `tests/test_stop_and_loss.py`

**Interfaces:**
- Consumes: normal/forced stop requests and transport EOF/process-exit events.
- Produces: cancelled or lost attempts, expired approvals, exited health, and preserved structured terminal results.

- [ ] Test graceful stop ordering: interrupt, bounded wait, disconnect, deny approvals, terminal transition.
- [ ] Test forced stop skips the grace period and preserves existing workspace files.
- [ ] Test unexpected EOF before result becomes `lost`, while EOF after a result preserves result-derived completion.
- [ ] Implement stop and loss handling without automatic coder retry.
- [ ] Run: `uv run pytest tests/test_stop_and_loss.py -q`.
- [ ] Commit: `git commit -m "feat: stop workers and report loss"`.

## Tracer Bullet 4: Model Policy, Authentication Preflight, and Resume

### Task 9: Add configuration and runtime preflight

**Files:**
- Create: `src/qworker/config.py`
- Create: `src/qworker/preflight.py`
- Modify: `src/qworker/cli.py`
- Create: `tests/test_preflight.py`

**Interfaces:**
- Consumes: user/project TOML, environment authentication, bundled/external runtime selection, and live model catalog.
- Produces: narrowed effective configuration and `DoctorResult` with stable error/warning codes.

- [ ] Test user configuration plus project narrowing, rejecting unauthorized project permission expansion.
- [ ] Test auth selection order: PAT, service account, then optional local-login reuse.
- [ ] Test bundled runtime default and explicit external runtime version recording.
- [ ] Test `qodercli_auth` initialization timeout classification with `qodercli_auth_reuse_failed` and no secret values.
- [ ] Implement `doctor` and spawn preflight without private SDK initialization hooks.
- [ ] Run: `uv run pytest tests/test_preflight.py -q` and `uv run mypy src tests/test_preflight.py`.
- [ ] Commit: `git commit -m "feat: preflight qoder runtime and authentication"`.

### Task 10: Resume a lost conversation as a new attempt

**Files:**
- Modify: `src/qworker/supervisor.py`
- Modify: `src/qworker/qoder_sdk.py`
- Modify: `src/qworker/cli.py`
- Create: `tests/test_resume.py`

**Interfaces:**
- Consumes: eligible worker ID with stored session ID and original canonical working directory.
- Produces: incremented attempt using new SDK process configured with `resume=<session_id>` and a recovery message.

- [ ] Test eligibility for lost, failed, and cancelled workers and explicit rejection for live/completed/no-session workers.
- [ ] Test original working directory verification, attempt increment, new transport construction, and recovery prompt contents.
- [ ] Test pending callbacks and queued messages are not fabricated or replayed across attempts.
- [ ] Implement resume as respawn-with-history and expose it through CLI/RPC.
- [ ] Run: `uv run pytest tests/test_resume.py -q`.
- [ ] Commit: `git commit -m "feat: resume qoder conversation in new attempt"`.

## Tracer Bullet 5: Coder Contract and Shared-Writer Visibility

### Task 11: Run coder workers in the caller workspace

**Files:**
- Create: `src/qworker/coder.py`
- Create: `src/qworker/workspace.py`
- Modify: `src/qworker/supervisor.py`
- Create: `tests/test_coder.py`
- Create: `tests/test_workspace_overlap.py`

**Interfaces:**
- Consumes: coder work contract and live shared-workspace workers.
- Produces: coder prompt/policy, structured changed-file and validation report, and persisted overlap warnings.

- [ ] Test canonical equal, ancestor, descendant, sibling, and symlink-resolved working-directory overlap cases.
- [ ] Test warning visibility without spawn rejection, worktree creation, lock creation, or Git normalization.
- [ ] Run a fake coder through edit/validation/result reporting and an opt-in real coder only in a disposable directory.
- [ ] Implement coder policy with `tools="default"`, `acceptEdits`, dangerous bypass disabled, and explicit setting sources.
- [ ] Run: `uv run pytest tests/test_coder.py tests/test_workspace_overlap.py -q`.
- [ ] Commit: `git commit -m "feat: run shared-workspace coder workers"`.

## Tracer Bullet 6: Codex-Facing Qoder Worker Skill

### Task 12: Package stable worker workflows as a skill

**Files:**
- Create: `skills/qoder-worker/SKILL.md`
- Create: `skills/qoder-worker/references/contract.md`
- Create: `tests/test_skill_contract.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: user intent and stable `qworker --json` commands.
- Produces: role inference, proactive activation, prompt-file usage, lifecycle monitoring, and mutation-aware fallback guidance.

- [ ] Test review/implementation/explicit/ambiguous role inference examples against the skill contract.
- [ ] Test proactive auditor allowance and proactive coder project opt-in.
- [ ] Test fallback messaging before and after possible workspace mutation.
- [ ] Write the skill with compact context pointers and JSON-only machine interaction.
- [ ] Run: `uv run pytest tests/test_skill_contract.py -q`.
- [ ] Commit: `git commit -m "feat: add qoder worker skill"`.

## Tracer Bullet 7: Failure Injection, Security, and Real Integration

### Task 13: Exercise deterministic failures and security invariants

**Files:**
- Create: `tests/failure/test_runtime_failures.py`
- Create: `tests/failure/test_security_enforcement.py`
- Create: `tests/failure/test_redaction.py`
- Modify: `src/qworker/qoder_sdk.py`
- Modify: `src/qworker/supervisor.py`

**Interfaces:**
- Consumes: injected auth/runtime/protocol/EOF/database/socket/callback failures and adversarial tool requests.
- Produces: stable errors, fail-closed decisions, redacted diagnostics, and valid lifecycle transitions.

- [ ] Cover every stable error code from design section 13 with a deterministic fixture.
- [ ] Attempt write, edit, notebook, shell, unknown, extension, and nested-agent tools through the actual callback seam.
- [ ] Seed secrets in environment, exception text, tool arguments, and raw events; assert absence from normal outputs and persistence.
- [ ] Run: `uv run pytest tests/failure -q` and `uv run mypy src tests/failure`.
- [ ] Commit: `git commit -m "test: inject qworker failures and security attacks"`.

### Task 14: Prove every V1 acceptance criterion end to end

**Files:**
- Create: `tests/integration/test_real_worker_lifecycle.py`
- Create: `tests/integration/test_real_auditor_security.py`
- Create: `tests/integration/test_real_resume.py`
- Create: `docs/research/qworker-v1-acceptance.md`

**Interfaces:**
- Consumes: installed SDK/runtime, opt-in credentials, disposable workspaces, and the public CLI.
- Produces: traceable evidence for all fourteen V1 acceptance criteria.

- [ ] Run two workers concurrently and control them from later CLI processes.
- [ ] Verify cursor replay, Qwen3.8-Max requested/resolved/actual reporting, direct-subagent telemetry/transcript, all steering priorities, cancellation, approval response, stop, loss, and explicit resume.
- [ ] Verify a disposable coder edit and actual callback denial of auditor/helper write and shell attempts.
- [ ] Verify no child process remains after stop and no credential value appears in arguments, persistence, events, or logs.
- [ ] Run default verification: `uv run pytest -m 'not real_qoder' -q`, `uv run ruff format --check src tests`, `uv run ruff check src tests`, and `uv run mypy src tests`.
- [ ] Run credential-gated verification: `uv run pytest tests/integration -m real_qoder -q`.
- [ ] Record each acceptance criterion, command, environment version, and observed evidence in the acceptance report.
- [ ] Commit: `git commit -m "test: prove qworker v1 acceptance criteria"`.

## Plan Self-Review

- Every design implementation-sequence bullet maps to one named tracer-bullet section.
- Ticket #1 is fully covered by Tasks 1–4, including model resolution, result/task settlement, nested read-only policy, safe authentication diagnostics, real-test gating, and a planned-files-only diff.
- Later tasks retain explicit unsupported-operation and recovery semantics rather than adding nonexistent SDK capabilities.
- All production SDK imports remain localized to `qoder_sdk.py`; normalized domain events cross the transport seam.
- No task requires a real credential for default verification.
