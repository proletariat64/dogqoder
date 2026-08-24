# Qoder Worker Task 9 Preflight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven development. This task is being executed inline because the parent worker explicitly prohibited subagents. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic configuration loading, Qoder runtime/authentication preflight, `qworker doctor`, and spawn-time preflight without exposing credentials.

**Architecture:** `config.py` parses a closed TOML schema and combines user policy with project policy through an explicit narrowing check. `preflight.py` owns SDK-independent auth ordering and result/diagnostic types behind an injected backend; `qoder_sdk.py` implements that backend only with public SDK connection APIs. CLI doctor calls the same runner used by production supervisor spawn, while existing supervisor tests can omit preflight and remain deterministic.

**Tech Stack:** Python 3.12, `tomllib`, asyncio, qoder-agent-sdk 1.0.13, pytest, Ruff, strict mypy.

**Spec:** `docs/superpowers/specs/2026-08-24-qoder-worker-design.md` sections 11 and 15; `.superpowers/sdd/2026-08-24-qoder-worker-v1/task-9-brief.md`

## Global Constraints

- User configuration path is `~/.config/qworker/config.toml`.
- Project `.qworker.toml` may contain policy only and may not expand user policy unless `[project].allow_expansion = true` appears in user configuration.
- Authentication priority is PAT through `access_token_from_env()`, explicitly configured service account, then optional `qodercli_auth()` reuse.
- Preflight uses public `connect(None)`, `get_server_info()`, and `disconnect()` APIs; no private initialization reader.
- Credential values never enter results, errors, events, persistence, logs, or command arguments.
- Tests use fakes only; no real Qoder credentials, network, credits, or live QoderCLI.

---

### Task 1: Closed user/project configuration

**Files:**
- Create: `src/qworker/config.py`
- Test: `tests/test_preflight.py`

**Interfaces:**
- Produces: `EffectiveConfig`, `PolicyConfig`, `ConfigError`, `default_user_config_path()`, and `load_config(cwd, user_path=...)`.
- Policy fields: proactive auditor/coder booleans, coder permission mode, coder denied-tool tuple, and auditor web access.

- [ ] **Step 1: Write project narrowing tests**

```python
config = load_config(project, user_path=user_file)
assert config.policy.proactive_coder is False
assert config.policy.coder_permission_mode == "dontAsk"
assert config.policy.coder_denied_tools == ("Bash", "Write")
```

- [ ] **Step 2: Run focused test and verify RED**

Run: `uv run pytest tests/test_preflight.py -q`
Expected: import failure because `qworker.config` does not exist.

- [ ] **Step 3: Implement closed TOML parsing and narrowing relation**

```python
@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    runtime_path: Path | None
    service_account_env: str | None
    reuse_qodercli_auth: bool
    allow_project_expansion: bool
    policy: PolicyConfig
```

Reject unknown tables/keys, direct credential values, relative runtime paths, invalid environment names, and project runtime/auth settings. Treat disabling proactive/web behavior, lowering permission mode, and adding denied tools as narrowing.

- [ ] **Step 4: Run focused test and verify GREEN**

Run: `uv run pytest tests/test_preflight.py -q`
Expected: configuration cases pass.

### Task 2: Runtime/auth/control preflight

**Files:**
- Create: `src/qworker/preflight.py`
- Modify: `src/qworker/qoder_sdk.py`
- Test: `tests/test_preflight.py`

**Interfaces:**
- Produces: `AuthSelection`, `RuntimeInfo`, `DoctorResult`, `PreflightFailure`, `PreflightBackend`, `RuntimePreflight.run()`, and `select_auth()`.
- SDK adapter produces: `QoderPreflightBackend` and configured production transport construction.

- [ ] **Step 1: Write auth-order and runtime-selection tests through fake backend**

```python
result = await RuntimePreflight(fake, environ=env, user_path=user_file).run(project)
assert result.auth_source == "personal_access_token"
assert result.runtime_path == "bundled"
assert result.runtime_version == "1.1.23"
```

- [ ] **Step 2: Run cases and verify RED**

Run: `uv run pytest tests/test_preflight.py -q`
Expected: missing preflight public interfaces.

- [ ] **Step 3: Implement selection and public SDK backend**

Use only environment-variable names in `AuthSelection`. Backend translates the selection with `access_token_from_env()`, `service_account_from_env()`, or `qodercli_auth()`, constructs options with resolved `cli_path`, calls `connect(None)`, reads `get_server_info()`, and disconnects. Runtime probe executes only `[runtime, "-v"]`; interactive diagnostic executes only `[runtime, "status"]` and discards output.

- [ ] **Step 4: Write timeout/redaction regression**

```python
assert result.error.code == "initialize_timeout"
assert result.warnings == ("qodercli_auth_reuse_failed",)
assert secret not in json.dumps(result.to_json())
```

- [ ] **Step 5: Implement bounded secret-safe diagnostics and verify GREEN**

Run: `uv run pytest tests/test_preflight.py -q`
Expected: auth/runtime/control and redaction cases pass.

### Task 3: Doctor and spawn integration

**Files:**
- Modify: `src/qworker/cli.py`
- Modify: `src/qworker/supervisor.py`
- Modify: `src/qworker/rpc.py`
- Test: `tests/test_preflight.py`

**Interfaces:**
- CLI produces: finite `doctor --json` result with exit 0 on success and 1 on failed preflight.
- Supervisor consumes: optional injected preflight runner; production server always supplies it.
- Spawn persists SDK/runtime metadata returned by successful preflight and rejects failed preflight with its stable code.

- [ ] **Step 1: Write doctor and supervisor-spawn failing tests**

```python
exit_code = await run(["doctor", "--json"], stdin=StringIO(), stdout=stdout, doctor_runner=fake)
assert exit_code == 0
assert json.loads(stdout.getvalue())["ok"] is True
```

- [ ] **Step 2: Run cases and verify RED**

Run: `uv run pytest tests/test_preflight.py -q`
Expected: CLI does not recognize doctor and Supervisor has no preflight seam.

- [ ] **Step 3: Implement minimal wiring and verify GREEN**

Run: `uv run pytest tests/test_preflight.py -q`
Expected: doctor and spawn integration pass without live SDK calls.

### Task 4: Verification, report, and commit

**Files:**
- Create: `.superpowers/sdd/2026-08-24-qoder-worker-v1/task-9-report.md`

- [ ] **Step 1: Run focused and relevant full tests**

Run: `uv run pytest tests/test_preflight.py tests/unit/test_auth_diagnostics.py tests/test_cli.py tests/test_supervisor_rpc.py -q`

Run: `uv run pytest -q`

- [ ] **Step 2: Run static and diff checks**

Run: `uv run ruff check .`

Run: `uv run mypy src tests/test_preflight.py`

Run: `git diff --check`

- [ ] **Step 3: Refresh graph and review scope**

Run: `code-review-graph update --brief`

Run: `code-review-graph detect-changes --base main --brief`

- [ ] **Step 4: Record RED/GREEN evidence and caveats in report**

Document exact commands, failures before implementation, passing results, Context7 miss/local pinned-SDK evidence, public API usage, redaction controls, and skipped real-Qoder tests.

- [ ] **Step 5: Commit**

```bash
git add src/qworker/config.py src/qworker/preflight.py src/qworker/qoder_sdk.py src/qworker/cli.py src/qworker/supervisor.py src/qworker/rpc.py tests/test_preflight.py docs/superpowers/plans/2026-08-24-qoder-worker-task-9.md .superpowers/sdd/2026-08-24-qoder-worker-v1/task-9-report.md
git commit -m "feat: preflight qoder runtime and authentication"
```
