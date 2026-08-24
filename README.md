# dogqoder — Qoder workers for Codex

`dogqoder` gives Codex persistent, observable, steerable Qoder workers while
Codex continues using its native OpenAI provider. It is designed for work that
benefits from Qoder's model catalog—especially `Qwen3.8-Max`—without switching
the main Codex session to another provider.

The current implementation provides a persistent local supervisor, stable JSON
CLI, read-only auditors, shared-workspace coders, and a model-invoked Codex
skill. Failure injection and V1 acceptance hardening are complete under the
documented direct-subagent telemetry waiver.

## Goals

- A `coder` worker receives a clear contract, edits the shared workspace, and
  owns relevant linting, type checking, and tests.
- An `auditor` worker independently reviews code or designs through a layered
  read-only boundary.
- Every top-level worker has a stable ID, lifecycle, health, task telemetry,
  approvals, steering, and a structured result.
- Qoder remains responsible for its own reasoning and direct internal-agent
  harness. Codex controls only the top-level worker.
- Shared workspaces are the default. Worktrees and exclusive write locks are
  optional external coordination choices.

## Architecture

```text
Codex
  │ invokes
  ▼
$qoder-worker skill
  │ stable JSON contract
  ▼
qworker CLI
  │ local NDJSON RPC
  ▼
asyncio supervisor ── SQLite worker/event store
  │
  ├── QoderSDKClient worker ── QoderCLI ── direct Qoder subagents
  └── QoderSDKClient worker ── QoderCLI ── direct Qoder subagents
```

The supervisor owns live SDK clients and callback futures. If it dies, the
system reports the worker as lost; recovery starts a new QoderCLI process with
the saved session ID and conversation history. It does not pretend to reattach
to or continue an interrupted process.

## Documentation

| Document | Purpose |
| --- | --- |
| [`docs/superpowers/specs/2026-08-24-qoder-worker-design.md`](docs/superpowers/specs/2026-08-24-qoder-worker-design.md) | Normative V1 architecture, contracts, lifecycle, security, tests, and acceptance criteria |
| [`docs/research/2026-08-24-qoder-sdk-feasibility.md`](docs/research/2026-08-24-qoder-sdk-feasibility.md) | Results from the real SDK/Qwen feasibility audit and the corrections it forced |
| [`skills/qoder-worker/SKILL.md`](skills/qoder-worker/SKILL.md) | Model-invoked Codex policy for selecting, starting, observing, and recovering Qoder workers |
| [`skills/qoder-worker/references/contract.md`](skills/qoder-worker/references/contract.md) | Stable JSON-only `qworker` command and lifecycle reference used by the skill |
| [`spikes/qoder_audit_probe.py`](spikes/qoder_audit_probe.py) | Read-only executable probe that starts a Qoder auditor and observes its direct subagent |
| [`HANDOFF.md`](HANDOFF.md) | Copy-ready prompt for continuing implementation in a fresh Codex session |

The design specification is authoritative. Research records evidence and may
describe limitations of a particular SDK version.

## Quick start

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/), and Qoder
credentials accepted by the Agent SDK.

```bash
git clone git@github.com:proletariat64/dogqoder.git
cd dogqoder
uv sync
uv run python -c "import qoder_agent_sdk; print('Qoder SDK ready')"
```

For unattended SDK sessions, configure `QODER_PERSONAL_ACCESS_TOKEN` in the
calling environment. The value must never be committed or written to project
configuration.

Mainland users can select a mirror without changing the lockfile:

```bash
uv sync --default-index https://mirrors.aliyun.com/pypi/simple/
```

The feasibility probe performs a real, credit-consuming Qoder request:

```bash
uv run python spikes/qoder_audit_probe.py
```

## Foreground auditor verification

Default tests never contact Qoder and exclude the `real_qoder` marker:

```bash
uv run pytest -q
```

The real integration uses the SDK-bundled QoderCLI, copies one harmless fixture
to a disposable pytest workspace, enforces the read-only auditor policy, and
consumes Qoder credits. Export `QODER_PERSONAL_ACCESS_TOKEN` without writing its
value to repository configuration, then opt in explicitly:

```bash
uv run pytest tests/integration/test_real_qoder_auditor.py -m real_qoder -q
```

## Codex worker skill

The model-invoked [`qoder-worker` skill](skills/qoder-worker/SKILL.md) maps
explicit delegation and eligible proactive work to the public `qworker` CLI.
Review and verification intent selects a read-only auditor, implementation
intent selects a coder, and ambiguity stays read-only. Independent audits may
activate proactively; proactive coding requires effective project policy to
enable it. All lifecycle interaction consumes JSON, with mutation-aware
fallback once a coder may have touched the shared workspace.

## Core mechanisms

| Mechanism | V1 behavior |
| --- | --- |
| Worker ownership | One `QoderSDKClient` and QoderCLI process per live top-level worker |
| Steering | SDK priorities `now`, `next`, and `later`; queued messages use UUIDs |
| Nested visibility | Task events when emitted; current SDK/CLI cannot provide correlatable post-run direct-helper transcript discovery |
| Nested control | Observe only; Python SDK 1.0.13 cannot steer or stop one selected nested agent |
| Recovery | Explicit respawn with `resume=<session_id>` and the original cwd |
| Auditor safety | Tool visibility, denylist, `dontAsk`, and fail-closed permission callback |
| Coder workspace | Direct shared-workspace edits; qworker warns about concurrent writers |
| Completion | Main result plus nested settlement; missing terminal telemetry becomes an explicit warning |
| Runtime | SDK-bundled QoderCLI by default; external runtime is an explicit tested override |

## Project structure

```text
dogqoder/
├── src/qworker/           # CLI, supervisor, domain model, SDK adapter
├── skills/qoder-worker/   # Codex policy plus disclosed CLI contract
├── tests/                 # Unit, adapter-contract, and integration tests
├── docs/research/         # Empirical SDK findings
├── docs/superpowers/specs/# Accepted design specifications
├── docs/superpowers/plans/# Executable implementation plans
├── spikes/                # Disposable or reproducible feasibility probes
├── HANDOFF.md             # Fresh-session implementation prompt
├── pyproject.toml
└── uv.lock
```

The supervisor, persistence, RPC, JSON CLI, auditor and coder paths, Codex
skill, failure coverage, and V1 acceptance evidence now exist under
`src/qworker`, `tests`, `skills`, and `docs/research`.

## Agent workflow

```text
read README
→ read the normative design
→ read feasibility corrections
→ write the implementation plan
→ implement one end-to-end tracer bullet at a time
→ run unit and real credential-gated integration tests
→ verify every acceptance criterion
```

Completed tracer bullets cover initialization, model resolution, event
reduction, durable supervision and control, explicit recovery, structured
auditor and coder completion, failure injection, and bounded real-Qoder
acceptance evidence.

## Technical stack

Python 3.12 · asyncio · qoder-agent-sdk 1.0.13 · SQLite · uv · pytest
