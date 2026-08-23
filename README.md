# dogqoder — Qoder workers for Codex

`dogqoder` gives Codex persistent, observable, steerable Qoder workers while
Codex continues using its native OpenAI provider. It is designed for work that
benefits from Qoder's model catalog—especially `Qwen3.8-Max`—without switching
the main Codex session to another provider.

The project is currently at the approved-design stage. The SDK integration has
been proven against a real Qoder session; the production supervisor and CLI are
the next implementation work.

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

## Core mechanisms

| Mechanism | V1 behavior |
| --- | --- |
| Worker ownership | One `QoderSDKClient` and QoderCLI process per live top-level worker |
| Steering | SDK priorities `now`, `next`, and `later`; queued messages use UUIDs |
| Nested visibility | Task events plus persisted Qoder subagent transcripts |
| Nested control | Observe only; Python SDK 1.0.13 cannot steer or stop one selected nested agent |
| Recovery | Explicit respawn with `resume=<session_id>` and the original cwd |
| Auditor safety | Tool visibility, denylist, `dontAsk`, and fail-closed permission callback |
| Coder workspace | Direct shared-workspace edits; qworker warns about concurrent writers |
| Completion | Main result plus nested settlement; missing terminal telemetry becomes an explicit warning |
| Runtime | SDK-bundled QoderCLI by default; external runtime is an explicit tested override |

## Planned directory structure

```text
dogqoder/
├── src/qworker/           # CLI, supervisor, domain model, SDK adapter
├── tests/                 # Unit, adapter-contract, and integration tests
├── docs/research/         # Empirical SDK findings
├── docs/superpowers/specs/# Accepted design specifications
├── docs/superpowers/plans/# Executable implementation plans
├── spikes/                # Disposable or reproducible feasibility probes
├── HANDOFF.md             # Fresh-session implementation prompt
├── pyproject.toml
└── uv.lock
```

Implementation code and tests are intentionally absent until the approved
design has been converted into a tracer-bullet implementation plan.

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

The first tracer bullet is a foreground, single-worker, read-only auditor using
the public SDK. It must prove initialization, model resolution, event reduction,
and structured completion before the daemon and full CLI are added.

## Technical stack

Python 3.12 · asyncio · qoder-agent-sdk 1.0.13 · SQLite · uv · pytest
