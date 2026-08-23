# Qoder Worker for Codex — Design Specification

Status: approved for implementation
Date: 2026-08-24
Initial verified versions: `qoder-agent-sdk==1.0.13`, SDK-bundled QoderCLI `1.1.23`, external QoderCLI `1.1.28`

## 1. Purpose

Qoder Worker gives Codex persistent, observable, steerable Qoder sessions that feel like top-level subagents while retaining Qoder's native model and agent harness. It allows one Codex session to keep using OpenAI models while delegating selected work to Qoder models such as `Qwen3.8-Max`.

The system provides two first-class worker roles:

- `coder`: receives a precise implementation contract, works directly in the caller's workspace, and owns relevant linting, type checking, and tests.
- `auditor`: independently examines requirements, code, or a design through a read-only tool boundary and returns structured findings from a different model family.

Qoder controls how its worker reasons and how it uses its direct internal subagents. Codex controls only the top-level worker lifecycle, shared-workspace coordination, steering messages, approvals, and result consumption.

## 2. Goals

V1 shall:

1. Spawn more than one independent Qoder worker from a Codex session.
2. Return a stable worker ID immediately and expose lifecycle, health, task, approval, model, and result state afterward.
3. Keep each worker alive across separate `qworker` CLI invocations while the supervisor remains alive.
4. Permit steering with Qoder SDK priorities `now`, `next`, and `later`.
5. Permit responses to live permission and elicitation requests.
6. Preserve Qoder's direct internal-subagent behavior and expose the telemetry Qoder emits about it.
7. Use the caller's workspace directly, without requiring a worktree or an exclusive write lock.
8. Enforce an auditor boundary that denies filesystem mutation and shell execution, including calls originating from nested agents.
9. Persist enough controller state to diagnose loss and explicitly resume the Qoder conversation in a new process.
10. Expose stable machine-readable output suitable for a Codex skill.

## 3. Non-goals

V1 does not provide:

- ACP integration.
- A general abstraction over every agent provider.
- Reattachment to a dead or detached QoderCLI process.
- Transparent continuation of an in-flight turn after supervisor failure.
- Direct steering, interruption, or cancellation of a specific Qoder nested agent.
- Nested-agent recursion beyond the depth supported by Qoder.
- A default worktree, container, sandbox, or workspace write lock.
- Automatic rollback of worker edits.
- A web dashboard, remote supervisor API, or multi-user service.
- Automatic replacement of a worker with Codex after the worker has modified the workspace.

## 4. Verified SDK constraints

The design treats the following as runtime constraints, not assumptions:

- `QoderSDKClient` owns one stdio-connected QoderCLI subprocess.
- `query()` supports `now`, `next`, and `later` delivery priorities, stable message UUIDs, and `should_query`.
- `cancel_async_message()` only guarantees removal while a message remains queued.
- `interrupt()` applies to the active top-level turn, not a selected nested task.
- Python SDK 1.0.13 exposes `TaskStartedMessage`, `TaskProgressMessage`, and `TaskNotificationMessage`, but no public `backgroundTasks` or `stopTask` method.
- `receive_response()` ends at the first `ResultMessage`; V1 must use `receive_messages()` to maintain task state around the result boundary.
- Permission and elicitation callbacks are live in-process exchanges. They cannot be reconstructed after the stdio process or supervisor dies.
- Session and subagent transcripts are persisted by Qoder and can be inspected with `list_subagents()` and `get_subagent_messages()`.
- A new process may resume conversation history with `resume=<session_id>`. This is respawn-with-history, not process reattachment.
- Qoder subagents cannot recursively spawn additional subagents.
- SDK-local CLI authentication may fail even when interactive `qodercli status` succeeds. Personal access-token authentication must be supported and preflighted.
- The SDK prefers its bundled QoderCLI runtime unless `cli_path` or `QODERCLI_PATH` overrides it.

The successful feasibility probe used `Qwen3.8-Max`, spawned one internal Qoder subagent, observed task events, and completed without invoking a write-capable tool.

## 5. System architecture

```text
Codex
  |
  | invokes and interprets
  v
$qoder-worker skill
  |
  | stable CLI/JSON contract
  v
qworker CLI
  |
  | local NDJSON RPC over Unix socket
  v
qworker supervisor (single asyncio process)
  |-- SQLite worker/event store
  |-- approval and steering broker
  |-- model catalog/cache
  |
  +-- worker task A --> QoderSDKClient --> QoderCLI process --> Qoder direct subagents
  +-- worker task B --> QoderSDKClient --> QoderCLI process --> Qoder direct subagents
```

The boundaries are deliberate:

- The skill decides when and why to delegate, infers a role, and translates Codex intent into the CLI contract.
- The CLI is a thin client. It never owns a Qoder process.
- The supervisor is the sole owner of live SDK clients, callback futures, event ordering, and child processes.
- Each worker task contains one SDK client and remains in one asyncio context for its entire lifetime.
- Qoder remains responsible for prompting, reasoning, tool sequencing, and direct internal-subagent orchestration inside the worker.

## 6. Components

### 6.1 `$qoder-worker` skill

The skill provides the Codex-facing policy and command vocabulary.

Activation policy:

- Explicit user invocation is always allowed.
- Codex may proactively spawn an `auditor` when an independent model perspective materially improves review, risk analysis, or design validation.
- Codex may proactively spawn a `coder` only when project configuration enables proactive Qoder coding.
- If initial worker creation fails before any workspace mutation, the skill may report the failure and offer or use Codex as the fallback when the active policy permits it.
- Once a Qoder worker may have modified the workspace, replacement is never silent. Codex reports the partial-worker state before continuing itself or launching another writer.

Role inference:

- Review, audit, verify, cross-check, challenge, or second-opinion intent selects `auditor`.
- Implement, fix, refactor, test, or code intent selects `coder`.
- An explicit `--role` or user instruction overrides inference.
- Ambiguous requests default to `auditor`, because it is non-mutating.

The skill consumes JSON output. It does not parse human-formatted CLI text.

### 6.2 `qworker` CLI

`qworker` translates commands into local supervisor RPC and renders either concise human output or stable JSON.

Commands:

```text
qworker spawn
qworker list
qworker status WORKER_ID
qworker watch WORKER_ID
qworker steer WORKER_ID
qworker cancel-message WORKER_ID MESSAGE_UUID
qworker respond WORKER_ID REQUEST_ID
qworker stop WORKER_ID
qworker resume WORKER_ID
qworker result WORKER_ID
qworker doctor
```

Common behavior:

- `--json` returns one JSON object for finite commands.
- `watch --json` emits one JSON object per line.
- Exit code `0` means the RPC command was accepted, not that an asynchronous worker completed.
- Validation and connection failures use nonzero exit codes and structured errors on stdout when `--json` is present.
- Large prompts are accepted through `--prompt-file`, `--spec-file`, or stdin. Secrets and full prompts are not placed in process arguments.
- The CLI auto-starts the supervisor for `spawn`, unless `--no-start-supervisor` is supplied.

### 6.3 Supervisor

The supervisor is a local single-user Python 3.12 asyncio service. It owns:

- one asyncio worker task per live Qoder worker;
- one `QoderSDKClient` per worker attempt;
- local RPC connections;
- SQLite state and semantic events;
- approval futures;
- steering queues and UUIDs;
- Qoder model-catalog caching;
- child-process liveness and idle-time health inference.

The supervisor listens on `$XDG_RUNTIME_DIR/qworker/qworker.sock`, falling back to `~/.local/state/qworker/qworker.sock`. The state directory has mode `0700`; the socket and database have owner-only access.

RPC uses newline-delimited JSON. Each finite request carries `request_id`, `method`, and `params`; each response carries the same `request_id`, `ok`, and either `result` or `error`. `watch` upgrades the connection to an ordered event stream after its initial response.

### 6.4 Qoder SDK adapter

The adapter is the only component that imports `qoder_agent_sdk`. It converts SDK messages and callbacks into domain events and shields the rest of the system from SDK-version differences.

Responsibilities:

- construct `QoderAgentOptions` from a validated worker contract;
- establish authentication and initialize the runtime;
- discover and select a model before sending the first work prompt;
- consume `receive_messages()` continuously;
- reduce task events, results, tool permissions, elicitations, process exits, and raw capability-gated events;
- expose `steer`, `cancel_message`, `respond`, `interrupt`, `disconnect`, and `resume` operations to the supervisor;
- record runtime and SDK capabilities so unsupported operations fail explicitly.

The adapter defaults to the SDK-bundled QoderCLI. An external CLI path is allowed only through configuration and is recorded in every worker attempt.

### 6.5 Persistence

SQLite in WAL mode stores controller state at `~/.local/state/qworker/qworker.db`.

Qoder remains the source of truth for its conversation transcripts. The supervisor database is the source of truth for qworker lifecycle, event ordering, approvals, steering records, attempts, and warnings.

Semantic events are persisted. Token deltas and raw stdout are not persisted by default. This keeps the event log useful and prevents it from becoming a second transcript store.

## 7. Domain model

### 7.1 Worker

A worker is the stable top-level identity exposed to Codex. Restarting or resuming creates a new attempt under the same worker ID.

Required fields:

```text
worker_id                 ULID
role                      coder | auditor
cwd                       canonical absolute path
state                     starting | running | requires_action |
                          completed | failed | cancelled | lost
health                    unknown | healthy | quiet | stalled | exited
write_capability          shared_workspace | read_only
requested_model           exact model name or logical alias
resolved_model            selected catalog entry
actual_models             models reported by messages/result usage
session_id                explicit Qoder session UUID
attempt                    positive integer
runtime_path              bundled marker or absolute CLI path
runtime_version           detected version
sdk_version               installed SDK version
created_at                timestamp
started_at                optional timestamp
last_event_at             optional timestamp
ended_at                  optional timestamp
result_summary            optional structured result
nested_state              none | active | settled | unknown
warnings                  ordered warning codes
```

### 7.2 Event

Every worker event has:

```text
sequence      monotonically increasing per worker
event_id      ULID
worker_id     ULID
attempt       integer
timestamp     UTC timestamp
type          stable qworker event type
payload       versioned JSON object
```

Event types include:

- `worker.created`
- `worker.state_changed`
- `worker.health_changed`
- `runtime.started`
- `runtime.initialized`
- `model.resolved`
- `assistant.message`
- `tool.started`
- `tool.finished`
- `nested.started`
- `nested.progress`
- `nested.terminal`
- `approval.requested`
- `approval.resolved`
- `steer.queued`
- `steer.delivered`
- `steer.cancelled`
- `result.received`
- `worker.warning`
- `runtime.exited`

Unknown SDK messages are recorded as bounded diagnostic events only when debug logging is enabled. They never silently change lifecycle state.

### 7.3 Approval

An approval is a live bridge to `can_use_tool`, `on_elicitation`, or MCP OAuth handling.

```text
request_id        ULID
worker_id         ULID
attempt           integer
kind              tool_permission | elicitation | mcp_oauth
agent_id          optional nested-agent identifier
prompt            redacted, user-displayable request
choices           allowed response shape
status            pending | allowed | denied | answered | expired
created_at        timestamp
resolved_at       optional timestamp
```

The database record is durable for visibility; the callback future is not. If the supervisor or runtime dies, all pending approvals become `expired` and resolve fail-closed when resolution is still possible. A resumed turn receives no fabricated answer and must ask again.

## 8. Lifecycle and health

Lifecycle and health are separate.

### 8.1 State transitions

```text
starting -> running
starting -> failed

running -> requires_action -> running
running -> completed
running -> failed
running -> cancelled
running -> lost

requires_action -> cancelled
requires_action -> lost

lost -> starting       explicit resume; attempt increments
failed -> starting     explicit resume; attempt increments
cancelled -> starting  explicit resume; attempt increments
```

`completed`, `failed`, and `cancelled` are terminal for an attempt. `lost` means no live process is controlled, but the worker may be resumed into another attempt.

### 8.2 Health inference

The SDK exposes no heartbeat. Health is inferred from the child process and event stream:

- `unknown`: no successful initialization yet.
- `healthy`: process exists and events have arrived within the active threshold.
- `quiet`: process exists but no event has arrived within the quiet threshold.
- `stalled`: process exists, a turn is active, and no event has arrived within the configurable stalled threshold.
- `exited`: the child process is gone.

The default quiet threshold is 60 seconds. The default stalled threshold is 5 minutes. `stalled` generates a warning and remains reversible; it never causes automatic interruption or termination.

### 8.3 Completion reducer

The adapter consumes `receive_messages()` and maintains a task ledger keyed by Qoder task ID.

- `TaskStartedMessage` adds an active task.
- `TaskNotificationMessage` resolves it as completed, failed, or stopped.
- Capability-gated raw `background_tasks_changed` or `task_updated` events may reconcile the ledger, but only in a versioned adapter with fixture coverage.
- `ResultMessage` records the main turn result and starts a settlement window. Its target state is `failed` when `is_error` is true and `completed` otherwise.
- If the active ledger is empty, the worker enters the target state immediately.
- If tasks remain, the adapter waits up to five seconds for terminal or reconciliation events.
- If tasks remain unresolved after the settlement window, the worker enters the target state with `nested_state=unknown` and warning `nested_terminal_event_missing`.

This rule prevents an absent task-notification event from keeping a semantically complete worker alive forever. `result` and `status` must expose the degraded nested state; they must not claim every nested task was observed to finish.

## 9. Worker contracts

### 9.1 Common prompt envelope

The supervisor renders a stable prompt envelope from structured spawn parameters:

```text
ROLE
OBJECTIVE
WORKSPACE
CONTEXT
CONSTRAINTS
ACCEPTANCE CRITERIA
REPORT CONTRACT
```

The skill supplies facts and desired outcomes, not detailed reasoning instructions. Qoder retains control over its native harness.

Every worker shall return:

- outcome: `completed`, `partial`, or `blocked`;
- concise summary;
- files examined or changed;
- validation performed and results;
- unresolved risks or blockers;
- actual model usage when available.

### 9.2 Coder

The coder:

- may read and modify the shared workspace;
- does not create a worktree unless the caller explicitly requests one outside qworker;
- may use Qoder direct subagents;
- runs the relevant formatter, linter, type checker, and tests available in the project;
- uses test-first development when the contract requests it or the repository requires it;
- does not commit, push, publish, deploy, or rewrite unrelated user changes unless explicitly authorized;
- reports partial edits when blocked or interrupted.

Default SDK policy:

- `tools="default"`;
- `permission_mode="acceptEdits"`;
- dangerous permission bypass disabled;
- project-level overrides may make the policy more restrictive.

Codex is responsible for coordinating concurrent writers. The supervisor warns when another live `shared_workspace` worker has the same canonical cwd, but it does not block the spawn.

### 9.3 Auditor

The auditor is independent and read-only. It receives requirements, relevant repository context, and the change or design to assess. Its prompt does not include Codex's desired verdict.

Default visible tools:

```text
Read, Glob, Grep, WebFetch, WebSearch, Agent
```

Enforcement layers:

1. `tools` limits tool visibility.
2. `disallowed_tools` denies `Write`, `Edit`, `Bash`, `NotebookEdit`, and any installed mutation-capable extension.
3. `permission_mode="dontAsk"` prevents permission escalation from becoming an interactive bypass.
4. `can_use_tool` uses an exact allow policy and denies unknown tools. It applies the same decision to calls carrying a nested `agent_id`.
5. Registered auditor helper agents receive the same read-only tools, a mutation denylist, no `Agent` tool, and bounded `maxTurns`.

The auditor may choose whether to delegate. Codex neither scripts nor steers the nested auditor helper. Qoder's one-level nesting limit is accepted.

The report format is:

```text
VERDICT
CONFIRMED
FINDINGS
RISKS
REQUIRED_CHANGES
```

Every finding includes severity, evidence, and the affected requirement or code location when available.

## 10. Model resolution

Model aliases are ordered policy, not hidden fallback.

Initial defaults:

```text
qwen-auditor = [Qwen3.8-Max, Qwen3.7-Max, Auto]
qwen-coder   = [Qwen3.8-Max, Qwen3.7-Max, Auto]
```

An exact model request has no fallback. An alias explicitly permits the ordered candidates it contains.

Resolution flow:

1. Create the SDK client with no explicit model and call `connect(None)`.
2. Call `get_available_models()`.
3. Select the first enabled candidate for the requested alias, or validate the exact model.
4. Call `set_model(resolved_model)` before the first work prompt.
5. Persist requested and resolved values and emit `model.resolved`.
6. Capture models actually reported by assistant messages and `ResultMessage.model_usage`.

The catalog is cached for five minutes but refreshed after a model-not-found failure. Spawn fails with `model_unavailable` when no allowed candidate exists. The system never silently substitutes an unlisted fallback.

## 11. Authentication and runtime preflight

`qworker doctor` and every spawn perform these checks without logging secret values:

1. Import the pinned SDK and report its version.
2. Resolve the runtime path and version.
3. Select authentication in this order:
   - `QODER_PERSONAL_ACCESS_TOKEN` through `access_token_from_env()`;
   - explicitly configured service-account authentication;
   - `qodercli_auth()` when local-login reuse is enabled.
4. Initialize a control connection and retrieve server capabilities.
5. Classify explicit SDK authentication and runtime errors directly.
6. Classify a control-request timeout as `initialize_timeout`. When local-login reuse was selected, include the actionable warning `qodercli_auth_reuse_failed` if interactive `qodercli status` succeeds, because SDK 1.0.13 may hide its authentication result behind the initialize timeout.

V1 does not patch or depend on the SDK's private initialization reader to intercept hidden authentication results. Personal access-token authentication is therefore the preferred unattended path.

Secrets are never stored in SQLite, event payloads, command arguments, or logs. SDK-created credential payload files retain the SDK's owner-only permissions and cleanup behavior.

## 12. CLI contract

### 12.1 Spawn

```text
qworker spawn \
  --role auditor \
  --cwd /absolute/project \
  --model qwen-auditor \
  --spec-file /path/to/contract.md \
  --json
```

Successful acceptance returns immediately:

```json
{
  "worker_id": "01J...",
  "state": "starting",
  "role": "auditor",
  "cwd": "/absolute/project",
  "event_cursor": 1
}
```

### 12.2 Status and list

`status` returns the complete worker record, current attempt, active-task summaries, pending approvals, queued steering messages, warnings, and the latest event cursor.

`list` defaults to nonterminal workers and accepts `--all`, `--role`, `--state`, and `--cwd` filters.

### 12.3 Watch

```text
qworker watch WORKER_ID --since SEQUENCE --follow --json
```

Events are emitted in increasing sequence order. Reconnection with the last seen sequence is lossless for persisted semantic events. Ephemeral SDK token deltas are outside this guarantee.

### 12.4 Steer

```text
qworker steer WORKER_ID --priority now --message-file /path/to/message
```

The response contains a generated message UUID and whether it was accepted into the SDK input stream. `now` may interrupt the active turn; `next` waits for a safe boundary; `later` waits for idle. Cancellation is best-effort and reports the SDK boolean without strengthening its meaning.

### 12.5 Respond

`respond` resolves only a currently pending approval or elicitation request. The command validates the response against the stored choice schema. It cannot answer a request from an earlier, lost attempt.

### 12.6 Stop

Normal stop:

1. mark stop requested;
2. call `interrupt()`;
3. allow up to ten seconds for a result or abort acknowledgement;
4. call `disconnect()` and close the child process;
5. resolve pending approvals as denied;
6. mark the attempt `cancelled` unless a terminal result already established another state.

`--force` skips the grace period and terminates the process. Neither form rolls back workspace edits.

### 12.7 Resume

`resume` is accepted only for `lost`, `failed`, or `cancelled` workers with a stored session ID. It verifies the original cwd, increments `attempt`, creates a new SDK process with `resume=<session_id>`, and sends a short recovery message stating that the prior process ended and workspace state must be rechecked before continuing.

Resume never claims that an interrupted tool call completed.

## 13. Error behavior

Stable error codes include:

- `supervisor_unavailable`
- `invalid_request`
- `worker_not_found`
- `worker_not_live`
- `write_conflict_warning`
- `auth_required`
- `runtime_not_found`
- `runtime_incompatible`
- `initialize_timeout`
- `model_unavailable`
- `sdk_protocol_error`
- `approval_not_pending`
- `message_not_cancellable`
- `resume_not_possible`

Failures before runtime initialization set the worker to `failed`. Unexpected process exit before a main result sets it to `lost`. A model or tool error returned through `ResultMessage` sets `failed` while preserving the structured result and permission denials.

The supervisor never auto-retries a coder after the first work prompt was delivered. This avoids duplicate workspace mutations. Auditor retry may be added later but is not part of V1.

## 14. Shared-workspace coordination

qworker deliberately provides visibility instead of ownership locks.

- Every writer exposes `write_capability=shared_workspace` and its canonical cwd.
- `spawn` reports live writers whose cwd is the same as, or an ancestor/descendant of, the requested cwd.
- The warning is persisted and returned to Codex.
- Codex decides whether to proceed, wait, stop another worker, or use a worktree outside qworker.
- qworker does not reset, clean, stash, commit, or otherwise normalize the workspace.

Auditors may run concurrently with writers. Their findings identify the observed revision or dirty-worktree state when possible.

## 15. Configuration

User configuration lives at `~/.config/qworker/config.toml`. Project configuration may live at `.qworker.toml` and can only narrow proactive or permission policy unless the user config explicitly allows project expansion.

Configuration areas:

- SDK and runtime path/version policy;
- authentication source order;
- model aliases;
- health thresholds;
- proactive coder enablement;
- coder permission mode and tool restrictions;
- auditor web access;
- event retention;
- external Qoder setting sources and plugins.

Qoder `setting_sources` are explicit. V1 defaults to an isolated SDK session with no inherited user/project/local Qoder settings. A project may opt into selected sources after `doctor` reports the additional hooks, skills, and plugins they load. This prevents unrelated host hooks from silently joining every worker while retaining a deliberate path to Qoder customization.

## 16. Observability

`status` and `watch` shall make these distinctions visible:

- worker lifecycle versus inferred health;
- main result versus nested settlement confidence;
- live approval versus historical expired approval;
- requested, resolved, and actually reported model;
- queued steering message versus delivered or uncancellable message;
- supervisor event history versus Qoder transcript availability;
- graceful cancellation versus process loss.

Logs are structured and rotate locally. Default logs contain IDs, event types, durations, exit codes, and redacted error summaries. Full prompts, access tokens, tool arguments, and file contents require an explicit debug mode and remain redacted for known credential fields.

## 17. Testing strategy

### 17.1 Unit tests

- lifecycle and health reducers, including invalid transitions;
- task ledger and missing-terminal settlement behavior;
- exact and alias model resolution;
- auditor tool policy for top-level and nested `agent_id` calls;
- approval expiration on loss;
- steering UUID and cancellation state;
- cwd overlap warnings;
- RPC and event-schema compatibility;
- log and event redaction.

### 17.2 Adapter contract tests

Use an injected fake SDK transport to replay fixtures for:

- initialization and model discovery;
- assistant and tool messages;
- task start/progress/terminal events;
- a main result arriving before nested terminal telemetry;
- raw `background_tasks_changed` and `task_updated` variants;
- permission, elicitation, and OAuth requests;
- authentication result during initialize;
- process EOF and protocol errors;
- multiple turns with `now`, `next`, and `later` steering.

### 17.3 Real integration tests

Credential-gated tests shall:

1. initialize the bundled runtime and list models;
2. run a read-only `Qwen3.8-Max` auditor;
3. have the auditor spawn one direct internal subagent;
4. observe nested task telemetry and inspect the persisted subagent transcript;
5. steer an active worker with each priority;
6. cancel a queued UUID-stamped message;
7. exercise a permission callback and `respond`;
8. interrupt and gracefully stop a worker;
9. kill the supervisor, mark the worker lost, and resume its conversation in a new process;
10. verify that an auditor attempting `Write`, `Edit`, or `Bash` is denied at the callback boundary.

Real integration tests use a disposable directory. Coder integration tests never target a user's active project.

## 18. Acceptance criteria

V1 is accepted when all of the following are demonstrated:

1. Codex can spawn two Qoder workers, receive distinct IDs immediately, and observe both concurrently.
2. A worker remains controllable from later CLI invocations while its supervisor is alive.
3. `watch` reconnects from an event cursor without losing persisted semantic events.
4. `Qwen3.8-Max` can be resolved from the live catalog and is reported as requested, resolved, and actually used.
5. A coder modifies a disposable shared workspace and reports passing or failing validation without an automatic worktree.
6. An auditor and its direct helper cannot mutate the workspace or execute a shell command.
7. A Qoder direct subagent appears in live task telemetry and is discoverable afterward through the session transcript APIs.
8. Missing nested terminal telemetry produces `nested_state=unknown` and a warning rather than a permanently running worker.
9. `now`, `next`, and `later` steering are delivered with stable message UUIDs.
10. A live approval can be answered; an approval from a lost attempt cannot.
11. Normal stop preserves existing workspace edits and leaves no QoderCLI child process.
12. Supervisor loss is reported as `lost`; explicit resume starts a new attempt using the stored session and cwd.
13. No credential value appears in arguments, database rows, events, or normal logs.
14. Unsupported nested-agent stop or steering requests fail explicitly rather than pretending success.

## 19. Implementation sequence

The later implementation plan should preserve these tracer bullets:

1. SDK adapter and a foreground single-worker auditor probe.
2. Supervisor, SQLite records, local RPC, and `spawn/status/watch/result`.
3. Steering, cancellation, approvals, stop, and loss handling.
4. Model policy, authentication preflight, and explicit resume.
5. Coder contract and shared-writer visibility.
6. `$qoder-worker` skill and proactive activation policy.
7. Failure injection, security enforcement, and real integration suite.

Each tracer bullet must end in a demonstrable end-to-end path rather than a collection of disconnected internal modules.

## 20. References

- Qoder SDK overview: <https://docs.qoder.com/cli/sdk/overview>
- Qoder SDK architecture: <https://docs.qoder.com/cli/sdk/how-it-works>
- Qoder SDK input modes: <https://docs.qoder.com/cli/sdk/input-modes>
- Python SDK reference: <https://docs.qoder.com/cli/sdk/references-python>
- Qoder subagents: <https://docs.qoder.com/cli/subagent>
- Feasibility probe: `spikes/qoder_audit_probe.py`
