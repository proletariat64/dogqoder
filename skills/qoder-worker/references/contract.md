# qworker CLI contract

Use this reference after the skill selects activation and role. These command templates are the stable agent-facing surface. If installed help disagrees with them, stop and report a version mismatch; never invent a flag or parse human-formatted output.

## Machine interaction

Add `--json` to every command. Treat stdout as JSON or JSON Lines only and inspect the decoded values. `watch` emits one JSON value per line. A nonzero process exit is not the error contract by itself: inspect the JSON `error.code`, `error.message`, and `error.warnings` when present.

Keep credentials and substantial content out of command arguments. Put spawn prompts and specifications in `--spec-file`, steering text in `--message-file`, approval objects in `--response-file`, or pipe the corresponding content on stdin. Worker IDs, message IDs, request IDs, cursors, roles, paths, models, priorities, and flags remain normal arguments.

Invoke `qworker` directly from `PATH`. Do not assume dogqoder is the current
repository, substitute `uv run`, or start `python -m qworker.rpc` yourself. If
the console script is unavailable, stop and report that the dogqoder CLI needs
a user-level installation.

Every supervisor-backed command lazily starts the per-user supervisor when its
socket is unavailable. Startup alone does not contact Qoder or consume credits.
Use `--no-start-supervisor` only when the caller explicitly needs fail-fast
connection behavior.

Check configured runtime and authentication readiness when requested or when spawn reports a preflight failure:

```text
qworker doctor --json
```

## Start

Choose `ROLE` as `auditor` or `coder`. Use an absolute `CWD`. Prefer the role alias `qwen-auditor` or `qwen-coder` for `MODEL` unless the user requests an exact available model.

```text
qworker spawn --role ROLE --cwd CWD --model MODEL --spec-file SPEC_FILE --json
```

Omitting `--spec-file` reads the specification from stdin. Acceptance returns a JSON object containing `worker_id`, initial `state`, `role`, canonical `cwd`, and `event_cursor`; it may also contain `warnings`. Retain the ID and cursor. For a coder, inspect shared-workspace overlap warnings before introducing any other writer.

## Enumerate

```text
qworker list --json
```

The response contains `workers` and `count`. `workers` includes every persisted
worker newest-first. Each summary contains worker ID, role, canonical cwd,
lifecycle state, health, attempt, write capability, requested and resolved
models, creation and end timestamps, warnings, and latest event cursor. List
summaries exclude result bodies and session IDs; use `status` or `result` for
one worker's details.

## Observe

```text
qworker status WORKER_ID --json
qworker watch WORKER_ID --since EVENT_CURSOR --follow --json
qworker result WORKER_ID --json
```

`status` is a snapshot. Inspect lifecycle `state`, inferred `health`, `attempt`, `write_capability`, pending approvals, warnings, and `event_cursor` independently.

`watch` emits persisted events in sequence order. After each decoded line, retain its sequence as the next reconnection cursor. A disconnected follow can resume with the last observed cursor; token deltas are not part of the persisted guarantee.

Call `result` after a terminal state. Inspect `outcome`, `summary`, `files`, `validation`, `risks`, requested/resolved/actual models, `session_id`, `nested_state`, `warnings`, and `errors`. Auditor results also contain verdict, confirmed facts, findings, and required changes.

## Control

Use files or stdin for control content:

```text
qworker steer WORKER_ID --priority PRIORITY --message-file MESSAGE_FILE --json
qworker cancel-message WORKER_ID MESSAGE_ID --json
qworker respond WORKER_ID REQUEST_ID --response-file RESPONSE_FILE --json
qworker stop WORKER_ID --json
qworker stop WORKER_ID --force --json
```

`PRIORITY` is `now`, `next`, or `later`. A response file contains one JSON object matching the pending request schema. Normal stop allows a bounded grace period; force skips it. Neither stop form rolls back coder edits. Targeted nested-agent control may be rejected explicitly when unsupported.

## Recover

```text
qworker resume WORKER_ID --json
```

Resume applies only to a `lost`, `failed`, or `cancelled` worker with a stored session ID and valid original cwd. It starts a new process and attempt with saved conversation history; it never claims an interrupted tool call completed. Reinspect workspace state before resuming a coder, then use `status`, `watch`, and `result` for the new attempt.

## Fallback boundary

Before possible mutation, a JSON creation failure that proves no coder was accepted may use the active fallback policy. After acceptance, or when coder acceptance is uncertain, assume workspace mutation is possible: inspect worker status when an ID is known, inspect workspace changes, report the partial state, and obtain the next-writer decision before Codex or another worker writes.
