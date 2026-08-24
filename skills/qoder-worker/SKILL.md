---
name: qoder-worker
description: Delegate audits or code changes to persistent Qoder workers through qworker. Use when a user explicitly asks for Qoder delegation; when an independent Qoder perspective would materially improve review, risk analysis, or design validation; when project configuration enables proactive Qoder coding; or when managing an existing Qoder worker's lifecycle.
---

# Qoder worker

Map user intent to one persistent Qoder worker through the `qworker` CLI.
Use the public CLI rather than importing the SDK or contacting Qoder directly.

## Choose the role

Apply these rules in order:

1. An explicit role from the user wins.
2. Review, audit, verify, verification, cross-check, challenge, or second-opinion intent selects the read-only `auditor`.
3. Implementation, fix, refactor, test, or code intent selects `coder`.
4. Ambiguous intent defaults to the read-only `auditor`.

## Check activation

- Honor explicit Qoder delegation.
- Proactively start an `auditor` when an independent perspective materially improves review, risk analysis, or design validation.
- Proactively start a `coder` only after project configuration explicitly enables proactive Qoder coding.

Explicit requests do not require proactive opt-in. For a proactive coder, confirm the effective project policy enables `[policy] proactive_coder = true`; file presence alone is not enablement.

## Run the worker

For exact commands, response fields, lifecycle states, and recovery limits, read [the CLI contract](references/contract.md) before the first `qworker` call.

1. Write a focused worker specification containing role, objective, absolute workspace, relevant context, constraints, acceptance criteria, and required report fields.
2. Use files or stdin for prompts, specifications, steering messages, and approval responses; never place large content in command arguments.
3. Use `list --json` to discover every persisted worker before deciding whether a new worker is needed.
4. Run `spawn --json`. Inspect its JSON response and retain `worker_id`, `state`, `event_cursor`, and warnings. An accepted coder is now a possible workspace writer.
5. Use `status --json` for a snapshot and `watch --json` from the last cursor for ordered updates. Parse each JSON value; treat warnings, pending approvals, health, and lifecycle state as separate signals.
6. On a terminal state, use `result --json` and report its outcome, summary, files, validation, risks, actual models, nested state, warnings, and errors.
7. Use `stop --json` when execution should end. Existing coder edits remain in the shared workspace.
8. Use `resume --json` only for an eligible lost, failed, or cancelled worker with a stored session. Recheck the original workspace first, then monitor the new attempt as a fresh process.

## Apply fallback policy

- Before possible workspace mutation, a failed worker creation may fall back to Codex only when active policy permits.
- After a coder may have modified the workspace, report its partial state before Codex continues or another writer starts; never silently replace the writer.

Treat an accepted coder spawn, or a coder spawn whose acceptance is uncertain, as possible mutation. Inspect known worker state and current workspace changes before asking for the next writer decision.
