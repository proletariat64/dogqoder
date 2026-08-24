# AC5 reliable coder reporting research

Date: 2026-08-24

## 2026-08-25 decision and live result

The user separated coder work correctness from model-authored reporting.
qworker now treats completed lifecycle plus independently verified workspace
facts as AC5's authority; an exact five-key JSON report is optional telemetry.
The credential-free public replay passed with a deliberate partial result and
`report_contract_unparseable`, and one authorized real AC5 marker then passed
in 34.05 seconds with exact bytes, completed lifecycle, expected model,
non-worktree isolation, and credential exclusion. No retry ran.

Consequently, the `submit_report` MCP/Stop-hook design below is no longer needed
to establish core coder capability. It remains a possible reporting-quality
enhancement if a future consumer needs trustworthy model-authored summary,
files, validation narrative, or risks.

## Question and observed failure

AC5 asks a real Qoder coder to edit a disposable workspace and return a reliable
five-field report: `outcome`, `summary`, `files`, `validation`, and `risks`.
Two bounded live runs created the exact requested file and reported the expected
model, but neither `ResultMessage.result` nor any streamed assistant text
contained the contract. The deterministic replay proves qworker can recover a
valid assistant JSON object when one exists; the second live run shows that no
such object existed to recover. See
[`qworker-v1-acceptance.md`](./qworker-v1-acceptance.md#authorized-ac5-ac6-and-ac11-one-shot-markers).

No credentialed Qoder call was made for this research.

## High-confidence SDK findings

Facts below come from official documentation, official samples, PyPI metadata,
and the installed `qoder-agent-sdk==1.0.13` source.

- PyPI's live JSON metadata reports `1.0.13` as latest, matching this repo's
  installed and pinned version. The package is still classified Alpha.
  [PyPI](https://pypi.org/project/qoder-agent-sdk/)
- A query completes at `ResultMessage`; `receive_response()` stops immediately
  after yielding it, while `receive_messages()` continues over the session.
  `ResultMessage.result` is `str | None`, not a schema-validated object.
  [Python SDK reference](https://docs.qoder.com/cli/sdk/references-python),
  [SDK lifecycle](https://docs.qoder.com/cli/sdk/how-it-works)
- Local 1.0.13 explicitly does **not** support structured result output:
  `ResultMessage.structured_output` is an `InitVar[Any]` retained only for
  positional compatibility and is neither parsed nor stored. `QoderAgentOptions`
  has no `output_format`, `json_schema`, or response-schema field. Therefore an
  Anthropic-style `output_format`/`structured_output` fix is unavailable in the
  pinned and latest public Python SDK.
- QoderCLI `--output-format json` or `stream-json` structures the CLI envelope;
  it does not constrain the model's inner answer to an application JSON Schema.
  The SDK already starts qodercli with `stream-json`.
  [Run in scripts](https://docs.qoder.com/cli/run-in-scripts),
  [CLI reference](https://docs.qoder.com/cli/cli-reference)
- Python supports in-process MCP tools through `@tool()` and
  `create_sdk_mcp_server()`. Tool inputs can use a full JSON Schema, and the
  handler executes in the host process with direct access to host state.
  [Tools](https://docs.qoder.com/cli/sdk/tools),
  [MCP integration](https://docs.qoder.com/cli/sdk/mcp),
  [official Python sample](https://github.com/QoderAI/qoder-agent-sdk-samples/blob/main/python/custom-tools/main.py)
- A Python `Stop` hook receives `last_assistant_message` and
  `stop_hook_active`. Returning `{"decision": "block", "reason": ...}`
  prevents stopping and injects the reason as a continuation prompt. On the
  retry, `stop_hook_active` is true and must be used to prevent an infinite
  loop. [Hooks](https://docs.qoder.com/cli/hooks),
  [Python SDK reference](https://docs.qoder.com/cli/sdk/references-python),
  [official hook sample](https://github.com/QoderAI/qoder-agent-sdk-samples/blob/main/python/hooks/main.py)
- Qoder recommends preserving the qodercli coding preset and appending host
  rules with `system_prompt={"type": "preset", "preset": "qodercli",
  "append": "..."}`. Local 1.0.13 currently maps `system_prompt=None` to
  `--system-prompt ""`; qworker uses `None`. It is a reasonable inference that
  qworker is discarding useful default coding instructions and relying only on
  a lower-priority user prompt.
  [System prompt](https://docs.qoder.com/cli/sdk/system-prompt)
- Official samples define success only as `ResultMessage.subtype == "success"`;
  they do not demonstrate schema-constrained final reports. This confirms the
  missing guarantee rather than supplying a hidden extraction API.
  [official model-selection sample](https://github.com/QoderAI/qoder-agent-sdk-samples/blob/main/python/model-selection/main.py)

## Ranked solution candidates

### 1. Host-owned `submit_report` MCP channel plus bounded Stop repair

Recommended.

1. Register one in-process tool named `mcp__qworker__submit_report` with a full
   JSON Schema for the exact five fields, `additionalProperties: false`, and an
   enum for `outcome`.
2. Validate the raw handler argument again in qworker. Schema advertisement is
   model guidance, not sufficient trust. Accept only the first valid payload,
   store an immutable copy in per-run host state, and return a short success
   result. Reject malformed or duplicate submissions without replacing the
   accepted report.
3. Pre-authorize only this custom tool. Preserve normal coder behavior with the
   qodercli system-prompt preset and append a high-priority rule: finish the edit
   and validation, call `submit_report` exactly once, then stop.
4. Add an in-process `Stop` hook. If a valid report exists, allow stopping. If
   none exists and `stop_hook_active is not True`, block once with a concise
   instruction to call `submit_report`. If `stop_hook_active is True`, allow the
   stop so a bad model cannot create an infinite loop.
5. Treat the host-captured report as primary. A successful SDK result without an
   accepted report remains partial; a report does not override an error
   `ResultMessage`.
6. If the main turn still reaches `ResultMessage` without a report, make exactly
   one report-only recovery against the same conversation. Prefer a fresh
   resumed transport whose visible tools are restricted to read-only inspection
   plus `submit_report`, with mutation tools explicitly denied. Reusing the
   connected session is safe only after qworker gains a phase-aware,
   fail-closed permission callback; `dontAsk` or a prompt alone does not remove
   mutation tools from view. Do not ask it to edit or rerun work. If no valid
   report follows, fail closed as partial.

Why ranked first: it changes reporting from unconstrained prose into an explicit
tool call whose payload crosses an application-owned callback boundary. Stop
repair supplies one bounded self-correction, while the same-session report-only
turn uses existing context without repeating the edit. All required primitives
are public in SDK 1.0.13.

Risks: models may still refuse to call the tool; tool schema does not replace
handler validation; a recovery transport must preserve the conversation while
narrowing tool visibility; the Stop callback and report handler share mutable
run state and need an async lock; no report may be accepted after
terminalization/disconnect.

### 2. Host-derived file and validation evidence

Complement candidate 1 with controller evidence: observe `FileChanged` /
`PostToolUse` hooks, reconcile changed paths against a bounded workspace diff,
and record validation tool outcomes rather than trusting model-written strings.
This can prove that work happened even when narrative reporting fails. It cannot
infer an honest summary or unresolved risks, and shell side effects require
careful reconciliation, so it should not be the only report channel.

### 3. Prompt/text parsing and transcript recovery

Keep current strict `ResultMessage.result` / newest-assistant parsing only as a
compatibility fallback. A session transcript or `last_assistant_message` may aid
diagnosis, but the live AC5 rerun already proved that parsing cannot recover a
report the model never emitted. Community Stop-hook implementations likewise
use `last_assistant_message` as a primary final-text source and guard
`stop_hook_active`, but this is analogous evidence, not a Qoder structured-output
guarantee.
[community hook pattern](https://github.com/daymade/claude-code-skills/blob/main/daymade-claude-code/claude-code-hooks/references/hook_patterns.md)

## Rejected or low-value paths

- `ResultMessage.structured_output`: explicitly unsupported and discarded in
  local/latest 1.0.13.
- Undocumented `extra_args` such as `--json-schema`: no matching public QoderCLI
  flag or Python option was found; passing one would couple qworker to an
  unsupported boundary.
- CLI `--output-format json`: wraps free-form result metadata only.
- More permissive JSON repair: can silently reinterpret malformed model output
  and still cannot recover missing content.
- Automatic full coder retry: may duplicate or overwrite edits and violates
  qworker's no-automatic-coder-retry rule.

## Concrete credential-gated experiments

Run deterministic tests first with fake SDK messages and direct hook/tool calls.
Then request authorization for one bounded real matrix, using fresh disposable
non-worktrees and fixed boolean evidence only:

1. Normal coder calls `submit_report`: exact edit, one accepted report, success
   result, reported file, nonempty validation.
2. First stop omits report: Stop hook blocks once; second pass calls tool; hook
   does not loop.
3. Malformed and duplicate tool calls: handler rejects both safely; accepted
   state cannot be overwritten.
4. No tool after forced continuation: one report-only resumed turn occurs;
   mutating tools are absent or explicitly denied; then partial if still absent.
5. SDK error after a valid report: final outcome remains failed.
6. Race tests: report call versus stop/disconnect; terminal state persists at
   most one immutable report and never leaks raw tool input.

Success threshold for AC5: exact workspace edit, `ResultMessage.subtype` success,
one host-validated report naming the file, observed/nonempty validation evidence,
expected model, no worktree, and no credential in output, events, database, or
workspace.
