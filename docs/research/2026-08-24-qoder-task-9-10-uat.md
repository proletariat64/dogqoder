# Qoder Tasks 9–10 credentialed UAT

Date: 2026-08-24

## Scope

The user first authorized one credential-gated, credit-consuming real-Qoder
test, then explicitly authorized one additional diagnostic run through the
production adapter. Both tests used a pytest disposable directory and minimal
no-write prompts. No other `real_qoder` test ran and neither command was
retried.

Initial raw-client diagnostic, executed once:

```bash
uv run pytest -m real_qoder \
  tests/integration/test_real_qoder_preflight_resume.py::test_live_pat_preflight_and_resumed_conversation \
  -q -s
```

Observed result: `1 failed in 98.31s` at the bounded initial-turn timeout.

Production-adapter diagnostic, executed once with an outer 230-second process
bound:

```bash
uv run pytest -m real_qoder \
  tests/integration/test_real_qoder_preflight_resume.py::test_live_pat_preflight_and_resumed_conversation \
  -q -s
```

Observed result: `1 passed in 25.94s`.

## Task 9 live result: confirmed

`RuntimePreflight(QoderPreflightBackend()).run(tmp_path)` completed before both
conversation attempts. Its live result passed all of these assertions:

- `ok` was true;
- authentication source was `personal_access_token`;
- exposed capabilities were limited to the `modelPolicy` allowlist;
- serialized `DoctorResult.to_json()` did not contain the credential.

The UAT emitted no token value. Runtime and SDK metadata crossed only the
existing safe `DoctorResult` boundary.

## Task 10 first diagnostic: blocked

A fresh `QoderSDKClient` was created from public configured auditor options,
connected, and submitted a short marker prompt. The public
`receive_messages()` stream returned no `ResultMessage` within the 90-second
bound. The traceback showed an SDK-internal memory stream with an
`{"type": "end"}` frame buffered while its send channel remained open; the
receiver then waited until `asyncio.timeout()` cancelled it.

Because the initial turn produced no public `ResultMessage`, it produced no
trustworthy session ID. The first diagnostic therefore did not construct the
second client with `resume=<session_id>` and did not attempt a resumed prompt.
Live Task 10 resume was neither confirmed nor disproved by that run.

## Task 10 production-adapter diagnostic: confirmed

The additional authorized test exercised the production seams rather than the
raw client stream:

- `run_foreground_audit()` constructed a production `QoderSDKTransport` and
  `ForegroundAuditor` for the initial read-only audit;
- the initial adapter result supplied a trustworthy session ID;
- a fresh `QoderSDKTransport` was constructed through
  `create_resumed_transport(cwd, session_id)`;
- `ForegroundAuditor` consumed the resumed adapter result;
- the resumed audit recovered a randomized marker that existed only in the
  prior conversation;
- the resumed result retained the same session identity.

The emitted lifecycle signal contained only booleans and the stage name. It
contained no credential, session ID, prompt, randomized marker, or model
response.

Deterministic Task 10 coverage remains in `tests/test_resume.py`: it proves the
stored session is passed to a fresh transport factory, public
`QoderAgentOptions.resume` preserves the exact session ID, the durable attempt
increments, recovery text is sent, and earlier controls/messages are not
replayed. An additional adapter contract in `tests/test_real_qoder_gate.py`
proves the credential-gated UAT helper passes the stored session to
`create_resumed_transport` and consumes a `ResultMessage` through
`QoderSDKTransport` and `ForegroundAuditor`.

## Credits and safety

- The first raw-client diagnostic did not receive SDK-reported cost. Its
  initial query may have consumed credits; it sent no resumed query.
- The adapter does not expose SDK cost in `AuditResult`, so credit cost for the
  successful initial and resumed adapter audits is unknown.
- Both tests used only pytest temporary directories and auditor options with
  mutation tools denied; no user workspace file changed.
- No credential, token, session ID, prompt marker, or model response is recorded
  in this report.

## Documentation evidence

Context7 resolved `/websites/qoder` in both documentation checks, but returned
Qoder Cloud REST material rather than the Python `qoder-agent-sdk` surface.
The production adapter was therefore checked against the installed, pinned
`qoder-agent-sdk==1.0.13` public API. The live confirmation exercised the
repository's `QoderSDKTransport`, configured `QoderAgentOptions.resume`, and
`ForegroundAuditor` result-consumption path without direct raw-client streaming
in the test.
