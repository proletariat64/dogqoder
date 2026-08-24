# Qoder Tasks 9–10 credentialed UAT

Date: 2026-08-24

## Scope

The user authorized exactly one credential-gated, credit-consuming real-Qoder
test. The test used a pytest disposable directory and minimal no-write prompts.
No other `real_qoder` test ran.

Command executed once:

```bash
uv run pytest -m real_qoder \
  tests/integration/test_real_qoder_preflight_resume.py::test_live_pat_preflight_and_resumed_conversation \
  -q -s
```

Observed result: `1 failed in 98.31s` at the bounded initial-turn timeout. The
test has since been changed to report this known external block as `xfail`; it
was not rerun, preserving the one-test authorization.

## Task 9 live result: confirmed

`RuntimePreflight(QoderPreflightBackend()).run(tmp_path)` completed before the
conversation attempt. Its live result passed all of these assertions:

- `ok` was true;
- authentication source was `personal_access_token`;
- exposed capabilities were limited to the `modelPolicy` allowlist;
- serialized `DoctorResult.to_json()` did not contain the credential.

The UAT emitted no token value. Runtime and SDK metadata crossed only the
existing safe `DoctorResult` boundary.

## Task 10 live result: blocked

A fresh `QoderSDKClient` was created from public configured auditor options,
connected, and submitted the short prompt `Remember <random marker>. Reply only
READY.` The public `receive_messages()` stream returned no `ResultMessage`
within the 90-second bound. The traceback showed an SDK-internal memory stream
with an `{"type": "end"}` frame buffered while its send channel remained open;
the receiver then waited until `asyncio.timeout()` cancelled it.

Because the initial turn produced no public `ResultMessage`, it produced no
trustworthy session ID. The test therefore did not construct the second client
with `resume=<session_id>` and did not attempt a resumed prompt. Live Task 10
resume is neither confirmed nor disproved by this run.

Deterministic Task 10 coverage remains in `tests/test_resume.py`: it proves the
stored session is passed to a fresh transport factory, public
`QoderAgentOptions.resume` preserves the exact session ID, the durable attempt
increments, recovery text is sent, and earlier controls/messages are not
replayed.

## Credits and safety

- SDK-reported credit cost is unavailable because no `ResultMessage` arrived.
- The initial query may have consumed credits despite the missing result.
- The resumed query was not sent, so it consumed no credits.
- The test used only a pytest temporary directory and auditor options with
  mutation tools denied; no user workspace file changed.
- No credential, token, session ID, prompt marker, or model response is recorded
  in this report.

## Documentation evidence

Context7 resolved `/websites/qoder`, but returned Qoder Cloud REST material
rather than the Python `qoder-agent-sdk` surface. Public API construction was
therefore checked against installed, pinned `qoder-agent-sdk==1.0.13`:
`QoderSDKClient(options=...)`, `connect(None)`, `query()`,
`receive_messages()`, `ResultMessage.session_id`, and
`QoderAgentOptions.resume`.
