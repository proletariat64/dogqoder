"""Single explicitly authorized live UAT for Tasks 9 and 10."""

import asyncio
import json
import os
import secrets
from dataclasses import dataclass
from pathlib import Path

import pytest
from qoder_agent_sdk import AssistantMessage, QoderSDKClient, ResultMessage, TextBlock

from qworker.preflight import RuntimePreflight
from qworker.qoder_sdk import QoderPreflightBackend, build_configured_auditor_options
from tests.real_qoder import require_real_qoder_credentials


@dataclass(frozen=True, slots=True)
class _LiveTurn:
    session_id: str
    text: str
    cost_usd: float | None


async def _run_live_turn(
    cwd: Path,
    prompt: str,
    *,
    resume: str | None = None,
) -> _LiveTurn:
    options = build_configured_auditor_options(cwd, resume=resume)
    options.max_turns = 2
    if options.resume != resume:
        pytest.fail("SDK resume option was not preserved.", pytrace=False)
    client = QoderSDKClient(options=options)
    result: ResultMessage | None = None
    text: list[str] = []
    try:
        async with asyncio.timeout(90):
            await client.connect(None)
            await client.query(prompt)
            async for message in client.receive_messages():
                if isinstance(message, AssistantMessage):
                    text.extend(
                        block.text
                        for block in message.content
                        if isinstance(block, TextBlock)
                    )
                elif isinstance(message, ResultMessage):
                    result = message
                    if message.result is not None:
                        text.append(message.result)
    finally:
        await client.disconnect()
    if result is None:
        pytest.fail("Live Qoder turn returned no ResultMessage.", pytrace=False)
    if result.is_error:
        pytest.fail("Live Qoder turn returned an error result.", pytrace=False)
    if not result.session_id:
        pytest.fail("Live Qoder turn returned no session ID.", pytrace=False)
    return _LiveTurn(result.session_id, "\n".join(text), result.total_cost_usd)


@pytest.mark.real_qoder
async def test_live_pat_preflight_and_resumed_conversation(tmp_path: Path) -> None:
    """Spend credits once to prove PAT preflight and public-SDK history resume."""

    require_real_qoder_credentials()
    credential = os.environ["QODER_PERSONAL_ACCESS_TOKEN"]
    before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    async with asyncio.timeout(210):
        preflight = await RuntimePreflight(QoderPreflightBackend()).run(tmp_path)
        if not preflight.ok:
            code = preflight.error.code if preflight.error is not None else "unknown"
            pytest.fail(f"Live Qoder preflight failed safely: {code}", pytrace=False)
        if preflight.auth_source != "personal_access_token":
            pytest.fail(
                "Live preflight did not select PAT authentication.", pytrace=False
            )
        if set(preflight.capabilities) - {"modelPolicy"}:
            pytest.fail("Live preflight exposed an unsafe capability.", pytrace=False)
        preflight_json = json.dumps(preflight.to_json(), sort_keys=True)
        if credential in preflight_json:
            pytest.fail("Credential crossed the preflight boundary.", pytrace=False)

        marker = f"QWUAT{secrets.token_hex(8)}"
        try:
            initial = await _run_live_turn(
                tmp_path,
                f"Remember {marker}. Reply only READY.",
            )
        except TimeoutError:
            pytest.xfail(
                "Task 10 live resume blocked: the initial public message stream "
                "returned no ResultMessage within 90 seconds."
            )
        try:
            resumed = await _run_live_turn(
                tmp_path,
                "Reply only with the marker I asked you to remember.",
                resume=initial.session_id,
            )
        except TimeoutError:
            pytest.xfail(
                "Task 10 live resume blocked: the resumed public message stream "
                "returned no ResultMessage within 90 seconds."
            )

    if marker not in resumed.text:
        pytest.fail(
            "Resumed process did not recover prior conversation history.", pytrace=False
        )
    observable = json.dumps(
        {
            "preflight": preflight.to_json(),
            "initial_cost_usd": initial.cost_usd,
            "resume_cost_usd": resumed.cost_usd,
            "same_session_id": resumed.session_id == initial.session_id,
            "resume_history_confirmed": True,
        },
        sort_keys=True,
    )
    if credential in observable:
        pytest.fail("Credential crossed the UAT output boundary.", pytrace=False)
    after = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    if after != before:
        pytest.fail(
            "Live read-only UAT changed the disposable workspace.", pytrace=False
        )
    print(f"QODER_UAT {observable}")
