import pytest

from qworker.domain import AuditFinding
from qworker.events import (
    AssistantEvent,
    ResultEvent,
    SystemEvent,
    TaskProgressEvent,
    TaskStartedEvent,
    TaskTerminalEvent,
)
from qworker.reducer import ResultReducer

VALID_REPORT = (
    '{"outcome":"completed","summary":"safe","files":["README.md"],'
    '"validation":["pytest passed"],"risks":["none observed"],'
    '"verdict":"approved","confirmed":["auditor remained read-only"],'
    '"findings":[{"severity":"low","evidence":"README.md was read",'
    '"affected_requirement_or_location":"README.md"}],'
    '"required_changes":["none"]}'
)


def test_result_with_no_active_tasks_completes_immediately() -> None:
    reducer = ResultReducer(
        requested_model="qwen-auditor",
        resolved_model="Qwen3.8-Max",
    )
    reducer.apply(
        AssistantEvent(
            text=("checked code",),
            tools=("Read",),
            model="Qwen3.8-Max",
        )
    )
    reducer.apply(
        ResultEvent(
            session_id="session-1",
            is_error=False,
            result=VALID_REPORT,
        )
    )

    result = reducer.finish(settlement_expired=False)

    assert result.outcome == "completed"
    assert result.nested_state == "settled"
    assert result.actual_models == ("Qwen3.8-Max",)
    assert result.verdict == "approved"
    assert result.confirmed == ("auditor remained read-only",)
    assert result.findings == (
        AuditFinding(
            severity="low",
            evidence="README.md was read",
            affected_requirement_or_location="README.md",
        ),
    )
    assert result.required_changes == ("none",)


def started_reducer() -> ResultReducer:
    reducer = ResultReducer(
        requested_model="qwen-auditor",
        resolved_model="Qwen3.8-Max",
    )
    reducer.apply(TaskStartedEvent(task_id="task-1", description="inspect SDK"))
    return reducer


def test_terminal_task_after_result_settles_completion() -> None:
    reducer = started_reducer()
    reducer.apply(ResultEvent(session_id="session-1", is_error=False, result="done"))

    assert reducer.needs_settlement is True

    reducer.apply(TaskTerminalEvent(task_id="task-1", status="completed"))

    assert reducer.finish(settlement_expired=False).nested_state == "settled"


def test_missing_terminal_task_degrades_without_hanging() -> None:
    reducer = started_reducer()
    reducer.apply(ResultEvent(session_id="session-1", is_error=False, result="done"))

    result = reducer.finish(settlement_expired=True)

    assert result.outcome == "partial"
    assert result.nested_state == "unknown"
    assert "report_contract_unparseable" in result.warnings
    assert "nested_terminal_event_missing" in result.warnings


def test_error_result_fails_even_when_nested_state_is_unknown() -> None:
    reducer = started_reducer()
    reducer.apply(
        ResultEvent(
            session_id="session-1",
            is_error=True,
            result=None,
            errors=("tool failed",),
        )
    )

    result = reducer.finish(settlement_expired=True)

    assert result.outcome == "failed"
    assert "report_contract_unparseable" in result.warnings


def test_semantic_history_is_redacted_and_bounded() -> None:
    reducer = ResultReducer(
        requested_model="qwen-auditor",
        resolved_model="Qwen3.8-Max",
    )
    secret = "credential-token-should-not-be-recorded"
    reducer.apply(AssistantEvent(text=(secret,), tools=(secret,), model=secret))
    reducer.apply(SystemEvent(subtype=secret, message=secret))
    reducer.apply(
        ResultEvent(
            session_id=secret,
            is_error=True,
            result=secret,
            model_usage=(secret,),
            permission_denials=(secret,),
            errors=(secret,),
        )
    )

    assert secret not in str(reducer.semantic_events)

    for task_number in range(128):
        reducer.apply(
            TaskStartedEvent(
                task_id=f"task-{task_number}",
                description=secret,
            )
        )

    assert len(reducer.semantic_events) == 128
    assert {event.kind for event in reducer.semantic_events} == {"task_started"}


def test_semantic_history_distinguishes_tools_and_preserves_receive_order() -> None:
    reducer = ResultReducer(
        requested_model="qwen-auditor",
        resolved_model="Qwen3.8-Max",
    )

    reducer.apply(
        AssistantEvent(
            text=("checked code",),
            tools=("Read", "Grep"),
            model="Qwen3.8-Max",
        )
    )
    reducer.apply(TaskStartedEvent(task_id="task-1", description="inspect"))
    reducer.apply(
        TaskProgressEvent(
            task_id="task-1",
            description="checking",
            last_tool_name="Read",
        )
    )
    reducer.apply(TaskTerminalEvent(task_id="task-1", status="completed"))
    reducer.apply(SystemEvent(subtype="status", message="ready"))
    reducer.apply(ResultEvent(session_id="session-1", is_error=False, result=None))

    assert [event.kind for event in reducer.semantic_events] == [
        "assistant",
        "tool",
        "tool",
        "task_started",
        "task_progress",
        "task_terminal",
        "system",
        "result",
    ]


@pytest.mark.parametrize(
    "report",
    [
        None,
        "not json",
        (
            '{"outcome":"completed","summary":"safe","files":[],'
            '"validation":[],"risks":[]}'
        ),
        (
            '{"outcome":"completed","summary":"safe","files":[],'
            '"validation":[],"risks":[],"verdict":"approved",'
            '"confirmed":[],"findings":[{"severity":"low",'
            '"evidence":"observed",'
            '"affected_requirement_or_location":7}],'
            '"required_changes":[]}'
        ),
        VALID_REPORT[:-1] + ',"unexpected":true}',
        f"```json\n{VALID_REPORT}\n```\ntrailing content",
        f"```json\n```json\n{VALID_REPORT}\n```\n```",
    ],
)
def test_successful_invalid_report_is_partial(report: str | None) -> None:
    reducer = ResultReducer(
        requested_model="qwen-auditor",
        resolved_model="Qwen3.8-Max",
    )
    reducer.apply(
        ResultEvent(
            session_id="session-1",
            is_error=False,
            result=report,
        )
    )

    result = reducer.finish(settlement_expired=False)

    assert result.outcome == "partial"
    assert "report_contract_unparseable" in result.warnings
    assert result.verdict is None
    assert result.confirmed == ()
    assert result.findings == ()
    assert result.required_changes == ()


def test_single_json_fence_is_unwrapped_before_strict_schema_validation() -> None:
    reducer = ResultReducer(
        requested_model="qwen-auditor",
        resolved_model="Qwen3.8-Max",
    )
    reducer.apply(
        ResultEvent(
            session_id="session-1",
            is_error=False,
            result=f"```json\n{VALID_REPORT}\n```",
        )
    )

    result = reducer.finish(settlement_expired=False)

    assert result.outcome == "completed"
    assert result.verdict == "approved"
    assert "report_contract_unparseable" not in result.warnings


def test_error_result_overrides_valid_report_outcome() -> None:
    reducer = ResultReducer(
        requested_model="qwen-auditor",
        resolved_model="Qwen3.8-Max",
    )
    reducer.apply(
        ResultEvent(
            session_id="session-1",
            is_error=True,
            result=VALID_REPORT,
        )
    )

    assert reducer.finish(settlement_expired=False).outcome == "failed"
