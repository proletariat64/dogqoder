"""Foreground orchestration for one shared-workspace Qoder coder."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, TypeGuard, cast

from qworker.events import (
    AdapterEvent,
    AssistantEvent,
    ResultEvent,
    TaskStartedEvent,
    TaskTerminalEvent,
)
from qworker.model_policy import ModelUnavailableError, resolve_model
from qworker.qoder_sdk import (
    AdapterDiagnostic,
    classify_sdk_error,
    create_coder_transport,
)
from qworker.transport import QoderTransport

type CoderOutcome = Literal["completed", "partial", "blocked", "failed"]
type TransportFactory = Callable[[Path], QoderTransport]
type InitializationObserver = Callable[[str], Awaitable[None]]

_DEFAULT_SETTLEMENT_TIMEOUT = 5.0
_REPORT_KEYS = frozenset(("outcome", "summary", "files", "validation", "risks"))
_REPORT_OUTCOMES = frozenset(("completed", "partial", "blocked"))


@dataclass(frozen=True, slots=True)
class CoderContract:
    """Input for one shared-workspace coding run."""

    objective: str
    cwd: Path
    requested_model: str = "qwen-coder"
    context: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    acceptance_criteria: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CoderResult:
    """Structured outcome from one shared-workspace coding run."""

    outcome: CoderOutcome
    summary: str
    files: tuple[str, ...]
    validation: tuple[str, ...]
    risks: tuple[str, ...]
    requested_model: str
    resolved_model: str | None
    actual_models: tuple[str, ...] = ()
    session_id: str | None = None
    nested_state: Literal["none", "active", "settled", "unknown"] = "none"
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


class ForegroundCoder:
    """Run one coder contract through an injected transport."""

    def __init__(
        self,
        transport_factory: TransportFactory,
        *,
        settlement_timeout: float = _DEFAULT_SETTLEMENT_TIMEOUT,
        on_initialized: InitializationObserver | None = None,
    ) -> None:
        self._transport_factory = transport_factory
        self._settlement_timeout = settlement_timeout
        self._on_initialized = on_initialized

    async def run(self, contract: CoderContract) -> CoderResult:
        """Execute one contract and always close an initialized transport."""

        transport: QoderTransport | None = None
        result: CoderResult | None = None
        resolved_model: str | None = None
        try:
            transport = self._transport_factory(contract.cwd)
            await transport.connect()
            resolution = resolve_model(
                contract.requested_model,
                await transport.available_models(),
            )
            resolved_model = resolution.resolved
            await transport.select_model(resolved_model)
            if self._on_initialized is not None:
                await self._on_initialized(resolved_model)
            await transport.send(_render_prompt(contract))
            result = await self._consume_result(
                transport,
                requested_model=contract.requested_model,
                resolved_model=resolved_model,
            )
        except AdapterDiagnostic as error:
            diagnostic = classify_sdk_error(error, operation="runtime_construction")
            result = _failure_result(
                contract,
                summary=diagnostic.message,
                error_code=diagnostic.code,
                resolved_model=resolved_model,
            )
        except ModelUnavailableError as error:
            result = _failure_result(
                contract,
                summary=str(error),
                error_code="model_unavailable",
                resolved_model=resolved_model,
            )
        finally:
            if transport is not None:
                try:
                    await transport.disconnect()
                except AdapterDiagnostic as error:
                    diagnostic = classify_sdk_error(error, operation="disconnect")
                    if result is None:
                        result = _failure_result(
                            contract,
                            summary=diagnostic.message,
                            error_code=diagnostic.code,
                            resolved_model=resolved_model,
                        )
                    else:
                        result = replace(
                            result,
                            outcome="failed",
                            warnings=result.warnings + ("disconnect_failed",),
                            errors=result.errors + (diagnostic.code,),
                        )

        if result is None:
            raise RuntimeError("Foreground coder ended without a result")
        return result

    async def _consume_result(
        self,
        transport: QoderTransport,
        *,
        requested_model: str,
        resolved_model: str,
    ) -> CoderResult:
        reducer = _CoderResultReducer(
            requested_model=requested_model,
            resolved_model=resolved_model,
        )
        messages = transport.messages()
        async for event in messages:
            reducer.apply(event)
            if isinstance(event, ResultEvent):
                if not reducer.needs_settlement:
                    return reducer.finish(settlement_expired=False)
                break
        else:
            return reducer.finish(settlement_expired=False)

        try:
            async with asyncio.timeout(self._settlement_timeout):
                async for event in messages:
                    reducer.apply(event)
                    if not reducer.needs_settlement:
                        return reducer.finish(settlement_expired=False)
        except (BrokenPipeError, ConnectionResetError, EOFError):
            return reducer.finish(settlement_expired=True)
        except asyncio.CancelledError:
            return reducer.finish(settlement_expired=True)
        except TimeoutError:
            return reducer.finish(settlement_expired=True)

        return reducer.finish(settlement_expired=reducer.needs_settlement)


async def run_foreground_coder(contract: CoderContract) -> CoderResult:
    """Run one coder with isolated configured SDK options."""

    return await ForegroundCoder(create_coder_transport).run(contract)


class _CoderResultReducer:
    def __init__(self, *, requested_model: str, resolved_model: str) -> None:
        self._requested_model = requested_model
        self._resolved_model = resolved_model
        self._active_task_ids: set[str] = set()
        self._actual_models: list[str] = []
        self._result: ResultEvent | None = None

    @property
    def needs_settlement(self) -> bool:
        return self._result is not None and bool(self._active_task_ids)

    def apply(self, event: AdapterEvent) -> None:
        if isinstance(event, AssistantEvent):
            self._record_model(event.model)
        elif isinstance(event, TaskStartedEvent):
            self._active_task_ids.add(event.task_id)
        elif isinstance(event, TaskTerminalEvent):
            self._active_task_ids.discard(event.task_id)
        elif isinstance(event, ResultEvent):
            self._result = event
            for model in event.model_usage:
                self._record_model(model)

    def finish(self, *, settlement_expired: bool) -> CoderResult:
        event = self._result
        if event is None:
            return CoderResult(
                outcome="failed",
                summary="No result was received.",
                files=(),
                validation=(),
                risks=(),
                requested_model=self._requested_model,
                resolved_model=self._resolved_model,
                actual_models=tuple(self._actual_models),
                nested_state=self._nested_state(settlement_expired),
                errors=("result_missing",),
            )

        report = _parse_report(event.result)
        warnings: list[str] = []
        if report is None:
            outcome: CoderOutcome = "failed" if event.is_error else "partial"
            summary = event.result or ""
            files: tuple[str, ...] = ()
            validation: tuple[str, ...] = ()
            risks: tuple[str, ...] = ()
            warnings.append("report_contract_unparseable")
        else:
            outcome, summary, files, validation, risks = report
            if event.is_error:
                outcome = "failed"

        nested_state = self._nested_state(settlement_expired)
        if nested_state == "unknown":
            warnings.append("nested_terminal_event_missing")
        return CoderResult(
            outcome=outcome,
            summary=summary,
            files=files,
            validation=validation,
            risks=risks,
            requested_model=self._requested_model,
            resolved_model=self._resolved_model,
            actual_models=tuple(self._actual_models),
            session_id=event.session_id,
            nested_state=nested_state,
            warnings=tuple(warnings),
            errors=event.errors + event.permission_denials,
        )

    def _nested_state(
        self,
        settlement_expired: bool,
    ) -> Literal["none", "active", "settled", "unknown"]:
        if self._result is None:
            return "active" if self._active_task_ids else "none"
        if not self._active_task_ids:
            return "settled"
        return "unknown" if settlement_expired else "active"

    def _record_model(self, model: str | None) -> None:
        if model is not None and model not in self._actual_models:
            self._actual_models.append(model)


def _parse_report(
    result_text: str | None,
) -> tuple[CoderOutcome, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]] | None:
    if result_text is None:
        return None
    try:
        payload: object = json.loads(result_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or set(payload) != _REPORT_KEYS:
        return None
    outcome = payload.get("outcome")
    summary = payload.get("summary")
    files = payload.get("files")
    validation = payload.get("validation")
    risks = payload.get("risks")
    if (
        not isinstance(outcome, str)
        or outcome not in _REPORT_OUTCOMES
        or not isinstance(summary, str)
        or not _is_string_list(files)
        or not _is_string_list(validation)
        or not _is_string_list(risks)
    ):
        return None
    return (
        cast(CoderOutcome, outcome),
        summary,
        tuple(files),
        tuple(validation),
        tuple(risks),
    )


def _is_string_list(value: object) -> TypeGuard[list[str]]:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _render_prompt(contract: CoderContract) -> str:
    sections = (
        ("ROLE", "Qoder shared-workspace coder"),
        ("OBJECTIVE", contract.objective),
        ("WORKSPACE", str(contract.cwd)),
        ("CONTEXT", _render_items(contract.context)),
        (
            "CONSTRAINTS",
            _render_items(
                (
                    "Work directly in the caller workspace; do not create a worktree.",
                    "Preserve unrelated caller changes.",
                    "Do not reset, clean, stash, or roll back workspace state.",
                    "Do not commit, push, publish, deploy, or release changes.",
                    "Run relevant project validation and report its exact outcome.",
                    *contract.constraints,
                )
            ),
        ),
        ("ACCEPTANCE CRITERIA", _render_items(contract.acceptance_criteria)),
        (
            "REPORT CONTRACT",
            (
                "Return exactly one JSON object with keys outcome, summary, files, "
                "validation, and risks. outcome must be completed, partial, or "
                "blocked; summary is a string; files is an array of changed path "
                "strings; validation is an array of commands/checks and outcomes; "
                "risks is an array of unresolved risk or blocker strings."
            ),
        ),
    )
    return "\n\n".join(f"{heading}\n{body}" for heading, body in sections)


def _render_items(items: tuple[str, ...]) -> str:
    if not items:
        return "- none"
    return "\n".join(f"- {item}" for item in items)


def _failure_result(
    contract: CoderContract,
    *,
    summary: str,
    error_code: str,
    resolved_model: str | None,
) -> CoderResult:
    return CoderResult(
        outcome="failed",
        summary=summary,
        files=(),
        validation=(),
        risks=(),
        requested_model=contract.requested_model,
        resolved_model=resolved_model,
        errors=(error_code,),
    )
