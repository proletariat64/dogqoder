"""Foreground orchestration for one read-only Qoder audit."""

import asyncio
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from qworker.domain import AuditContract, AuditResult
from qworker.events import ResultEvent
from qworker.model_policy import ModelUnavailableError, resolve_model
from qworker.qoder_sdk import (
    AdapterDiagnostic,
    classify_sdk_error,
    create_default_transport,
)
from qworker.reducer import ResultReducer
from qworker.transport import QoderTransport

type TransportFactory = Callable[[Path], QoderTransport]

_DEFAULT_SETTLEMENT_TIMEOUT = 5.0


class ForegroundAuditor:
    """Run one audit to structured completion in the current asyncio context."""

    def __init__(
        self,
        transport_factory: TransportFactory,
        *,
        settlement_timeout: float = _DEFAULT_SETTLEMENT_TIMEOUT,
    ) -> None:
        self._transport_factory = transport_factory
        self._settlement_timeout = settlement_timeout

    async def run(self, contract: AuditContract) -> AuditResult:
        """Execute one contract and always close an initialized transport."""

        transport: QoderTransport | None = None
        result: AuditResult | None = None
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
            await transport.send(_render_prompt(contract))
            result = await self._consume_result(
                transport,
                requested_model=contract.requested_model,
                resolved_model=resolved_model,
            )
        except AdapterDiagnostic as error:
            result = _diagnostic_result(
                contract,
                classify_sdk_error(error, operation="runtime_construction"),
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
                    diagnostic = classify_sdk_error(
                        error,
                        operation="disconnect",
                    )
                    if result is None:
                        result = _diagnostic_result(
                            contract,
                            diagnostic,
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
            raise RuntimeError("Foreground audit ended without a result")
        return result

    async def _consume_result(
        self,
        transport: QoderTransport,
        *,
        requested_model: str,
        resolved_model: str,
    ) -> AuditResult:
        reducer = ResultReducer(
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
        except TimeoutError:
            return reducer.finish(settlement_expired=True)

        return reducer.finish(settlement_expired=reducer.needs_settlement)


async def run_foreground_audit(contract: AuditContract) -> AuditResult:
    """Run one foreground audit with the SDK-bundled production transport."""

    return await ForegroundAuditor(create_default_transport).run(contract)


def _render_prompt(contract: AuditContract) -> str:
    sections = (
        ("ROLE", "Independent read-only auditor"),
        ("OBJECTIVE", contract.objective),
        ("WORKSPACE", str(contract.cwd)),
        ("CONTEXT", _render_items(contract.context)),
        (
            "CONSTRAINTS",
            _render_items(
                (
                    "Do not modify the workspace or run shell commands.",
                    *contract.constraints,
                )
            ),
        ),
        ("ACCEPTANCE CRITERIA", _render_items(contract.acceptance_criteria)),
        (
            "REPORT CONTRACT",
            (
                "Return one JSON object with keys outcome, summary, files, "
                "validation, and risks. outcome must be completed, partial, "
                "or blocked. All fields except outcome and summary are arrays "
                "of strings. Include severity, evidence, and affected requirement "
                "or code location for every finding."
            ),
        ),
    )
    return "\n\n".join(f"{heading}\n{body}" for heading, body in sections)


def _render_items(items: tuple[str, ...]) -> str:
    if not items:
        return "- none"
    return "\n".join(f"- {item}" for item in items)


def _diagnostic_result(
    contract: AuditContract,
    diagnostic: AdapterDiagnostic,
    *,
    resolved_model: str | None,
) -> AuditResult:
    return _failure_result(
        contract,
        summary=diagnostic.message,
        error_code=diagnostic.code,
        resolved_model=resolved_model,
    )


def _failure_result(
    contract: AuditContract,
    *,
    summary: str,
    error_code: str,
    resolved_model: str | None,
) -> AuditResult:
    return AuditResult(
        outcome="failed",
        summary=summary,
        files=(),
        validation=(),
        risks=(),
        requested_model=contract.requested_model,
        resolved_model=resolved_model,
        errors=(error_code,),
    )
