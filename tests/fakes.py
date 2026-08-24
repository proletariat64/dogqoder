"""Test doubles for the SDK-independent Qoder transport seam."""

import asyncio
from collections.abc import AsyncIterator

from qworker.control import ControlCallbacks, SteeringPriority
from qworker.events import AdapterEvent, ResultEvent
from qworker.model_policy import AvailableModel

SUCCESSFUL_AUDIT_REPORT = (
    '{"outcome":"completed","summary":"safe","files":[],'
    '"validation":[],"risks":[],"verdict":"approved",'
    '"confirmed":["workspace inspected"],"findings":[],'
    '"required_changes":[]}'
)


class FakeQoderTransport:
    """Deterministic transport recording foreground-auditor calls."""

    def __init__(
        self,
        *,
        models: tuple[AvailableModel, ...],
        events: tuple[AdapterEvent, ...],
        event_delays: tuple[float, ...] = (),
        hang_after_events: bool = False,
    ) -> None:
        self._models = models
        self._events = events
        self._event_delays = event_delays
        self._hang_after_events = hang_after_events
        self.calls: list[str] = []
        self.disconnected = False
        self.sent_prompts: list[str] = []
        self.control_callbacks: ControlCallbacks | None = None

    @classmethod
    def successful_audit(cls, *, model: str) -> "FakeQoderTransport":
        return cls(
            models=(AvailableModel(value=model, enabled=True),),
            events=(
                ResultEvent(
                    session_id="session-1",
                    is_error=False,
                    result=SUCCESSFUL_AUDIT_REPORT,
                    model_usage=(model,),
                ),
            ),
        )

    async def connect(self) -> None:
        self.calls.append("connect")

    async def available_models(self) -> tuple[AvailableModel, ...]:
        self.calls.append("models")
        return self._models

    async def select_model(self, model: str) -> None:
        self.calls.append(f"model:{model}")

    async def send(self, prompt: str) -> None:
        self.calls.append("send")
        self.sent_prompts.append(prompt)

    def bind_control(self, callbacks: ControlCallbacks) -> None:
        self.control_callbacks = callbacks

    async def steer(
        self,
        message: str,
        *,
        priority: SteeringPriority,
        message_id: str,
    ) -> None:
        del message, priority, message_id

    async def cancel_message(self, message_id: str) -> bool:
        del message_id
        return False

    async def interrupt(self) -> None:
        self.calls.append("interrupt")

    async def messages(self) -> AsyncIterator[AdapterEvent]:
        for index, event in enumerate(self._events):
            if index < len(self._event_delays):
                await asyncio.sleep(self._event_delays[index])
            yield event
        if self._hang_after_events:
            await asyncio.Event().wait()

    async def disconnect(self) -> None:
        self.disconnected = True
        self.calls.append("disconnect")

    def abort(self) -> None:
        self.disconnected = True
        self.calls.append("abort")
