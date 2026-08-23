"""SDK-independent transport seam for foreground Qoder workers."""

from collections.abc import AsyncIterator
from typing import Protocol

from qworker.events import AdapterEvent
from qworker.model_policy import AvailableModel


class QoderTransport(Protocol):
    """Minimal conversation transport consumed by worker orchestration."""

    async def connect(self) -> None: ...

    async def available_models(self) -> tuple[AvailableModel, ...]: ...

    async def select_model(self, model: str) -> None: ...

    async def send(self, prompt: str) -> None: ...

    def messages(self) -> AsyncIterator[AdapterEvent]: ...

    async def disconnect(self) -> None: ...
