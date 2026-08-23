"""Model-resolution policy independent of the Qoder SDK."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class AvailableModel:
    value: str
    enabled: bool


@dataclass(frozen=True, slots=True)
class ModelResolution:
    requested: str
    resolved: str
    used_fallback: bool


MODEL_ALIASES: Final = {
    "qwen-auditor": ("Qwen3.8-Max", "Qwen3.7-Max", "Auto"),
    "qwen-coder": ("Qwen3.8-Max", "Qwen3.7-Max", "Auto"),
}


class ModelUnavailableError(ValueError):
    """Raised when no enabled catalog model satisfies a request."""


def resolve_model(requested: str, catalog: Iterable[AvailableModel]) -> ModelResolution:
    """Resolve an exact model or configured alias against enabled catalog entries."""

    enabled_models = {model.value for model in catalog if model.enabled}
    candidates = MODEL_ALIASES.get(requested)
    if candidates is None:
        if requested in enabled_models:
            return ModelResolution(
                requested=requested,
                resolved=requested,
                used_fallback=False,
            )
        raise ModelUnavailableError(f"Model is unavailable: {requested}")

    for index, candidate in enumerate(candidates):
        if candidate in enabled_models:
            return ModelResolution(
                requested=requested,
                resolved=candidate,
                used_fallback=index > 0,
            )

    raise ModelUnavailableError(f"No enabled model is available for: {requested}")
