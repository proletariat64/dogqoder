import pytest

from qworker.model_policy import (
    AvailableModel,
    ModelUnavailableError,
    resolve_model,
)


def test_exact_model_requires_an_enabled_catalog_entry() -> None:
    catalog = [AvailableModel(value="Qwen3.8-Max", enabled=True)]

    assert resolve_model("Qwen3.8-Max", catalog).resolved == "Qwen3.8-Max"


def test_alias_selects_first_enabled_candidate() -> None:
    catalog = [
        AvailableModel(value="Qwen3.8-Max", enabled=False),
        AvailableModel(value="Qwen3.7-Max", enabled=True),
    ]

    resolution = resolve_model("qwen-auditor", catalog)

    assert resolution.resolved == "Qwen3.7-Max"
    assert resolution.used_fallback is True


def test_exact_model_never_falls_back() -> None:
    with pytest.raises(ModelUnavailableError):
        resolve_model(
            "Qwen3.8-Max",
            [AvailableModel(value="Auto", enabled=True)],
        )
