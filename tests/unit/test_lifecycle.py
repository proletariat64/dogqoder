import pytest

from qworker.lifecycle import WorkerStateReducer


def test_reducer_enforces_explicit_lifecycle_transitions() -> None:
    reducer = WorkerStateReducer("starting")

    assert reducer.transition("running") == "running"
    assert reducer.transition("requires_action") == "requires_action"
    assert reducer.transition("running") == "running"
    assert reducer.transition("completed") == "completed"

    with pytest.raises(ValueError, match="completed -> running"):
        reducer.transition("running")

    resumed = WorkerStateReducer("lost")
    assert resumed.transition("starting") == "starting"
