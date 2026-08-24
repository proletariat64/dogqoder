"""Canonical shared-workspace overlap classification."""

from pathlib import Path
from typing import Literal

type WorkspaceRelation = Literal["same", "ancestor", "descendant"]


def canonical_workspace(cwd: Path) -> Path:
    """Resolve one existing directory, including every symlink component."""

    try:
        canonical = cwd.resolve(strict=True)
    except (OSError, RuntimeError):
        raise ValueError("Workspace must be an existing directory.") from None
    if not canonical.is_dir():
        raise ValueError("Workspace must be an existing directory.")
    return canonical


def classify_workspace_overlap(
    requested_cwd: Path,
    live_cwd: Path,
) -> WorkspaceRelation | None:
    """Describe a live workspace relative to the requested workspace."""

    requested = canonical_workspace(requested_cwd)
    live = canonical_workspace(live_cwd)
    if requested == live:
        return "same"
    if live in requested.parents:
        return "ancestor"
    if requested in live.parents:
        return "descendant"
    return None
