# Repository Guidelines

## Layout

Core package: `src/qworker/`. CLI lives `cli.py`; supervisor, lifecycle, RPC,
storage, SDK adapter, and worker policies stay nearby. Tests live `tests/`:
unit tests in `tests/unit/`, integration tests in `tests/integration/`,
failure cases in `tests/failure/`. Test doubles: `tests/fakes.py`.

Codex worker skill: `skills/qoder-worker/`; JSON command contract:
`skills/qoder-worker/references/contract.md`. Design truth:
`docs/superpowers/specs/`. SDK evidence: `docs/research/`. Keep experiments in
`spikes/`; no production behavior depends on them.

## Setup, Run, Check

```bash
uv sync                         # install locked Python 3.12 environment
uv run qworker doctor --json    # inspect local worker runtime
uv run pytest -q                # normal suite; excludes real_qoder
uv run pytest tests/unit -q     # fast unit loop
uv run ruff check src tests     # lint
uv run mypy src                 # strict types
```

Real-Qoder tests spend credits and need `QODER_PERSONAL_ACCESS_TOKEN`:

```bash
uv run pytest -m real_qoder -q
```

## Code Shape

Python 3.12. Four-space indentation. Ruff owns formatting and lint rules;
Mypy runs strict. Use `snake_case` for modules, functions, variables;
`PascalCase` for classes; `UPPER_CASE` for constants. Keep CLI output stable
JSON when `--json` requested. Model lifecycle transitions, event schemas,
persistence, and permission boundaries explicitly; no hidden mutable global
state.

## Tests

Name files `test_<area>.py`; tests `test_<behavior>`. Add regression test with
every bug fix. Unit tests avoid Qoder network calls. Mark credentialed tests
`@pytest.mark.real_qoder`; keep them opt-in, workspace-safe, clear about credit
use. Run narrow test first, then `uv run pytest -q` before review.

## Changes and Reviews

Use conventional commits: `feat:`, `fix:`, `test:`, `docs:`, `refactor:`,
`chore:`. Recent style: `fix: make qworker commands self-starting`.

PR body: intent, behavior change, commands run, linked issue. Include JSON
examples for CLI-contract changes. Call out supervisor recovery, shared
workspace writes, schema migrations, permissions, or real-Qoder coverage.

## Agent Notes

Issues/specs: read `docs/agents/issue-tracker.md`. Labels: read
`docs/agents/triage-labels.md`. Domain docs: read `docs/agents/domain.md`.
Never commit secrets, tokens, local SQLite data, or `.code-review-graph/`.
