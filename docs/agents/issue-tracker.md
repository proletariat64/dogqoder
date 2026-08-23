# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`.
- **Read an issue**: `gh issue view <number> --comments`.
- **List issues**: `gh issue list` with appropriate label and state filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`.
- **Apply or remove labels**: use `gh issue edit`.
- **Close an issue**: `gh issue close <number> --comment "..."`.

Infer the repository from the current clone and its Git remote.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## Publishing

When a skill says “publish to the issue tracker,” create a GitHub issue.

When a skill says “fetch the relevant ticket,” run:

```bash
gh issue view <number> --comments
```

## Blocking relationships

Use GitHub native issue dependencies when available. Create tickets in dependency order so blocker identifiers exist before dependent tickets are published.

If native dependencies are unavailable, add this line near the top of the dependent issue:

```text
Blocked by: #<number>, #<number>
```

A ticket becomes unblocked when all referenced blockers are closed.
