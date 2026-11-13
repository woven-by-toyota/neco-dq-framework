# Contributing to neco_dq

Thanks for taking the time to contribute. This guide covers how to set up the project, the conventions we follow, and how to get a change merged.

## Development setup

You need Python 3.11 or newer. Create the virtual environment with a 3.11+ interpreter.

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager).

```bash
git clone https://github.com/woven-by-toyota/neco-dq-framework.git
cd neco-dq-framework
uv sync --extra dev --frozen
pre-commit install
```

Use `uv run <command>` to run tools inside the managed environment (e.g. `uv run pytest`, `uv run ruff check .`). The test suite uses PySpark, which the `dev` extra pulls in, so you can run everything locally without a Databricks connection.

## Code style

We use [Ruff](https://github.com/astral-sh/ruff) for linting and formatting. Run both before opening a PR:

```bash
uv run ruff check .
uv run ruff format --check .
```

To fix what can be fixed automatically:

```bash
uv run ruff check --fix .
uv run ruff format .
```

A few conventions worth knowing:

- 120-character lines, double quotes, 4-space indent, Google-style docstrings
- Prefer `X | None` over `Optional[X]` in new code
- No commented-out code, no `TODO`/`FIXME` in new code
- Keep checks generic and reusable, with no hardcoded table, catalog, or column names

## Testing

```bash
uv run pytest tests/ -v
```

Add tests for any behavior you change. Test names follow `test_<action>_<object>_<qualifier>`.

## Pull requests

1. Fork the repository and create a branch from `main`.
2. Make your change, with tests.
3. Run the linter, formatter, and tests.
4. Open a pull request describing what changed and why.

A maintainer reviews and merges. One approval is required.

## Commit messages

We follow Conventional Commits:

```
<type>: <description>
```

Valid types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `build`, `ci`, `revert`.

## Design principles

This is a framework, not a script collection. When you add or change a check, keep these in mind:

- The orchestrator never computes metrics. Suites do.
- Suites are stateless and have no side effects.
- Policy runs after execution, never during.
- Results carry statistics, counts, and query templates, never raw row values or PII.

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you agree to uphold it.
