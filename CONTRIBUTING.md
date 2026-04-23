# Contributing to datalathe-client-python

Thanks for your interest in contributing! This is the Python client for the [Datalathe](https://datalathe.com) API.

## Getting set up

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/<your-fork>/datalathe-client-python.git
cd datalathe-client-python
uv sync --extra dev
```

Run the test suite:

```bash
uv run pytest
```

Tests use [`responses`](https://github.com/getsentry/responses) to mock the Datalathe HTTP API, so no running backend is required.

## Supported Python versions

Python 3.10 and newer. CI runs the test suite against 3.10, 3.11, 3.12, and 3.13 — please make sure your change works on all of them.

## Making a change

1. Fork the repo and create a branch off `main`.
2. Make your change. Add or update tests in `tests/` to cover it.
3. Run `uv run pytest` locally and confirm everything passes.
4. Open a PR against `DataLathe/datalathe-client-python:main`. CI will run automatically.

### Style

- Match the surrounding code. The codebase is plain, explicit Python — no metaclasses, minimal abstractions.
- Public API lives in `src/datalathe/__init__.py`. If you add a new public symbol, export it there and document it in `README.md`.
- Prefer small, focused PRs. If a change touches more than one area (e.g. a new feature *and* unrelated cleanup), split it.

### Commit messages

Short imperative subject line (e.g. `Add S3 partition support to create_chip`). Reference issues with `Fixes #123` in the body when applicable.

## Reporting bugs

Open an issue with:

- What you ran (minimal reproducing snippet preferred)
- What you expected to happen
- What actually happened (including the full exception + traceback if any)
- `datalathe` version (`pip show datalathe`) and Python version

## Releases

Releases are cut by the maintainers. Publishing to PyPI is handled automatically by `.github/workflows/publish.yml` when a GitHub Release is published — contributors don't need to do anything release-related as part of a PR.

## License

By contributing, you agree that your contributions will be licensed under the project's MIT License.
