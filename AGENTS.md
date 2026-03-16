# Repository Guidelines

## Collaboration First
- If any concern, ambiguity, or scope conflict appears, discuss it with maintainers before implementing changes.
- Do not proceed with uncertain or conflicting work without explicit alignment.

## Project Structure & Module Organization
- `src/` contains platform code by domain:
  - `src/spark/` realtime Spark jobs and SQL builders.
  - `src/generator/` bounded-run event generator and lifecycle utilities.
  - `src/scripts/` acceptance runners and contract verifiers.
  - `src/trino/` semantic serving SQL.
- `tests/` mirrors runtime modules with `test_*.py` coverage for contracts, SQL, generator behavior, and verifiers.
- `docs/` is the source of truth for scope and data contracts; validate changes against relevant contract docs before coding.
- `artifacts/` stores generated run evidence and acceptance outputs.

## Build, Test, and Development Commands
- `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt` installs dependencies.
- `.venv/bin/python -m pytest` runs the full unit/contract test suite.
- `.venv/bin/python -m pytest tests/test_rt_action_decisioning.py` runs a focused test file.
- `bash src/scripts/run_realtime_signoff_acceptance.sh` executes realtime acceptance checks.
- `docker compose up -d` starts local infra used by streaming/serving workflows.

## Coding Style & Naming Conventions
- Use Python 3.10, 4-space indentation, and explicit type-safe logic where practical.
- Keep modules focused on one contract surface (e.g., CDC upsert, content aggregation, decisioning).
- Follow existing naming:
  - files: `snake_case.py`
  - tests: `tests/test_<module_or_behavior>.py`
  - script entrypoints: `run_<domain>_acceptance.sh` or `verify_<contract>.py`
- Prefer deterministic, contract-first implementations over implicit behavior.

## Testing Guidelines
- Frameworks: `pytest` with `unittest`-style test classes.
- Add/adjust tests for every behavior or contract change in `src/` and SQL outputs.
- Keep tests deterministic (fixed seeds, stable IDs, explicit timestamps when needed).
- Before PR: run full `.venv/bin/python -m pytest` and the relevant acceptance script for touched domains.

## Commit & Pull Request Guidelines
- Follow repository commit style: `type: concise summary` (examples in history: `docs: ...`, `refactor: ...`).
- Keep commits scoped to one change theme and include docs/tests updates when behavior changes.
- PRs should include:
  - what changed and why,
  - linked issue/scope reference,
  - verification evidence (pytest/acceptance commands and results),
  - artifact paths or screenshots when dashboard/serving outputs are affected.
