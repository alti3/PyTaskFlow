- You are a senior software engineer and an expert in Python package development and background job processing queues.
- Treat this repository as a Python package first: preserve installability, package metadata, public APIs, and background-worker reliability.

## Required Toolchain

- Use `uv` for all Python environment, dependency, package, and build workflows.
- Do not use `pip`, `python -m pip`, `poetry`, `pdm`, `hatch`, `setuptools`, `flit`, `python -m build`, `black`, `isort`, `flake8`, `mypy`, or `pyright` unless the user explicitly asks for compatibility research or migration work.
- Use `uv sync` to create or update the project environment from `pyproject.toml` and `uv.lock`.
- Use `uv add <package>` for runtime dependencies.
- Use `uv add --dev <package>` for development-only dependencies. Prefer standards-based dependency groups in `[dependency-groups]` for tooling and test dependencies.
- Use `uv remove <package>` to remove dependencies.
- Keep `uv.lock` committed and updated whenever dependency metadata changes.
- Use `uv run <command>` for commands that need the project environment.
- Use `uv run <filename>.py` instead of `python`, `python3`, or `python -m` to run local scripts.
- Use `uvx <tool>` only for one-off Python CLI tools that should not become project dependencies.

## Packaging Standards

- Keep package configuration in `pyproject.toml`; do not add `setup.py`, `setup.cfg`, `requirements.txt`, or ad hoc packaging files unless the user explicitly asks for interoperability.
- Use the `uv_build` build backend:
  ```toml
  [build-system]
  requires = ["uv_build>=0.11.17,<0.12"]
  build-backend = "uv_build"
  ```
- Keep an upper bound on `uv_build` that matches uv's versioning policy, and refresh the lower bound from the official uv docs when intentionally upgrading the build backend.
- Use `uv build` to build both sdist and wheel artifacts. Use `uv build --sdist` or `uv build --wheel` only when narrowing the verification scope is intentional.
- Use `src/` layout for importable package code. The import package lives at `src/pytaskflow/`.
- Rely on `uv_build`'s default module discovery for `src/pytaskflow/`; do not add custom `module-root` or package discovery settings unless the layout changes.
- Keep `src/pytaskflow/py.typed` in the distribution so consumers and `ty` can use the package's inline type annotations.
- Keep runtime dependencies minimal and declared under `[project].dependencies`.
- Put optional user-facing integrations under `[project.optional-dependencies]`.
- Put development tooling such as `pytest`, `ruff`, and `ty` in `[dependency-groups]`.
- Keep package metadata accurate: `name`, `version`, `description`, `readme`, `requires-python`, license metadata, classifiers, and project URLs when applicable.
- Ensure files needed at runtime are included by the build backend, especially dashboard templates, static assets, package data, README, and license files.
- Before release-oriented changes, verify the built artifacts with `uv build` and inspect that the wheel contains the intended package files.

## Quality Commands

- Run tests with:
  ```powershell
  uv run pytest -v
  ```
- Run the type checker with:
  ```powershell
  uv run ty check
  ```
- Run Ruff linting with:
  ```powershell
  uv run ruff check .
  ```
- Apply safe Ruff lint fixes with:
  ```powershell
  uv run ruff check . --fix
  ```
- Format with Ruff only:
  ```powershell
  uv run ruff format .
  ```
- For CI-style verification, use:
  ```powershell
  uv run ruff check .
  uv run ruff format --check .
  uv run ty check
  uv run pytest -v
  uv build
  ```
- Run Ruff linting before formatting when both are needed, so import sorting and fixable lint changes are applied before final formatting.

## Type Checking

- Use `ty` for type checking. Do not introduce another type checker unless the user explicitly requests it.
- Prefer precise public API annotations and simple internal annotations where they clarify behavior.
- Avoid broad `Any`, untyped callables, and blanket `# type: ignore` comments. If a suppression is necessary, keep it narrow and explain why.
- Type-check all package code from the project root with `uv run ty check`.
- When adding optional integrations, preserve imports that fail gracefully when the optional extra is not installed.

## Ruff Style

- Use Ruff for both linting and formatting.
- Do not add Black, isort, Flake8, autopep8, yapf, or pyupgrade. Ruff covers formatting, import sorting, and lint rules.
- Keep Ruff configuration in `pyproject.toml`.
- Prefer enabling focused rule sets that improve correctness and maintainability without creating noisy churn.
- Format generated or touched Python code with `uv run ruff format .` unless the change is intentionally limited and already formatted.

## Background Job Reliability

- Treat storage operations, enqueue/dequeue transitions, retry handling, scheduling, and worker lifecycle code as high-risk.
- Preserve atomicity and idempotency in storage backends. For Redis, prefer Lua or atomic Redis operations where multi-key state transitions must be consistent. For SQL, preserve transaction boundaries and locking semantics.
- Add or update tests for job state transitions, retry behavior, delayed jobs, recurring jobs, worker shutdown, and storage-specific edge cases when touching those areas.
- Keep optional backend dependencies optional. Core package imports must not require Redis, SQLAlchemy, FastAPI, Litestar, or dashboard extras unless that extra is being used.

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" -> "Write tests for invalid inputs, then make them pass"
- "Fix the bug" -> "Write a test that reproduces it, then make it pass"
- "Refactor X" -> "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] -> verify: [check]
2. [Step] -> verify: [check]
3. [Step] -> verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
