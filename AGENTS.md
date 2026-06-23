# Repository Guidelines

## Project Structure & Module Organization

`newsfeed/` contains the installable Python package. The CLI entry point is `newsfeed/__main__.py`; GDELT API query helpers live in `newsfeed/news/apis/`, database query implementations in `newsfeed/news/db/`, and shared utilities such as caching, async downloads, full-text extraction, and CAMEO data in `newsfeed/utils/`. Tests are in `test/` and currently exercise API, database, cache, incremental, async, and full-text behavior. Codex skill assets are under `skills/newsfeed/`; keep these separate from runtime package code unless a change explicitly updates the skill.

## Build, Test, and Development Commands

Create a Python 3.11+ environment, then install dependencies:

```bash
pip install -r requirements.txt
pip install -e .
```

Run the CLI locally with:

```bash
python -m newsfeed --db EVENT --version V2 --start 2021-01-01-00-00-00 --end 2021-01-02-00-00-00
```

Run tests with `pytest`. CI also installs `flake8` and runs:

```bash
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
```

Build release artifacts with `python -m build` when preparing package publication.

## Coding Style & Naming Conventions

Use standard Python style with 4-space indentation, `snake_case` for functions and variables, and `CamelCase` for query classes such as `EventV2` and `GKGV1`. Keep public imports and examples compatible with the existing package paths. Prefer explicit, small functions for parsing, downloading, and transformation logic. The CI lint configuration allows lines up to 127 characters, but shorter lines are preferred when readability improves.

## Testing Guidelines

Name test files `test/test_*.py` and test functions `test_*`. Tests may contact GDELT or article URLs, so handle network failures deliberately rather than making unrelated assertions brittle. For cache or incremental features, verify both the returned `pandas.DataFrame` shape/type and the expected reuse behavior. Run targeted checks with `pytest test/test_db.py` or a single test path before running the full suite.

## Commit & Pull Request Guidelines

Recent history uses short messages such as `update aio` and `fix download fultext error`. Keep commits concise, imperative, and more specific when possible, for example `fix fulltext download errors` or `add async GKG test`. Pull requests should include a short summary, affected commands or APIs, test results, and notes about network-dependent behavior. Include CLI examples or screenshots only when user-facing output changes.

## Security & Configuration Tips

Do not commit downloaded datasets, cache files, credentials, or generated article outputs. Keep package dependency changes synchronized between `requirements.txt`, `setup.py`, and `pixi.toml` when applicable.
