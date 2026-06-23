#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}"

FOCUSED_TESTS=(
  test/test_gsg.py
  test/test_analysis.py
  test/test_gal.py
  test/test_tv_api.py
  test/test_geo_api.py
  test/test_others_db.py
  test/test_query_nowtime.py
  test/test_cli.py
)

if [[ "${1:-}" == "--all" ]]; then
  shift
  TEST_ARGS=(test)
else
  TEST_ARGS=("${FOCUSED_TESTS[@]}")
fi

uv run --with-requirements requirements.txt --with pytest pytest "${TEST_ARGS[@]}" "$@"
