#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PKG_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Check prerequisites
command -v python3 >/dev/null 2>&1 || { echo "python3 is required"; exit 1; }
command -v duckdb >/dev/null 2>&1 || { echo "duckdb CLI is required"; exit 1; }

export ICEBERG_FIXTURES_DIR=$(mktemp -d)
echo "==> Fixtures directory: $ICEBERG_FIXTURES_DIR"

cleanup() {
  echo "==> Cleaning up $ICEBERG_FIXTURES_DIR"
  rm -rf "$ICEBERG_FIXTURES_DIR"
}
trap cleanup EXIT

echo "==> Generating Iceberg test fixtures with Elixir + DuckDB"
cd "$PKG_DIR"
MIX_ENV=test mix run test/integration/python_fixtures.exs

echo "==> Setting up Python environment"
PYTHON_DIR="$SCRIPT_DIR/python"
cd "$PYTHON_DIR"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

.venv/bin/pip install -q -r requirements.txt

echo "==> Running PyIceberg validation tests"
.venv/bin/pytest -v

echo "==> All validation tests passed!"
