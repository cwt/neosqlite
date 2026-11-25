#!/bin/bash
set -eu

export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring

UPDATE_LOCKS=false
for arg in "$@"; do
  case "$arg" in
    --update-locks)
      UPDATE_LOCKS=true
      ;;
    -h|--help)
      echo "Usage: $0 [--update-locks]"
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 1
      ;;
  esac
done

OS_NAME=$(uname -s)
CPU_CORES=""
case "$OS_NAME" in
  Darwin)  CPU_CORES=$(sysctl -n hw.physicalcpu 2>/dev/null || true);;
  Linux)   CPU_CORES=$(lscpu 2>/dev/null | awk '/^Core\(s\) per socket:/ {c=$4} /^Socket\(s\):/ {s=$2; sum += c * s} END {print sum}' || true);;
  *)       CPU_CORES=$(poetry run python3 -c "import multiprocessing; print(multiprocessing.cpu_count())" 2>/dev/null || true);;
esac

# Minimal container images may not ship lscpu; fall back to getconf.
if ! [[ "$CPU_CORES" =~ ^[1-9][0-9]*$ ]]; then
  CPU_CORES=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)
fi

# cleanup
rm -rf ./*/__pycache__ .pytest_cache .coverage

if [ "$UPDATE_LOCKS" = true ]; then
  # update existing packages as defined on pyproject.toml
  poetry update
fi

# install current project as editable
poetry run pip install -q -e ".[docs,memory-constrained]"

# install jsbox if a prebuilt wheel is available for this platform
if ! poetry run pip install -q --only-binary=jsbox -e ".[js]" 2>/dev/null; then
  echo "Note: prebuilt jsbox wheel not available for this platform; skipping jsbox."
fi

if [ "$UPDATE_LOCKS" = true ]; then
  # regenerate poetry.lock based on installed/updated packages
  rm -f poetry.lock && poetry lock --no-cache
fi

# run unit tests
PYTHONPATH=. poetry run pytest tests/ -n "$CPU_CORES" --cov=neosqlite --cov-report=term-missing --cov-fail-under=80


# NX-27017
cd packages/nx_27017

# Install both the local NeoSQLite tree and nx-27017 as editable, plus the
# test runner. A plain `pip install -e .[speed]` does not install the dev
# group, so `poetry run pytest` previously failed with "Command not found".
poetry run pip install -q \
    -e ../.. \
    -e ".[speed]" \
    pytest pytest-asyncio pytest-cov pytest-xdist

if [ "$UPDATE_LOCKS" = true ]; then
  # regenerate poetry.lock based on installed/updated packages
  rm -f poetry.lock && poetry lock --no-cache
fi

# run unit tests
PYTHONPATH=. poetry run pytest tests/ -n "$CPU_CORES" --cov=nx_27017 --cov-report=term-missing

# return to previous path
cd - >/dev/null

