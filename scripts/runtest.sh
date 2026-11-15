#!/bin/bash
export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring

OS_NAME=$(uname -s)
case "$OS_NAME" in
  Darwin)  CPU_CORES=$(sysctl -n hw.physicalcpu 2>/dev/null || true);;
  Linux)   CPU_CORES=$(lscpu 2>/dev/null | awk '/^Core\(s\) per socket:/ {c=$4} /^Socket\(s\):/ {s=$2; sum += c * s} END {print sum}');;
  *)       CPU_CORES=$(poetry run python3 -c "import multiprocessing; print(multiprocessing.cpu_count())");;
esac

# Minimal container images may not ship lscpu; fall back to getconf.
if ! [[ "$CPU_CORES" =~ ^[1-9][0-9]*$ ]]; then
  CPU_CORES=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)
fi

# cleanup
rm -rf */__pycache__ .pytest_cache .coverage

# update existing packages as defined on pyproject.toml
poetry update

# install current project as editable
poetry run pip install -q -e .[docs,memory-constrained]

# regenerate poetry.lock based on installed/updated packages
rm -f poetry.lock && poetry lock --no-cache

# run unit tests
PYTHONPATH=. poetry run pytest tests/ -n $CPU_CORES --cov=neosqlite --cov-report=term-missing --cov-fail-under=80


# NX-27017
cd packages/nx_27017

# Install both the local NeoSQLite tree and nx-27017 as editable, plus the
# test runner. A plain `pip install -e .[speed]` does not install the dev
# group, so `poetry run pytest` previously failed with "Command not found".
poetry run pip install -q \
    -e ../.. \
    -e .[speed] \
    pytest pytest-asyncio pytest-cov pytest-xdist

# regenerate poetry.lock based on installed/updated packages
rm -f poetry.lock && poetry lock --no-cache

# run unit tests
PYTHONPATH=. poetry run pytest tests/ -n $CPU_CORES --cov=nx_27017 --cov-report=term-missing

# return to previous path
cd -

