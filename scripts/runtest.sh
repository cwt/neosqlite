#!/bin/bash

CPU_CORES=$(
  case "$(uname -s)" in
    Darwin) sysctl -n hw.physicalcpu ;;
         *) lscpu | awk '/^Core\(s\) per socket:/ {c=$4} /^Socket\(s\):/ {s=$2; sum += c * s} END {print sum}' ;;
  esac
)

# cleanup
rm -rf */__pycache__ .pytest_cache .coverage

# update existing packages as defined on pyproject.toml
poetry update

# install current project as editable
pip install -q -e .[docs,memory-constrained]

# regenerate poetry.lock based on installed/updated packages
rm -f poetry.lock && poetry lock --no-cache

# run unit tests
PYTHONPATH=. poetry run pytest tests/ -n $CPU_CORES --cov=neosqlite --cov-report=term-missing --cov-fail-under=80


# NX-27017
cd packages/nx_27017

# install current project as editable
pip install -q -e .[speed]

# regenerate poetry.lock based on installed/updated packages
rm -f poetry.lock && poetry lock --no-cache

# run unit tests
PYTHONPATH=. poetry run pytest tests/ -n $CPU_CORES --cov=nx_27017 --cov-report=term-missing

# return to previous path
cd -

