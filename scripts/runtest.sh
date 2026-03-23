#!/bin/bash

CPU_CORES=$(lscpu | awk '/^Core\(s\) per socket:/ {c=$4} /^Socket\(s\):/ {s=$2} END {print c * s}')

rm -rf */__pycache__ .pytest_cache .coverage
poetry update
PYTHONPATH=. poetry run pytest tests/ -n $CPU_CORES --cov=neosqlite --cov-report=term-missing --cov-fail-under=80

cd packages/nx_27017
poetry lock
poetry install -E speed
poetry run pytest tests/ -n $CPU_CORES --cov=nx_27017 --cov-report=term-missing
cd -
