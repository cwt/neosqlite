#!/bin/bash

CPU_CORES=$(lscpu | awk '/^Core\(s\) per socket:/ {c=$4} /^Socket\(s\):/ {s=$2} END {print c * s}')

rm -rf */__pycache__ .pytest_cache
poetry update
poetry install
poetry run pytest tests/ -n $CPU_CORES --cov=neosqlite --cov-report=term-missing --cov-fail-under=85
