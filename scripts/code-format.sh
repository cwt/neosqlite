#!/bin/bash
set -eu

poetry run black -t py312 -l 80 .

# Remove trailing whitespace in all project .py files
if [[ "${OSTYPE:-}" == "darwin"* ]]; then
    find . -type d \( -name .hg -o -name .git -o -name .venv -o -name venv -o -name build -o -name dist -o -name .mypy_cache -o -name .ruff_cache -o -name .pytest_cache -o -name __pycache__ \) -prune -o -type f -name "*.py" -exec sed -i '' 's/[[:space:]]*$//' {} +
else
    find . -type d \( -name .hg -o -name .git -o -name .venv -o -name venv -o -name build -o -name dist -o -name .mypy_cache -o -name .ruff_cache -o -name .pytest_cache -o -name __pycache__ \) -prune -o -type f -name "*.py" -exec sed -i 's/[[:space:]]*$//' {} +
fi
