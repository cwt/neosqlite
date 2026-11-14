#!/bin/bash

# Core rules:
#   I    - Import organization (isort compatibility)
#   F    - Pyflakes (unused imports, undefined names, etc.)
#   UP   - Python 3.12 syntax and stdlib modernizations
#   PERF - Performance idioms
#   FURB - Refurb readability and correctness rules
poetry run ruff check \
    --select=I,F,UP,PERF,FURB \
    --target-version=py312 \
    --fix .

