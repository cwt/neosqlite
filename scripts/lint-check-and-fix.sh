#!/bin/bash

# Core rules:
#   I    - Import organization (isort compatibility)
#   F    - Pyflakes (unused imports, undefined names, etc.)
#   UP   - Python 3.12 syntax and stdlib modernizations
#   FURB - Refurb readability and correctness rules
poetry run ruff check \
    --select=I,F,UP,FURB \
    --target-version=py312 \
    --fix .

# Performance rules are enforced on the shipped library, not on examples,
# stress scripts, or tests where readability and setup code dominate.
poetry run ruff check \
    --select=PERF \
    --target-version=py312 \
    neosqlite packages/nx_27017/src

