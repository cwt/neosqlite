#!/bin/bash

rm -rf */__pycache__ .pytest_cache
poetry update
poetry install
poetry run pytest --cov=neosqlite --cov-report=term-missing --cov-fail-under=85
