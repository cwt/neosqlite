Remove-Item -Recurse -Force *\__pycache__
Remove-Item -Recurse -Force .pytest_cache
poetry update
poetry install
poetry run pytest --cov=neosqlite --cov-report=term-missing --cov-fail-under=80
