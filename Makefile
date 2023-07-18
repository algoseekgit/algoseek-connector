# Install repository for development
.PHONY: dev-install
dev-install:
	poetry install --with dev
	poetry pre-commit install

# Run unit tests
.PHONY: tests
tests:
	poetry run pytest

# Run unit tests, integration tests and check code coverage.
.PHONY: coverage
coverage:
	poetry run pytest --cov=src tests && coverage html
