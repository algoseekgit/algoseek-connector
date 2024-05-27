# User installation
.PHONY: install
	poetry install

# Install repository for development
.PHONY: dev-install
dev-install:
	poetry install --with dev,docs
	poetry run pre-commit install

# Run unit tests
.PHONY: unit-tests
unit-tests:
	poetry run pytest

# Run integration tests
.PHONY: integration-tests
integration-tests:
	poetry run pytest tests/integration

# Run learning tests
.PHONY: learning-tests
learning-tests:
	poetry run pytest tests/learning

# Run all tests and check code coverage.
.PHONY: coverage
coverage:
	poetry run pytest --cov=src tests
	poetry run coverage html

.PHONY: format
format:
	poetry run ruff check --fix
	poetry run ruff format
