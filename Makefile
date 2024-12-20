.PHONY: dev-install
dev-install:
	type uv && uv venv || >&2 echo "Error: uv not found in user PATH." && \
	uv pip install --editable .[dev,docs]
	uv run pre-commit install

# Run unit tests
.PHONY: unit-tests
unit-tests:
	uv run pytest tests/unit

# Run integration tests
.PHONY: integration-tests
integration-tests:
	uv run pytest tests/integration

# Run learning tests
.PHONY: learning-tests
learning-tests:
	uv run pytest tests/learning

# Run all tests and check code coverage.
.PHONY: coverage
coverage:
	uv run pytest --cov=src tests
	uv run coverage html

.PHONY: format
format:
	uv run ruff check --fix
	uv run ruff format

.PHONY: clean
clean:
	rm -rf .venv
	find -iname "*.pyc" -delete