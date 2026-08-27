# Makefile for neco_dq development and testing

PYTHON := python3
VENV := .venv
PYTEST := uv run pytest
VERSION := $(shell grep -E '^__version__' neco_dq/__init__.py | cut -d'"' -f2)

.PHONY: help venv test test-html test-cov test-unit test-allure allure-serve clean build publish version

help:
	@echo "Available targets:"
	@echo ""
	@echo "Testing:"
	@echo "  venv          - Create virtualenv with all dev dependencies (including PySpark)"
	@echo "  test          - Run all tests"
	@echo "  test-html     - Run all tests with HTML report"
	@echo "  test-cov      - Run tests with coverage report"
	@echo "  test-unit     - Run only unit tests (no Spark required)"
	@echo "  test-allure   - Run tests with Allure report"
	@echo "  allure-serve  - Open Allure report in browser"
	@echo ""
	@echo "Building & Publishing:"
	@echo "  version       - Show current version"
	@echo "  build         - Build wheel (validates with ruff first)"
	@echo "  publish       - Build and publish to PyPI"
	@echo "  bump-patch    - Bump patch version (0.0.X)"
	@echo "  bump-minor    - Bump minor version (0.X.0)"
	@echo "  bump-major    - Bump major version (X.0.0)"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean         - Remove virtualenv, cache files, and build artifacts"

# Install all dev dependencies via uv
venv:
	@echo "Installing dev dependencies with uv..."
	uv sync --extra dev --frozen
	@echo ""
	@echo "Environment ready. Run 'make test' to execute tests"

# Run all tests
test: venv
	$(PYTEST) tests/ -v

# Run all tests with HTML report
test-html: venv
	$(PYTEST) tests/ -v --html=reports/report.html --self-contained-html
	@echo ""
	@echo "HTML report: reports/report.html"

# Run tests with coverage
test-cov: venv
	$(PYTEST) tests/ -v --cov=neco_dq --cov-report=term-missing --cov-report=html

# Run only unit tests (pure functions, no Spark)
test-unit: venv
	$(PYTEST) tests/ -v -m "not spark"

# Run tests with Allure report
test-allure: venv
	$(PYTEST) tests/ -v --alluredir=reports/allure-results
	@echo ""
	@echo "Allure results: reports/allure-results/"
	@echo "Run 'make allure-serve' to view the report"

# Open Allure report in browser
allure-serve:
	@if command -v allure &> /dev/null; then \
		allure serve reports/allure-results; \
	else \
		echo "Allure CLI not installed. Install with: brew install allure"; \
	fi

# Clean up
clean:
	rm -rf $(VENV)
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf reports
	rm -rf .allure
	rm -rf .coverage
	rm -rf dist build *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Show current version
version:
	@echo "neco-dq-framework v$(VERSION)"

# Build wheel (validates with ruff first)
build: venv
	@echo "Validating with ruff..."
	uv run ruff check neco_dq/
	uv run ruff format --check neco_dq/
	@echo ""
	@echo "Building wheel..."
	uv build --wheel
	@echo ""
	@echo "Built: dist/neco_dq_framework-$(VERSION)-py3-none-any.whl"

# Publish to PyPI (requires PYPI_TOKEN) — use release.sh for full release workflow
publish: build
	@if [ -z "$$PYPI_TOKEN" ]; then \
		echo "Error: PYPI_TOKEN environment variable not set"; \
		exit 1; \
	fi
	@echo "Publishing to PyPI..."
	uv publish --username __token__ --password $$PYPI_TOKEN
	@echo ""
	@echo "Published: neco-dq-framework v$(VERSION)"

# Bump version (patch/minor/major)
bump-patch:
	bump-my-version bump patch

bump-minor:
	bump-my-version bump minor

bump-major:
	bump-my-version bump major
