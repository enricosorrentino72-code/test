# Makefile for Purchase Order Item Streaming Pipeline
# Provides automation for testing, quality checks, and build processes

.PHONY: help install test lint format type-check security coverage clean all
.DEFAULT_GOAL := help

# Variables
PYTHON = python
PIP = pip
SOURCE_DIRS = class utility
TEST_DIRS = tests
COVERAGE_MIN = 80

help: ## Show this help message
	@echo "Purchase Order Item Streaming Pipeline - Build Automation"
	@echo "=========================================================="
	@echo ""
	@echo "Available commands:"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Installation and Setup
install: ## Install all dependencies
	$(PIP) install -r requirements.txt
	pre-commit install

install-dev: ## Install development dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -e .
	pre-commit install

# Testing
test: ## Run all tests
	pytest $(TEST_DIRS) -v

test-unit: ## Run unit tests only
	pytest $(TEST_DIRS)/unit -v -m unit

test-integration: ## Run integration tests only
	pytest $(TEST_DIRS)/integration -v -m integration

test-performance: ## Run performance tests only
	pytest $(TEST_DIRS)/performance -v -m performance

test-cloud: ## Run cloud-specific tests
	pytest $(TEST_DIRS)/cloud -v -m cloud

test-parallel: ## Run tests in parallel
	pytest $(TEST_DIRS) -v -n auto

test-watch: ## Run tests in watch mode
	pytest-watch $(TEST_DIRS)

# Coverage
coverage: ## Run tests with coverage report
	pytest $(TEST_DIRS) --cov=$(SOURCE_DIRS) --cov-report=term-missing --cov-fail-under=$(COVERAGE_MIN)

coverage-html: ## Generate HTML coverage report
	pytest $(TEST_DIRS) --cov=$(SOURCE_DIRS) --cov-report=html --cov-fail-under=$(COVERAGE_MIN)
	@echo "Coverage report generated in htmlcov/index.html"

coverage-xml: ## Generate XML coverage report
	pytest $(TEST_DIRS) --cov=$(SOURCE_DIRS) --cov-report=xml --cov-fail-under=$(COVERAGE_MIN)

coverage-json: ## Generate JSON coverage report
	pytest $(TEST_DIRS) --cov=$(SOURCE_DIRS) --cov-report=json --cov-fail-under=$(COVERAGE_MIN)

coverage-badge: ## Generate coverage badge
	coverage-badge -o coverage.svg -f

# Code Quality
lint: ## Run all linters
	@echo "Running Ruff..."
	ruff check $(SOURCE_DIRS) $(TEST_DIRS)
	@echo "Running Pylint..."
	pylint $(SOURCE_DIRS)
	@echo "Running Flake8..."
	flake8 $(SOURCE_DIRS) $(TEST_DIRS)

lint-fix: ## Run linters with automatic fixes
	@echo "Running Ruff with fixes..."
	ruff check $(SOURCE_DIRS) $(TEST_DIRS) --fix
	@echo "Running isort..."
	isort $(SOURCE_DIRS) $(TEST_DIRS)

format: ## Format code
	@echo "Formatting with Black..."
	black $(SOURCE_DIRS) $(TEST_DIRS)
	@echo "Sorting imports with isort..."
	isort $(SOURCE_DIRS) $(TEST_DIRS)
	@echo "Formatting docstrings..."
	docformatter --in-place --recursive $(SOURCE_DIRS)

format-check: ## Check code formatting
	@echo "Checking Black formatting..."
	black --check $(SOURCE_DIRS) $(TEST_DIRS)
	@echo "Checking import order..."
	isort --check-only $(SOURCE_DIRS) $(TEST_DIRS)

type-check: ## Run type checking
	@echo "Running MyPy..."
	mypy $(SOURCE_DIRS)

# Security
security: ## Run all security scans
	@echo "Running Bandit security scan..."
	bandit -r $(SOURCE_DIRS) -f json -o bandit-report.json
	@echo "Running Safety dependency check..."
	safety check --json --output safety-report.json
	@echo "Running pip-audit..."
	pip-audit --format=json --output=pip-audit-report.json
	@echo "Running Semgrep..."
	semgrep --config=auto --json --output=semgrep-report.json $(SOURCE_DIRS)

security-bandit: ## Run Bandit security scan
	bandit -r $(SOURCE_DIRS) -ll

security-safety: ## Check dependencies for vulnerabilities
	safety check --full-report

security-audit: ## Run pip-audit on dependencies
	pip-audit

# Code Complexity
complexity: ## Check code complexity
	@echo "Running Radon complexity analysis..."
	radon cc $(SOURCE_DIRS) -s -nb --total-average
	@echo "Running Xenon complexity monitoring..."
	xenon $(SOURCE_DIRS) --max-absolute B --max-modules A --max-average A

complexity-report: ## Generate complexity report
	radon cc $(SOURCE_DIRS) -j > radon-report.json
	radon mi $(SOURCE_DIRS) -j > maintainability-report.json

# Documentation
docs: ## Check documentation coverage
	@echo "Checking docstring coverage..."
	interrogate $(SOURCE_DIRS) --fail-under=80 --verbose
	@echo "Checking docstring style..."
	pydocstyle $(SOURCE_DIRS)

docs-coverage: ## Generate documentation coverage report
	interrogate $(SOURCE_DIRS) --generate-badge coverage-docs.svg

# Quality Reports
quality-report: ## Generate comprehensive quality report
	@echo "Generating comprehensive quality report..."
	@mkdir -p reports
	pytest $(TEST_DIRS) --cov=$(SOURCE_DIRS) --cov-report=html:reports/coverage --html=reports/pytest-report.html --self-contained-html
	ruff check $(SOURCE_DIRS) $(TEST_DIRS) --format=json --output-file=reports/ruff-report.json
	bandit -r $(SOURCE_DIRS) -f json -o reports/bandit-report.json
	safety check --json --output reports/safety-report.json
	radon cc $(SOURCE_DIRS) -j > reports/radon-report.json
	@echo "Quality reports generated in reports/ directory"

# Pre-commit
pre-commit: ## Run pre-commit hooks on all files
	pre-commit run --all-files

pre-commit-update: ## Update pre-commit hooks
	pre-commit autoupdate

# Build and Package
build: ## Build package
	$(PYTHON) -m build

clean-build: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/

# Cleaning
clean: ## Clean temporary files and caches
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -rf .tox/

clean-reports: ## Clean generated reports
	rm -rf reports/
	rm -f *-report.json
	rm -f *-report.xml
	rm -f *-report.html
	rm -f coverage.xml
	rm -f coverage.json
	rm -f *.svg

clean-all: clean clean-build clean-reports ## Clean everything

# Comprehensive workflows
check: lint type-check security ## Run all checks without tests
	@echo "All checks completed successfully!"

test-all: test coverage quality-report ## Run all tests with coverage and reports
	@echo "All tests completed successfully!"

quality-check: lint type-check security complexity docs ## Run all quality checks
	@echo "All quality checks completed successfully!"

ci: install test-all quality-check ## Full CI pipeline
	@echo "CI pipeline completed successfully!"

# Development workflow
dev-setup: install-dev pre-commit ## Setup development environment
	@echo "Development environment setup completed!"

dev-check: format lint-fix type-check test-unit ## Quick development checks
	@echo "Development checks completed!"

# Performance
benchmark: ## Run performance benchmarks
	pytest $(TEST_DIRS)/performance --benchmark-only --benchmark-json=benchmark-results.json

profile: ## Profile code performance
	$(PYTHON) -m cProfile -o profile.stats -m pytest $(TEST_DIRS)/performance

# Monitoring
monitor-coverage: ## Monitor test coverage over time
	pytest $(TEST_DIRS) --cov=$(SOURCE_DIRS) --cov-report=term --cov-report=json:coverage-$(shell date +%Y%m%d-%H%M%S).json

monitor-complexity: ## Monitor code complexity over time
	radon cc $(SOURCE_DIRS) -j > complexity-$(shell date +%Y%m%d-%H%M%S).json

# Docker (if needed)
docker-build: ## Build Docker image
	docker build -t purchase-order-pipeline .

docker-test: ## Run tests in Docker
	docker run --rm purchase-order-pipeline make test

# Documentation generation
docs-build: ## Build documentation
	@echo "Building documentation..."
	# Add documentation build commands here if using Sphinx or similar

# Release preparation
release-check: clean test-all quality-check ## Check if ready for release
	@echo "Release readiness check completed!"

# Environment info
env-info: ## Show environment information
	@echo "Python version:"
	$(PYTHON) --version
	@echo ""
	@echo "Pip version:"
	$(PIP) --version
	@echo ""
	@echo "Installed packages:"
	$(PIP) list
	@echo ""
	@echo "Git status:"
	git status --porcelain

# Help for specific topics
help-testing: ## Show testing help
	@echo "Testing Commands:"
	@echo "  make test         - Run all tests"
	@echo "  make test-unit    - Run unit tests only"
	@echo "  make test-integration - Run integration tests"
	@echo "  make coverage     - Run tests with coverage"
	@echo "  make test-parallel - Run tests in parallel"

help-quality: ## Show quality check help
	@echo "Quality Check Commands:"
	@echo "  make lint         - Run all linters"
	@echo "  make format       - Format code"
	@echo "  make type-check   - Run type checking"
	@echo "  make security     - Run security scans"
	@echo "  make complexity   - Check code complexity"

help-ci: ## Show CI/CD help
	@echo "CI/CD Commands:"
	@echo "  make ci           - Full CI pipeline"
	@echo "  make quality-report - Generate quality reports"
	@echo "  make pre-commit   - Run pre-commit hooks"