# Code Quality Standards & Guidelines

This document outlines the comprehensive code quality standards, static analysis tools, and best practices for the Purchase Order Item Streaming Pipeline project.

## 🚀 Quick Setup Guide

### Prerequisites & Environment Setup

#### 1. **Virtual Environment (Required)**
```bash
# Windows
python -m venv venv
venv\Scripts\activate.bat  # CMD
# or
venv\Scripts\Activate.ps1  # PowerShell

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

#### 2. **Install Dependencies**
```bash
# Install all tools (after activating virtual environment)
pip install -r requirements.txt

# Verify installation
pip list | grep -E "ruff|black|mypy|bandit|safety"  # Linux/Mac
pip list | findstr "ruff black mypy bandit safety"   # Windows
```

#### 3. **Setup Pre-commit Hooks**
```bash
# Install pre-commit hooks (one-time setup)
pre-commit install

# Verify setup
pre-commit --version
git config --get-regexp 'hooks\..*'
```

### Running Static Analysis

#### **Option 1: Automatic (Recommended)**
```bash
# Runs ALL checks automatically on commit
git add .
git commit -m "Your message"
# Pre-commit hooks run automatically here
```

#### **Option 2: Manual Command-Line**
```bash
# Run all checks at once
pre-commit run --all-files

# Or run individual tools:
python -m ruff check class utility tests --fix      # Linting
python -m black class utility tests                 # Formatting
python -m mypy class utility                        # Type checking
python -m bandit -r class utility -ll              # Security
python -m safety check                             # Dependencies
python -m pip-audit                                # Supply chain
```

#### **Option 3: Using Makefile (Linux/Mac/WSL)**
```bash
make quality-check    # Run all checks
make lint            # Linting only
make format          # Formatting only
make security        # Security only
make type-check      # Type checking only
```

#### **Option 4: Windows Batch Script**
Create `quality-check.bat`:
```batch
@echo off
echo [1/6] Running Ruff linter...
python -m ruff check class utility tests --fix

echo [2/6] Running Black formatter...
python -m black class utility tests

echo [3/6] Running isort import sorter...
python -m isort class utility tests

echo [4/6] Running MyPy type checker...
python -m mypy class utility

echo [5/6] Running Bandit security scan...
python -m bandit -r class utility -ll

echo [6/6] Running Safety dependency check...
python -m safety check

echo.
echo ✅ All quality checks completed!
pause
```

## 📊 Static Analysis Reports

The project generates comprehensive reports in multiple formats for different use cases:

### **Report Types & Formats**

#### **1. Security Analysis Reports**
```bash
# Generate security reports
python -m bandit -r class utility -f json -o bandit-report.json    # JSON format
python -m bandit -r class utility -f html -o bandit-report.html    # HTML format
python -m safety check --json > safety-report.json                # Vulnerability scan
python -m pip-audit --format=json --output=pip-audit-report.json   # Supply chain
```

#### **2. Code Quality Reports**
```bash
# Generate linting reports
python -m ruff check class utility --output-format=json > ruff-report.json
python -m pylint class utility --output-format=json > pylint-report.json
python -m mypy class utility --json-report mypy-report/
```

#### **3. Coverage & Test Reports**
```bash
# Generate test and coverage reports
pytest --cov=class --cov-report=html:reports/coverage --html=reports/pytest-report.html
pytest --cov=class --cov-report=json:coverage.json --cov-report=xml:coverage.xml
```

#### **4. Complexity Analysis Reports**
```bash
# Generate complexity reports
python -m radon cc class utility -j > radon-complexity.json
python -m radon mi class utility -j > radon-maintainability.json
```

### **Automated Report Generation**

#### **Using Makefile (Linux/Mac/WSL)**
```bash
# Generate all reports at once
make quality-report
# Creates reports/ directory with:
# - coverage/index.html (coverage report)
# - pytest-report.html (test results)
# - ruff-report.json (linting issues)
# - bandit-report.json (security scan)
# - safety-report.json (vulnerabilities)
```

#### **Using Batch Script (Windows)**
Create `generate-reports.bat`:
```batch
@echo off
echo Generating comprehensive quality reports...

mkdir reports 2>nul

echo [1/6] Generating Ruff linting report...
python -m ruff check class utility --output-format=json > reports/ruff-report.json

echo [2/6] Generating Bandit security report...
python -m bandit -r class utility -f json -o reports/bandit-report.json

echo [3/6] Generating Safety vulnerability report...
python -m safety check --json > reports/safety-report.json

echo [4/6] Generating test coverage report...
pytest --cov=class --cov-report=html:reports/coverage --quiet

echo [5/6] Generating MyPy type checking report...
python -m mypy class utility --json-report reports/mypy-report 2>nul

echo [6/6] Generating complexity analysis report...
python -m radon cc class utility -j > reports/radon-report.json

echo.
echo ✅ All reports generated in reports/ directory
echo    View HTML reports by opening reports/coverage/index.html
```

### **Report File Locations**

```
project/
├── reports/                          # Generated reports directory
│   ├── coverage/                     # HTML coverage reports
│   │   └── index.html               # Main coverage report
│   ├── pytest-report.html          # Interactive test results
│   ├── ruff-report.json            # Linting issues (JSON)
│   ├── bandit-report.json          # Security scan results
│   ├── safety-report.json          # Dependency vulnerabilities
│   ├── mypy-report/                 # Type checking results
│   └── radon-report.json           # Code complexity analysis
├── htmlcov/                         # Default pytest coverage
│   └── index.html                   # Coverage report
├── coverage.xml                     # CI/CD coverage format
├── coverage.json                    # Machine-readable coverage
└── test-results.xml                 # JUnit XML for CI/CD
```

### **Viewing Reports**

#### **HTML Reports (Interactive)**
```bash
# Windows
start reports/coverage/index.html
start reports/pytest-report.html

# Mac
open reports/coverage/index.html
open reports/pytest-report.html

# Linux
xdg-open reports/coverage/index.html
xdg-open reports/pytest-report.html
```

#### **JSON Reports (Machine Processing)**
```bash
# Parse JSON reports with tools like jq
cat reports/ruff-report.json | jq '.[] | select(.code=="E501")'  # Line length issues
cat reports/bandit-report.json | jq '.results[] | select(.issue_severity=="MEDIUM")'  # Medium security issues
```

### **CI/CD Integration**

#### **GitHub Actions Example**
```yaml
- name: Generate Quality Reports
  run: |
    python -m ruff check class --output-format=json > ruff-report.json
    python -m bandit -r class -f json -o bandit-report.json
    python -m pytest --cov=class --cov-report=xml --junit-xml=test-results.xml

- name: Upload Reports
  uses: actions/upload-artifact@v3
  with:
    name: quality-reports
    path: |
      ruff-report.json
      bandit-report.json
      coverage.xml
      test-results.xml
```

### **Report Content Overview**

#### **Security Reports (Bandit)**
- **File Analysis**: Each file scanned with line-by-line security assessment
- **Issue Severity**: High, Medium, Low confidence levels
- **Vulnerability Types**: SQL injection, hardcoded passwords, shell injection
- **Line Numbers**: Exact location of security issues

#### **Code Quality Reports (Ruff)**
- **Rule Violations**: 738 issues found across different categories
- **Auto-fixable**: 178 issues can be automatically fixed
- **Categories**: Import issues, type hints, security, complexity, style

#### **Coverage Reports (pytest-cov)**
- **Line Coverage**: Percentage of code lines executed
- **Branch Coverage**: Percentage of code branches tested
- **Missing Lines**: Exact lines not covered by tests
- **File Breakdown**: Coverage per module/file

#### **Vulnerability Reports (Safety)**
- **Known CVEs**: Common Vulnerabilities and Exposures database
- **Affected Packages**: Which dependencies have security issues
- **Severity Levels**: Critical, High, Medium, Low vulnerabilities
- **Fix Recommendations**: Suggested package updates

## 📊 Quality Metrics Overview

### Coverage Requirements
- **Minimum Code Coverage**: 80%
- **Target Code Coverage**: 85%+
- **Documentation Coverage**: 80%+
- **Branch Coverage**: Enabled

### Complexity Thresholds
- **Cyclomatic Complexity**: < 10 per function
- **Cognitive Complexity**: < 15 per function
- **Maximum Function Lines**: 50
- **Maximum Class Lines**: 500

## 🔧 Static Code Analysis Tools

### 1. **SonarQube/SonarCloud Integration**

#### Configuration
```properties
# sonar-project.properties
sonar.projectKey=streaming-purchase-order-item
sonar.coverage.exclusions=**/__init__.py,**/tests/**
sonar.qualitygate.wait=true
```

#### Quality Gates
- **Code Coverage**: ≥ 80%
- **Security Hotspots**: 0
- **Bugs**: 0
- **Code Smells**: < 5
- **Duplicated Lines**: < 3%
- **Maintainability Rating**: A
- **Reliability Rating**: A
- **Security Rating**: A

#### Usage
```bash
# Run SonarScanner
sonar-scanner -Dsonar.projectKey=streaming-purchase-order-item \
  -Dsonar.sources=class,utility \
  -Dsonar.tests=tests \
  -Dsonar.python.coverage.reportPaths=coverage.xml
```

### 2. **Code Linting & Formatting**

#### Ruff (Primary Linter)
```bash
# Run Ruff linting
ruff check class utility tests --fix

# Check specific rule categories
ruff check --select E,W,F,I,B,C4,UP,ARG,SIM,PL,RUF,S,N,D
```

#### Black (Code Formatter)
```bash
# Format code
black class utility tests --line-length 120

# Check formatting without changes
black --check class utility tests
```

#### isort (Import Sorter)
```bash
# Sort imports
isort class utility tests --profile black

# Check import order
isort --check-only class utility tests
```

### 3. **Type Checking**

#### MyPy Configuration
```bash
# Run type checking
mypy class utility --strict

# Generate type coverage report
mypy class utility --html-report mypy-report/
```

### 4. **Security Scanning (SAST)**

#### Bandit (Security Linter)
```bash
# Run security scan
bandit -r class utility -f json -o bandit-report.json

# High confidence issues only
bandit -r class utility -ll
```

#### Safety (Dependency Vulnerability Check)
```bash
# Check dependencies for vulnerabilities
safety check --json --output safety-report.json

# Check with full report
safety check --full-report
```

#### pip-audit (Supply Chain Security)
```bash
# Audit dependencies
pip-audit --format=json --output=pip-audit-report.json

# Fix vulnerable packages automatically
pip-audit --fix
```

### 5. **Complexity Analysis**

#### Radon (Cyclomatic Complexity)
```bash
# Check complexity
radon cc class utility -s -nb --total-average

# Generate JSON report
radon cc class utility -j > radon-report.json

# Check maintainability index
radon mi class utility -s
```

#### Xenon (Complexity Monitoring)
```bash
# Monitor complexity over time
xenon class utility --max-absolute B --max-modules A --max-average A
```

## 📏 Code Style Standards

### Python Style Guide
- **Line Length**: 120 characters
- **Indentation**: 4 spaces
- **String Quotes**: Double quotes preferred
- **Function Names**: snake_case
- **Class Names**: PascalCase
- **Constants**: UPPER_SNAKE_CASE

### Docstring Standards
```python
def calculate_total_amount(quantity: int, unit_price: float, discount: float = 0.0) -> float:
    """Calculate the total amount for a purchase order item.

    Args:
        quantity: Number of items ordered
        unit_price: Price per unit
        discount: Discount amount (default: 0.0)

    Returns:
        Total amount after applying discount

    Raises:
        ValueError: If quantity or unit_price is negative

    Example:
        >>> calculate_total_amount(5, 10.0, 2.0)
        48.0
    """
    if quantity < 0 or unit_price < 0:
        raise ValueError("Quantity and unit price must be non-negative")
    return (quantity * unit_price) - discount
```

### Type Hints
```python
from typing import List, Dict, Optional, Union
from dataclasses import dataclass

@dataclass
class PurchaseOrderItem:
    order_id: str
    product_id: str
    quantity: int
    unit_price: float
    total_amount: Optional[float] = None
    metadata: Dict[str, Union[str, int, float]] = None
```

## 🧪 Testing Standards

### Test Structure
```
tests/
├── unit/           # Unit tests (80% of total tests)
├── integration/    # Integration tests (15% of total tests)
├── performance/    # Performance tests (5% of total tests)
├── fixtures/       # Test data and utilities
└── conftest.py     # Pytest configuration
```

### Test Naming Convention
```python
class TestPurchaseOrderItemModel:
    def test_should_calculate_total_when_valid_inputs_provided(self):
        """Test total calculation with valid inputs."""
        pass

    def test_should_raise_error_when_negative_quantity_provided(self):
        """Test error handling for negative quantities."""
        pass
```

### Test Coverage Requirements
- **Unit Tests**: 85%+ coverage
- **Integration Tests**: 70%+ coverage
- **Critical Path**: 100% coverage
- **Error Handling**: 90%+ coverage

## 📋 Documentation Standards

### Code Documentation
```python
class PurchaseOrderDQXPipeline:
    """Bronze to Silver DQX pipeline for purchase order data quality validation.

    This class implements a comprehensive data quality pipeline that transforms
    raw purchase order data from Bronze layer to validated Silver layer using
    Databricks DQX framework.

    Attributes:
        config: Pipeline configuration settings
        dqx_engine: DQX engine instance for quality validation
        spark_session: Spark session for data processing

    Example:
        >>> config = PipelineConfig(quality_threshold=0.8)
        >>> pipeline = PurchaseOrderDQXPipeline(config)
        >>> result = pipeline.process_bronze_to_silver(bronze_df)
    """
```

### README Documentation
- Installation instructions
- Usage examples
- Configuration options
- Troubleshooting guide
- Contributing guidelines

## 🔄 Continuous Integration

### Pre-commit Hooks
```bash
# Install pre-commit
pip install pre-commit

# Setup hooks
pre-commit install

# Run all hooks manually
pre-commit run --all-files
```

### GitHub Actions Workflow
```yaml
name: Code Quality
on: [push, pull_request]

jobs:
  quality-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: pip install -r requirements-dev.txt
      - name: Run linting
        run: ruff check .
      - name: Run type checking
        run: mypy class utility
      - name: Run security scan
        run: bandit -r class utility
      - name: Run tests with coverage
        run: pytest --cov=class --cov-fail-under=80
```

## 📊 Quality Metrics Dashboard

### Local Quality Reports
```bash
# Generate comprehensive quality report
make quality-report

# Individual tool reports
make lint-report
make type-report
make security-report
make coverage-report
make complexity-report
```

### Coverage Reports
- **HTML Report**: `htmlcov/index.html`
- **XML Report**: `coverage.xml` (for CI/CD)
- **JSON Report**: `coverage.json` (for tooling)

### Quality Badges
Add these badges to your README:
```markdown
[![Coverage](https://img.shields.io/badge/coverage-85%25-brightgreen)](./htmlcov/index.html)
[![Quality Gate](https://img.shields.io/badge/quality%20gate-passed-brightgreen)](https://sonarcloud.io/dashboard?id=streaming-purchase-order-item)
[![Security](https://img.shields.io/badge/security-A-brightgreen)](https://sonarcloud.io/dashboard?id=streaming-purchase-order-item)
[![Maintainability](https://img.shields.io/badge/maintainability-A-brightgreen)](https://sonarcloud.io/dashboard?id=streaming-purchase-order-item)
```

## 🎯 Quality Enforcement

### Mandatory Checks
1. **Code Coverage**: Must be ≥ 80%
2. **Security Scan**: Zero high-severity issues
3. **Type Checking**: Must pass without errors
4. **Linting**: Must pass all configured rules
5. **Complexity**: All functions < 10 cyclomatic complexity

### Quality Gates
- **Pull Request**: All quality checks must pass
- **Main Branch**: Additional security and performance tests
- **Release**: Comprehensive quality audit required

### Violation Handling
1. **Blocking**: Coverage < 80%, security issues, type errors
2. **Warning**: Minor style issues, documentation gaps
3. **Advisory**: Complexity warnings, performance suggestions

## 🚀 Best Practices

### Code Organization
```python
# Good: Clear separation of concerns
class PurchaseOrderValidator:
    def validate_financial_data(self, order: PurchaseOrderItem) -> ValidationResult:
        pass

    def validate_business_rules(self, order: PurchaseOrderItem) -> ValidationResult:
        pass
```

### Error Handling
```python
# Good: Specific error types and clear messages
def process_order(order_data: Dict[str, Any]) -> PurchaseOrderItem:
    try:
        return PurchaseOrderItem.from_dict(order_data)
    except KeyError as e:
        raise PurchaseOrderValidationError(f"Missing required field: {e}") from e
    except ValueError as e:
        raise PurchaseOrderValidationError(f"Invalid data format: {e}") from e
```

### Logging
```python
import structlog

logger = structlog.get_logger(__name__)

def process_batch(orders: List[Dict[str, Any]]) -> ProcessingResult:
    logger.info("Processing batch", batch_size=len(orders))
    try:
        # Processing logic
        logger.info("Batch processed successfully", processed_count=len(orders))
    except Exception as e:
        logger.error("Batch processing failed", error=str(e), batch_size=len(orders))
        raise
```

## 📚 Resources

### Documentation
- [Python Style Guide (PEP 8)](https://pep8.org/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [Type Hints (PEP 484)](https://www.python.org/dev/peps/pep-0484/)

### Tools Documentation
- [Ruff Configuration](https://docs.astral.sh/ruff/)
- [Black Configuration](https://black.readthedocs.io/)
- [MyPy Configuration](https://mypy.readthedocs.io/)
- [SonarQube Python](https://docs.sonarqube.org/latest/analysis/languages/python/)

### Training Resources
- Code Quality Fundamentals
- Security Best Practices
- Testing Strategies
- Documentation Standards