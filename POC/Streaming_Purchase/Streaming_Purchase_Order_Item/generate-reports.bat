@echo off
REM =======================================================
REM Comprehensive Quality Reports Generator for Windows
REM =======================================================
REM This script generates all static analysis and test reports
REM Equivalent to 'make quality-report' on Linux/Mac

echo.
echo ========================================
echo    COMPREHENSIVE REPORTS GENERATOR
echo    Purchase Order Streaming Pipeline
echo ========================================
echo.

REM Check if virtual environment is activated
python -c "import sys; exit(0 if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) else 1)" 2>nul
if %errorlevel% neq 0 (
    echo WARNING: Virtual environment not activated!
    echo Please activate your virtual environment first:
    echo    venv\Scripts\activate.bat
    echo.
    choice /C YN /M "Continue anyway"
    if errorlevel 2 goto :end
)

echo Creating reports directory...
if not exist reports mkdir reports
if not exist reports\coverage mkdir reports\coverage
if not exist reports\mypy-report mkdir reports\mypy-report

echo.
echo [1/8] Generating Ruff linting report (JSON)...
python -m ruff check class utility --output-format=json > reports\ruff-report.json 2>nul
if %errorlevel% equ 0 (
    echo    ✓ Ruff report generated: reports\ruff-report.json
) else (
    echo    ⚠ Ruff report generated with issues found
)

echo.
echo [2/8] Generating Bandit security report (JSON + HTML)...
python -m bandit -r class utility -f json -o reports\bandit-report.json 2>nul
python -m bandit -r class utility -f html -o reports\bandit-report.html 2>nul
if exist reports\bandit-report.json (
    echo    ✓ Security reports generated: bandit-report.json/.html
) else (
    echo    ⚠ Failed to generate Bandit reports
)

echo.
echo [3/8] Generating Safety vulnerability report (JSON)...
python -m safety check --json > reports\safety-report.json 2>nul
if exist reports\safety-report.json (
    echo    ✓ Vulnerability report generated: safety-report.json
) else (
    echo    ⚠ Failed to generate Safety report
)

echo.
echo [4/8] Generating pip-audit supply chain report (JSON)...
python -m pip-audit --format=json --output=reports\pip-audit-report.json 2>nul
if exist reports\pip-audit-report.json (
    echo    ✓ Supply chain report generated: pip-audit-report.json
) else (
    echo    ⚠ Failed to generate pip-audit report
)

echo.
echo [5/8] Generating test coverage report (HTML)...
python -m pytest tests\unit --cov=class --cov-report=html:reports\coverage --cov-report=json:reports\coverage.json --cov-report=xml:reports\coverage.xml --quiet --tb=no 2>nul
if exist reports\coverage\index.html (
    echo    ✓ Coverage reports generated: reports\coverage\index.html
) else (
    echo    ⚠ Failed to generate coverage reports
)

echo.
echo [6/8] Generating pytest test report (HTML)...
python -m pytest tests\unit --html=reports\pytest-report.html --self-contained-html --quiet --tb=no 2>nul
if exist reports\pytest-report.html (
    echo    ✓ Test report generated: pytest-report.html
) else (
    echo    ⚠ Failed to generate pytest report
)

echo.
echo [7/8] Generating MyPy type checking report (JSON)...
python -m mypy class utility --json-report reports\mypy-report --ignore-missing-imports 2>nul
if exist reports\mypy-report (
    echo    ✓ Type checking report generated: mypy-report\
) else (
    echo    ⚠ Failed to generate MyPy report
)

echo.
echo [8/8] Generating complexity analysis reports (JSON)...
python -m radon cc class utility -j > reports\radon-complexity.json 2>nul
python -m radon mi class utility -j > reports\radon-maintainability.json 2>nul
if exist reports\radon-complexity.json (
    echo    ✓ Complexity reports generated: radon-*.json
) else (
    echo    ⚠ Failed to generate complexity reports
)

echo.
echo ========================================
echo    REPORTS GENERATION SUMMARY
echo ========================================
echo.
echo Generated reports in reports\ directory:
echo.
echo 📊 INTERACTIVE REPORTS (Open in Browser):
echo    reports\coverage\index.html       - Code coverage visualization
echo    reports\pytest-report.html        - Test execution results
echo    reports\bandit-report.html        - Security scan (HTML format)
echo.
echo 📋 DATA REPORTS (JSON for CI/CD):
echo    reports\ruff-report.json          - Code quality issues
echo    reports\bandit-report.json        - Security vulnerabilities
echo    reports\safety-report.json        - Dependency vulnerabilities
echo    reports\pip-audit-report.json     - Supply chain security
echo    reports\coverage.json/.xml        - Coverage data
echo    reports\radon-*.json              - Code complexity analysis
echo.
echo 🚀 QUICK ACTIONS:
echo    1. View coverage: start reports\coverage\index.html
echo    2. View test results: start reports\pytest-report.html
echo    3. View security: start reports\bandit-report.html
echo.
echo 📁 All reports saved in: %cd%\reports\
echo.

:end
pause