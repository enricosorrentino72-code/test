@echo off
REM =======================================================
REM Static Code Analysis & Quality Check Script for Windows
REM =======================================================
REM This script runs all static analysis tools for code quality
REM Equivalent to 'make quality-check' on Linux/Mac

echo.
echo ========================================
echo    STATIC CODE ANALYSIS SUITE
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

echo [1/8] Checking Python version...
python --version

echo.
echo [2/8] Running Ruff linter with auto-fix...
python -m ruff check class utility tests --fix
if %errorlevel% neq 0 (
    echo    ⚠ Ruff found issues that need manual fixing
) else (
    echo    ✓ Ruff check passed
)

echo.
echo [3/8] Running Black formatter...
python -m black class utility tests --check
if %errorlevel% neq 0 (
    echo    ⚠ Black would reformat files. Running formatter...
    python -m black class utility tests
    echo    ✓ Files formatted
) else (
    echo    ✓ Black check passed - files already formatted
)

echo.
echo [4/8] Running isort import sorter...
python -m isort class utility tests --check-only
if %errorlevel% neq 0 (
    echo    ⚠ isort would reorder imports. Running sorter...
    python -m isort class utility tests
    echo    ✓ Imports sorted
) else (
    echo    ✓ isort check passed - imports already sorted
)

echo.
echo [5/8] Running MyPy type checker...
python -m mypy class utility --ignore-missing-imports
if %errorlevel% neq 0 (
    echo    ⚠ MyPy found type errors
) else (
    echo    ✓ MyPy type check passed
)

echo.
echo [6/8] Running Bandit security scanner...
python -m bandit -r class utility -ll
if %errorlevel% neq 0 (
    echo    ⚠ Bandit found security issues
) else (
    echo    ✓ Bandit security check passed
)

echo.
echo [7/8] Running Safety dependency vulnerability check...
python -m safety check --short-report
if %errorlevel% neq 0 (
    echo    ⚠ Safety found vulnerable dependencies
) else (
    echo    ✓ Safety dependency check passed
)

echo.
echo [8/8] Running pip-audit supply chain check...
python -m pip-audit
if %errorlevel% neq 0 (
    echo    ⚠ pip-audit found supply chain issues
) else (
    echo    ✓ pip-audit check passed
)

echo.
echo ========================================
echo    QUALITY CHECK SUMMARY
echo ========================================
echo.
echo Static analysis complete!
echo.
echo Next steps:
echo 1. Fix any issues reported above
echo 2. Run tests: pytest --cov=class --cov-report=html
echo 3. Commit changes: git add . ^&^& git commit -m "message"
echo    (pre-commit hooks will run automatically)
echo.

:end
pause