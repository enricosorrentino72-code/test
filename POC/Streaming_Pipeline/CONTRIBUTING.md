# Contributing to Databricks Streaming Pipeline

Thank you for your interest in contributing! This document provides guidelines and information for contributing to the project.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Code Quality](#code-quality)
- [Pull Request Process](#pull-request-process)
- [Performance Guidelines](#performance-guidelines)
- [Documentation](#documentation)

## Code of Conduct

This project adheres to a code of conduct that promotes a welcoming and inclusive environment. By participating, you agree to uphold these standards.

## Getting Started

### Prerequisites
- Python 3.8 or higher
- Git
- Azure account (for testing with real services)
- Basic understanding of streaming data pipelines

### Repository Structure
```
databricks-streaming-pipeline/
├── .github/                    # GitHub workflows and templates
├── tests/                      # Test suite (67+ tests)
├── Bronze_to_Silver_*.py       # Main pipeline components
├── EventHub_*.py              # EventHub integration
├── requirements*.txt          # Dependencies
├── pyproject.toml            # Package configuration
└── docs/                     # Documentation
```

## Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/databricks-streaming-pipeline.git
   cd databricks-streaming-pipeline
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   pip install -r requirements-test.txt
   ```

4. **Set Up Environment Variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Install Pre-commit Hooks**
   ```bash
   pre-commit install
   ```

## Making Changes

### Branch Naming Convention
- `feature/description` - New features
- `bugfix/description` - Bug fixes
- `docs/description` - Documentation updates
- `refactor/description` - Code refactoring
- `test/description` - Test improvements

### Commit Message Format
```
type(scope): description

body (optional)

footer (optional)
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Code formatting
- `refactor`: Code refactoring
- `test`: Tests
- `perf`: Performance improvements

**Examples:**
```
feat(eventhub): add retry logic for connection failures
fix(dqx): resolve validation error handling
docs(readme): update installation instructions
test(integration): add end-to-end pipeline tests
```

## Testing

### Test Categories
The project uses comprehensive testing with different priorities:

#### Unit Tests (44 tests)
```bash
pytest tests/ -m "unit" -v
```
- Fast, isolated tests
- Mock all external dependencies
- Target: >95% coverage for critical functions

#### Integration Tests (12 tests)  
```bash
pytest tests/ -m "integration" -v
```
- End-to-end workflow testing
- Test component interactions
- Target: Complete workflow coverage

#### Performance Tests (11 tests)
```bash
pytest tests/ -m "performance" -v
```
- Throughput and speed validation
- Memory usage monitoring
- Performance regression detection

#### Priority-Based Testing
```bash
# Critical functions (95% coverage target)
pytest tests/ -m "critical" -v

# High priority (95% coverage target)  
pytest tests/ -m "high" -v

# Medium priority (80% coverage target)
pytest tests/ -m "medium" -v

# Low priority (60% coverage target)
pytest tests/ -m "low" -v
```

### Running All Tests
```bash
# Run working test files only
pytest tests/simple_test_eventhub_producer.py tests/simple_test_eventhub_listener.py tests/simple_test_bronze_to_silver_dqx.py -v

# With coverage
pytest tests/simple_test_*.py --cov --cov-report=html
```

### Writing Tests

#### Test Structure
```python
import pytest
from unittest.mock import MagicMock, patch

class TestEventHubProducer:
    @pytest.mark.unit
    @pytest.mark.high
    def test_connection_validation(self):
        """Test EventHub connection validation logic."""
        # Arrange
        producer = EventHubProducer()
        
        # Act & Assert
        with patch('azure.eventhub.EventHubProducerClient'):
            result = producer.check_eventhub_connection()
            assert result is True
```

#### Performance Test Example
```python
@pytest.mark.performance
@pytest.mark.critical
def test_transformation_throughput(self):
    """Test Bronze-to-Silver transformation performance."""
    start_time = time.time()
    
    # Process test data
    result = transform_bronze_to_silver(test_data)
    
    execution_time = time.time() - start_time
    records_per_second = len(test_data) / execution_time
    
    # Performance assertions
    assert execution_time < 0.3  # <300ms per 1K records
    assert records_per_second > 8000  # >8K records/second
```

## Code Quality

### Code Formatting
```bash
# Format code with Black
black .

# Sort imports with isort
isort .

# Check formatting
black --check .
isort --check-only .
```

### Linting
```bash
# Run flake8 linting
flake8 .

# Type checking with mypy
mypy . --ignore-missing-imports
```

### Pre-commit Hooks
The project uses pre-commit hooks that run automatically:
- Black code formatting
- isort import sorting
- flake8 linting
- mypy type checking
- Trailing whitespace removal

## Pull Request Process

1. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Changes**
   - Write tests first (TDD approach recommended)
   - Implement functionality
   - Update documentation

3. **Verify Quality**
   ```bash
   # Run all quality checks
   black .
   isort .
   flake8 .
   mypy .
   
   # Run tests
   pytest tests/simple_test_*.py -v
   ```

4. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat(component): description of changes"
   ```

5. **Push and Create PR**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **PR Requirements**
   - All tests pass
   - Code coverage maintained
   - Documentation updated
   - Performance benchmarks included (if applicable)
   - PR template completed

## Performance Guidelines

### Performance Targets
- **Transformation Throughput**: >8K records/second
- **EventHub Processing**: >10K messages/second  
- **DQX Validation**: >20K rule evaluations/second
- **Memory Usage**: <120MB increase under load
- **Response Time**: <300ms per 1K records

### Performance Testing
```python
# Include performance assertions in tests
def test_performance_requirement():
    start_time = time.time()
    result = process_data(large_dataset)
    execution_time = time.time() - start_time
    
    throughput = len(large_dataset) / execution_time
    assert throughput > REQUIRED_THROUGHPUT
```

### Optimization Guidelines
- Use efficient data structures
- Minimize memory allocations
- Implement proper caching strategies
- Optimize database queries
- Use async/await for I/O operations

## Documentation

### Documentation Requirements
- Update README.md for user-facing changes
- Add docstrings for new functions/classes
- Update API documentation
- Include code examples
- Update deployment guides if needed

### Docstring Format
```python
def transform_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw data with DQX validation.
    
    Args:
        data: Raw input data dictionary
        
    Returns:
        Transformed data with quality metadata
        
    Raises:
        ValidationError: If data fails quality checks
        
    Example:
        >>> result = transform_data({"temp": 25.5})
        >>> assert "dqx_metadata" in result
    """
```

## Getting Help

### Communication Channels
- **Issues**: Use GitHub Issues for bug reports and feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Reviews**: Tag relevant maintainers for code reviews

### Maintainers
- Core team: @walgreens-rx-team
- Performance: @performance-team
- Testing: @qa-team

### Resources
- [Project Wiki](https://github.com/walgreens-rx/databricks-streaming-pipeline/wiki)
- [Testing Guide](tests/README.md)
- [Deployment Guide](Databricks_Deployment_Guide.md)

Thank you for contributing to make this project better! 🚀