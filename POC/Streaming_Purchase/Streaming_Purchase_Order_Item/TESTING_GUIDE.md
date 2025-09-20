# Testing Guide & Strategy

This comprehensive guide covers testing strategies, execution instructions, and best practices for the Purchase Order Item Streaming Pipeline project.

## 📊 Testing Overview

### Test Coverage Goals
- **Total Tests**: 130+ comprehensive tests
- **Code Coverage**: 85%+ (exceeding 80% requirement)
- **Documentation Coverage**: 80%+
- **Critical Path Coverage**: 100%

### Test Categories
- **Unit Tests**: 69 tests (53%)
- **Integration Tests**: 18 tests (14%)
- **Performance Tests**: 15 tests (12%)
- **End-to-End Tests**: 28 tests (21%)

## 🏗️ Test Architecture

### Test Structure
```
tests/
├── unit/                          # Unit tests (69 tests)
│   ├── test_purchase_order_item_model.py         # 26 tests
│   ├── test_purchase_order_item_factory.py       # 22 tests
│   ├── test_purchase_order_dqx_rules.py          # 21 tests
│   ├── test_purchase_order_dqx_pipeline.py       # 10 tests
│   ├── test_purchase_order_silver_manager.py     # 8 tests
│   ├── test_purchase_order_dqx_monitor.py        # 7 tests
│   ├── test_bronze_layer_handler.py              # 8 tests
│   ├── test_hive_metastore_manager.py            # 6 tests
│   ├── test_purchase_order_item_producer.py      # 7 tests
│   └── test_purchase_order_item_listener.py      # 7 tests
├── integration/                   # Integration tests (18 tests)
│   ├── test_eventhub_integration.py              # 4 tests
│   ├── test_end_to_end_pipeline.py               # 5 tests
│   ├── test_hive_operations.py                   # 4 tests
│   ├── test_adls_storage.py                      # 3 tests
│   └── test_dqx_framework.py                     # 2 tests
├── performance/                   # Performance tests (15 tests)
│   ├── test_throughput_benchmarks.py             # 5 tests
│   ├── test_memory_usage.py                      # 4 tests
│   ├── test_dqx_performance.py                   # 3 tests
│   └── test_concurrent_processing.py             # 3 tests
├── cloud/                         # Cloud-specific tests (28 tests)
│   ├── test_azure_eventhub_cloud.py              # 14 tests
│   └── test_databricks_dqx_cloud.py              # 14 tests
├── fixtures/                      # Test data and utilities
│   ├── sample_purchase_orders.json
│   ├── dqx_test_scenarios.json
│   └── test_schemas.py
├── conftest.py                    # Pytest configuration
└── README.md                      # This file
```

## 🧪 Test Categories Detailed

### 1. Unit Tests (69 tests)

#### test_purchase_order_item_model.py (26 tests)
**Purpose**: Test data model validation and business logic

**Key Test Areas**:
- Data validation and type checking
- Financial calculations accuracy
- Status combination validation
- Serialization/deserialization
- Edge cases and error handling

**Example Test**:
```python
def test_financial_validation_calculations(self):
    """Test financial validation and calculation accuracy."""
    order = PurchaseOrderItem(
        order_id="ORD-001",
        quantity=3,
        unit_price=15.50,
        total_amount=46.50  # 3 * 15.50 = 46.50
    )
    assert order.is_valid is True
    assert order.validation_errors is None
```

**Running Tests**:
```bash
# Run all model tests
pytest tests/unit/test_purchase_order_item_model.py -v

# Run specific test
pytest tests/unit/test_purchase_order_item_model.py::TestPurchaseOrderItemModel::test_financial_validation_calculations -v

# Run with coverage
pytest tests/unit/test_purchase_order_item_model.py --cov=class/purchase_order_item_model.py --cov-report=html
```

#### test_purchase_order_item_factory.py (22 tests)
**Purpose**: Test data generation and quality scenarios

**Key Test Areas**:
- Data generation patterns
- Quality issue injection
- Business scenario simulation
- Statistical validation
- Batch processing

**Example Test**:
```python
def test_generate_batch_with_quality_issues(self):
    """Test batch generation with controlled quality issues."""
    factory = PurchaseOrderItemFactory(quality_issue_rate=0.2)
    orders = factory.generate_batch(batch_size=100)

    invalid_orders = [order for order in orders if not order.is_valid]
    issue_rate = len(invalid_orders) / 100

    # Should be approximately 20% (allow variance)
    assert 0.15 <= issue_rate <= 0.25
```

#### test_purchase_order_dqx_rules.py (21 tests)
**Purpose**: Test DQX quality rules and validation

**Key Test Areas**:
- Rule definition and configuration
- Quality validation logic
- Rule categorization
- Error handling
- Fallback mechanisms

### 2. Integration Tests (18 tests)

#### test_eventhub_integration.py (4 tests)
**Purpose**: Test EventHub connectivity and message flow

**Test Scenarios**:
- Producer to EventHub connection
- Listener streaming from EventHub
- Message serialization/deserialization
- Error handling and retries

**Example Test**:
```python
@pytest.mark.integration
@pytest.mark.requires_eventhub
def test_producer_to_eventhub_connectivity(self):
    """Test producer can successfully connect to EventHub."""
    producer = PurchaseOrderItemProducer(eventhub_config)

    # Send test message
    test_order = factory.generate_single_item()
    result = producer.send_order(test_order)

    assert result.success is True
    assert result.message_id is not None
```

#### test_end_to_end_pipeline.py (5 tests)
**Purpose**: Test complete pipeline from producer to silver layer

**Test Scenarios**:
- Full data flow validation
- DQX processing end-to-end
- Checkpoint recovery
- Pipeline failure recovery
- Data quality monitoring

### 3. Performance Tests (15 tests)

#### test_throughput_benchmarks.py (5 tests)
**Purpose**: Measure processing throughput and performance

**Key Metrics**:
- Events processed per second
- Memory usage during processing
- CPU utilization
- Network I/O performance
- Storage I/O performance

**Example Test**:
```python
@pytest.mark.performance
@pytest.mark.benchmark(group="throughput")
def test_purchase_order_processing_throughput(self, benchmark):
    """Benchmark purchase order processing speed."""
    factory = PurchaseOrderItemFactory()
    orders = factory.generate_batch(1000)

    def process_orders():
        pipeline = PurchaseOrderDQXPipeline()
        return pipeline.process_batch(orders)

    result = benchmark(process_orders)

    # Should process at least 100 orders/second
    assert len(result.processed_orders) >= 100
```

### 4. Cloud-Specific Tests (28 tests)

#### test_azure_eventhub_cloud.py (14 tests)
**Purpose**: Test Azure EventHub cloud integration

**Test Scenarios**:
- Cloud EventHub connectivity
- Authentication and authorization
- Message routing and partitioning
- Scaling and performance
- Error handling and monitoring

## 🔧 Test Configuration

### pytest.ini Configuration
```ini
[pytest]
testpaths = tests
python_files = test_*.py
addopts =
    -v
    --strict-markers
    --cov=class
    --cov-fail-under=80
    --html=pytest-report.html
markers =
    unit: Unit tests
    integration: Integration tests
    performance: Performance tests
    cloud: Cloud-specific tests
```

### conftest.py Fixtures
```python
@pytest.fixture
def spark_session():
    """Provide Spark session for tests."""
    return SparkSession.builder.appName("test").getOrCreate()

@pytest.fixture
def sample_orders():
    """Provide sample purchase order data."""
    factory = PurchaseOrderItemFactory()
    return factory.generate_batch(50)

@pytest.fixture
def mock_eventhub():
    """Mock EventHub connection for testing."""
    with patch('azure.eventhub.EventHubProducerClient') as mock:
        yield mock
```

## 🚀 Running Tests

### Quick Start
```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
pytest

# Run with coverage
pytest --cov=class --cov-report=html

# Run specific test category
pytest -m unit
pytest -m integration
pytest -m performance
```

### Detailed Test Execution

#### Unit Tests
```bash
# Run all unit tests
pytest tests/unit/ -v

# Run specific unit test file
pytest tests/unit/test_purchase_order_item_model.py -v

# Run unit tests with coverage
pytest tests/unit/ --cov=class --cov-report=term-missing

# Run unit tests in parallel
pytest tests/unit/ -n auto
```

#### Integration Tests
```bash
# Run integration tests (requires cloud resources)
pytest tests/integration/ -v -m integration

# Run integration tests with specific markers
pytest -m "integration and not slow"

# Run integration tests with retry on failure
pytest tests/integration/ --reruns 2
```

#### Performance Tests
```bash
# Run performance tests
pytest tests/performance/ -v -m performance

# Run performance tests with benchmarking
pytest tests/performance/ --benchmark-only

# Generate performance report
pytest tests/performance/ --benchmark-json=benchmark-results.json
```

#### Cloud Tests
```bash
# Run cloud tests (requires Azure credentials)
pytest tests/cloud/ -v -m cloud

# Run cloud tests with specific configuration
pytest tests/cloud/ --cloud-config=test-config.yaml
```

### Test Reporting

#### Coverage Reports
```bash
# Generate HTML coverage report
pytest --cov=class --cov-report=html

# Generate XML coverage report (for CI/CD)
pytest --cov=class --cov-report=xml

# Generate JSON coverage report
pytest --cov=class --cov-report=json
```

#### Test Results
```bash
# Generate JUnit XML report
pytest --junit-xml=test-results.xml

# Generate HTML test report
pytest --html=pytest-report.html --self-contained-html

# Generate JSON test report
pytest --json-report --json-report-file=test-report.json
```

## 📊 Test Data Management

### Test Fixtures
```python
# tests/fixtures/sample_purchase_orders.json
{
  "valid_orders": [
    {
      "order_id": "ORD-123456",
      "product_id": "PROD-001",
      "quantity": 5,
      "unit_price": 29.99,
      "total_amount": 149.95
    }
  ],
  "invalid_orders": [
    {
      "order_id": "ORD-123457",
      "product_id": "PROD-002",
      "quantity": -1,
      "unit_price": 19.99,
      "total_amount": -19.99
    }
  ]
}
```

### Mock Data Generation
```python
class TestDataFactory:
    """Factory for generating test data."""

    @staticmethod
    def create_valid_order(**kwargs):
        """Create a valid purchase order for testing."""
        defaults = {
            "order_id": "ORD-TEST-001",
            "product_id": "PROD-TEST-001",
            "quantity": 1,
            "unit_price": 10.0,
            "total_amount": 10.0
        }
        defaults.update(kwargs)
        return PurchaseOrderItem(**defaults)

    @staticmethod
    def create_invalid_order(issue_type="negative_quantity"):
        """Create an invalid purchase order for testing."""
        if issue_type == "negative_quantity":
            return PurchaseOrderItem(
                order_id="ORD-INVALID-001",
                quantity=-1,
                unit_price=10.0
            )
```

## 🔍 Test Quality Assurance

### Test Review Checklist
- [ ] Test names clearly describe what is being tested
- [ ] Tests are independent and can run in any order
- [ ] Test data is realistic and covers edge cases
- [ ] Assertions are specific and meaningful
- [ ] Setup and teardown are properly handled
- [ ] Tests run within reasonable time limits
- [ ] Mock objects are used appropriately
- [ ] Error cases are tested thoroughly

### Test Metrics
```bash
# Test execution time
pytest --durations=10

# Test coverage by line
pytest --cov=class --cov-report=term-missing

# Test coverage by branch
pytest --cov=class --cov-branch

# Test flakiness detection
pytest --lf --ff  # Last failed, fail fast
```

## 🐛 Debugging Tests

### Test Debugging
```bash
# Run tests with pdb debugging
pytest --pdb

# Run specific test with verbose output
pytest tests/unit/test_model.py::test_specific -v -s

# Run tests with logging
pytest --log-cli-level=DEBUG

# Run tests with custom markers
pytest -m "not slow" -v
```

### Common Issues and Solutions

#### Issue: Tests fail due to missing dependencies
```bash
# Solution: Check and install test dependencies
pip install -r requirements-test.txt
```

#### Issue: Integration tests fail due to missing cloud resources
```bash
# Solution: Use mock objects or skip cloud tests
pytest -m "not cloud"
```

#### Issue: Performance tests are inconsistent
```bash
# Solution: Run multiple times and check for patterns
pytest tests/performance/ --count=5
```

## 📈 Continuous Integration

### GitHub Actions Workflow
```yaml
name: Test Suite
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.9, 3.10, 3.11]

    steps:
    - uses: actions/checkout@v4
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}

    - name: Install dependencies
      run: |
        pip install -r requirements-test.txt

    - name: Run unit tests
      run: |
        pytest tests/unit/ --cov=class --cov-report=xml

    - name: Run integration tests
      run: |
        pytest tests/integration/ -m "not cloud"

    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
```

### Quality Gates
- **Unit Tests**: Must pass with 85%+ coverage
- **Integration Tests**: Must pass for critical paths
- **Performance Tests**: Must meet baseline requirements
- **Security Tests**: Must pass without high-severity issues

## 📚 Resources

### Testing Frameworks
- [pytest](https://pytest.org/) - Main testing framework
- [pytest-cov](https://pytest-cov.readthedocs.io/) - Coverage plugin
- [pytest-mock](https://pytest-mock.readthedocs.io/) - Mocking plugin
- [pytest-benchmark](https://pytest-benchmark.readthedocs.io/) - Performance testing

### Best Practices
- [Testing Best Practices](https://docs.python-guide.org/writing/tests/)
- [pytest Good Practices](https://docs.pytest.org/en/stable/goodpractices.html)
- [Test-Driven Development](https://testdriven.io/)

### Documentation
- [Testing Documentation](./docs/testing/)
- [API Documentation](./docs/api/)
- [Contributing Guide](./CONTRIBUTING.md)