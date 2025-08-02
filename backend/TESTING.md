# Testing Guide for Cartridge Backend

This document provides comprehensive information about testing the Cartridge backend application.

## 🧪 Test Suite Overview

Our test suite is designed to ensure reliability, performance, and correctness of the Cartridge backend. It includes:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test complete workflows and component interactions
- **Performance Tests**: Ensure the application meets performance requirements
- **Database Tests**: Validate database schema, migrations, and operations

## 📁 Test Structure

```
backend/tests/
├── conftest.py              # Pytest configuration and fixtures
├── pytest.ini              # Pytest settings and markers
├── test_database_setup.py   # Database setup and migration tests
├── unit/                    # Unit tests
│   ├── test_models.py       # Database model tests
│   ├── test_api.py          # API endpoint tests
│   ├── test_tasks.py        # Celery task tests
│   └── test_config.py       # Configuration tests
├── integration/             # Integration tests
│   └── test_workflows.py    # End-to-end workflow tests
└── performance/             # Performance tests
    └── test_api_performance.py
```

## 🚀 Quick Start

### Prerequisites

1. **Python Environment**: Python 3.9+ with dependencies installed
2. **Database**: PostgreSQL running on localhost:5432
3. **Redis**: Redis running on localhost:6379
4. **Docker** (optional): For automated test database setup

### Running Tests

#### Using the Test Runner Script (Recommended)

```bash
# Run all tests with automatic setup
./scripts/run_tests.sh

# Run specific test suites
./scripts/run_tests.sh unit
./scripts/run_tests.sh integration
./scripts/run_tests.sh performance

# Run with coverage report
./scripts/run_tests.sh coverage

# Skip database/Redis setup (if already running)
./scripts/run_tests.sh all --skip-setup
```

#### Using Make Commands

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test types
make test-unit
make test-integration
make test-performance

# Run specific test files
make test-models
make test-api
make test-tasks
make test-config
make test-db

# Run tests in parallel
make test-parallel

# Watch mode for development
make test-watch
```

#### Using Pytest Directly

```bash
# All tests
pytest

# Specific test file
pytest tests/unit/test_models.py -v

# Specific test class
pytest tests/unit/test_models.py::TestUser -v

# Specific test method
pytest tests/unit/test_models.py::TestUser::test_create_user -v

# With coverage
pytest --cov=cartridge --cov-report=html

# Performance tests only
pytest -m slow

# Exclude performance tests
pytest -m "not slow"
```

## 📊 Test Categories

### Unit Tests

**Purpose**: Test individual components in isolation

- **Models** (`test_models.py`): Database model validation, relationships, constraints
- **API** (`test_api.py`): HTTP endpoint behavior, request/response validation
- **Tasks** (`test_tasks.py`): Celery task execution, error handling
- **Config** (`test_config.py`): Configuration loading, validation, environment variables

**Coverage Target**: 90%+

### Integration Tests

**Purpose**: Test complete workflows and component interactions

- **Full Workflows** (`test_workflows.py`): End-to-end user journeys
- **API Integration**: Multi-endpoint workflows
- **Database Integration**: Cross-table operations and transactions
- **Error Handling**: Failure scenarios and recovery

**Coverage Target**: 80%+

### Performance Tests

**Purpose**: Ensure application meets performance requirements

- **API Response Times**: < 100ms for health checks, < 2s for complex operations
- **Concurrent Load**: Handle 20+ concurrent requests
- **Memory Usage**: Stable memory usage under load
- **Database Performance**: Query optimization validation

**Performance Targets**:
- Health endpoint: < 50ms median response time
- API endpoints: < 500ms average response time
- 95% success rate under concurrent load
- Memory growth < 50MB for 1000 requests

### Database Tests

**Purpose**: Validate database schema, migrations, and operations

- **Schema Creation**: All tables and constraints created correctly
- **Migrations**: Alembic migration system functionality
- **Performance**: Query performance and connection pooling
- **Transactions**: ACID compliance and isolation

## 🔧 Test Configuration

### Environment Variables

Tests use the following environment variables:

```bash
CARTRIDGE_ENVIRONMENT=test
CARTRIDGE_DB_URL=postgresql://cartridge:cartridge@localhost:5432/cartridge_test
CARTRIDGE_REDIS_URL=redis://localhost:6379/1
CARTRIDGE_LOG_LEVEL=WARNING
```

### Test Database

Tests use a separate database (`cartridge_test`) to avoid conflicts with development data.

### Fixtures

Common fixtures available in all tests:

- `db_session`: Synchronous database session
- `async_db_session`: Asynchronous database session
- `client`: FastAPI test client
- `user`: Test user instance
- `data_source`: Test data source instance
- `project`: Test project instance
- `scan_result`: Test scan result instance

## 📈 Coverage Requirements

- **Overall Coverage**: Minimum 80%
- **Unit Tests**: Minimum 90%
- **Critical Paths**: 100% (authentication, data processing, API endpoints)

### Generating Coverage Reports

```bash
# HTML report (opens in browser)
pytest --cov=cartridge --cov-report=html
open htmlcov/index.html

# Terminal report
pytest --cov=cartridge --cov-report=term-missing

# XML report (for CI/CD)
pytest --cov=cartridge --cov-report=xml
```

## 🚨 Continuous Integration

### GitHub Actions Workflow

Tests run automatically on:
- Pull requests
- Pushes to main branch
- Scheduled runs (daily)

### Test Matrix

Tests run against:
- Python 3.9, 3.10, 3.11
- PostgreSQL 13, 14, 15
- Redis 6, 7

### Quality Gates

- All tests must pass
- Coverage must be ≥ 80%
- No linting errors
- No security vulnerabilities

## 🐛 Debugging Tests

### Common Issues

1. **Database Connection Errors**
   ```bash
   # Check if PostgreSQL is running
   pg_isready -h localhost -p 5432
   
   # Start test database
   docker run -d --name cartridge_test_db \
     -e POSTGRES_DB=cartridge_test \
     -e POSTGRES_USER=cartridge \
     -e POSTGRES_PASSWORD=cartridge \
     -p 5432:5432 postgres:15-alpine
   ```

2. **Redis Connection Errors**
   ```bash
   # Check if Redis is running
   redis-cli ping
   
   # Start Redis
   docker run -d --name cartridge_test_redis \
     -p 6379:6379 redis:7-alpine
   ```

3. **Import Errors**
   ```bash
   # Install test dependencies
   pip install -e ".[test]"
   
   # Check Python path
   export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
   ```

### Debugging Specific Tests

```bash
# Run with verbose output
pytest tests/unit/test_models.py -v -s

# Run with debugging
pytest tests/unit/test_models.py --pdb

# Run only failed tests
pytest --lf

# Run with warnings
pytest -W ignore::DeprecationWarning
```

## 📝 Writing New Tests

### Test Naming Convention

- Test files: `test_*.py`
- Test classes: `Test*`
- Test methods: `test_*`

### Example Test Structure

```python
class TestFeature:
    """Test feature functionality."""
    
    def test_happy_path(self, fixture):
        """Test the main success scenario."""
        # Arrange
        data = {"key": "value"}
        
        # Act
        result = function_under_test(data)
        
        # Assert
        assert result.status == "success"
        assert result.data == expected_data
    
    def test_error_handling(self, fixture):
        """Test error scenarios."""
        with pytest.raises(ExpectedException):
            function_under_test(invalid_data)
    
    @pytest.mark.asyncio
    async def test_async_operation(self, async_fixture):
        """Test asynchronous operations."""
        result = await async_function_under_test()
        assert result is not None
```

### Best Practices

1. **Use descriptive test names** that explain what is being tested
2. **Follow AAA pattern**: Arrange, Act, Assert
3. **Test both success and failure scenarios**
4. **Use appropriate fixtures** to set up test data
5. **Mock external dependencies** (APIs, file systems)
6. **Keep tests independent** - no test should depend on another
7. **Use parametrized tests** for multiple similar scenarios

## 🔍 Test Markers

Available pytest markers:

- `@pytest.mark.slow`: Performance tests (excluded by default)
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.unit`: Unit tests
- `@pytest.mark.asyncio`: Async tests
- `@pytest.mark.celery`: Tests that use Celery

Usage:
```bash
# Run only unit tests
pytest -m unit

# Exclude slow tests
pytest -m "not slow"

# Run integration and unit tests
pytest -m "integration or unit"
```

## 📊 Test Metrics

Track these metrics for test health:

- **Test Count**: Total number of tests
- **Coverage Percentage**: Code coverage
- **Test Execution Time**: How long tests take to run
- **Flaky Test Rate**: Tests that intermittently fail
- **Test Success Rate**: Percentage of passing tests

## 🔄 Test Maintenance

### Regular Tasks

- **Weekly**: Review test coverage reports
- **Monthly**: Update test dependencies
- **Quarterly**: Review and refactor slow tests
- **As needed**: Add tests for new features

### Refactoring Tests

When refactoring tests:
1. Ensure tests still validate the same behavior
2. Update test names to reflect changes
3. Remove obsolete tests
4. Add tests for new edge cases
5. Update documentation

---

## 🆘 Getting Help

If you encounter issues with tests:

1. Check this documentation
2. Look at existing similar tests
3. Review the test fixtures in `conftest.py`
4. Ask in team chat or create an issue
5. Consult the pytest documentation

Happy testing! 🧪✨