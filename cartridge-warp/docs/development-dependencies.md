# Development Dependencies Guide

This document explains the development dependencies setup for cartridge-warp and how to use them effectively.

## Installation Options

### Quick Setup (Recommended)
```bash
# Install all development tools
make install-dev

# Or install everything including optional connectors
make install-all
```

### Manual Installation
```bash
# Core + development tools
pip install -e ".[dev,test]"

# Everything (includes BigQuery, documentation tools, etc.)
pip install -e ".[all]"

# Individual dependency groups
pip install -e ".[dev]"      # Development tools only
pip install -e ".[test]"     # Testing tools only
pip install -e ".[bigquery]" # BigQuery connector only
```

## Development Tools Included

### Code Quality & Formatting
- **black**: Code formatting (88 character line length)
- **ruff**: Fast Python linter and code formatter
- **isort**: Import statement sorting
- **mypy**: Static type checking
- **pre-commit**: Git hooks for code quality

### Testing Framework
- **pytest**: Core testing framework with async support
- **pytest-asyncio**: Async test support
- **pytest-cov**: Code coverage reporting
- **pytest-xdist**: Parallel test execution
- **pytest-mock**: Enhanced mocking utilities
- **pytest-timeout**: Test timeout management
- **pytest-benchmark**: Performance benchmarking
- **testcontainers**: Integration testing with Docker containers

### Development Utilities
- **ipython**: Enhanced interactive Python shell
- **ipdb**: Enhanced debugger
- **memory-profiler**: Memory usage profiling
- **watchdog**: File system monitoring for auto-reload
- **factory-boy**: Test data factories
- **faker**: Fake data generation for testing

### Documentation
- **sphinx**: Documentation generation
- **sphinx-rtd-theme**: Read the Docs theme
- **myst-parser**: Markdown support for Sphinx

### Publishing & Versioning
- **bump2version**: Semantic version management
- **twine**: Package publishing to PyPI

### HTTP Testing
- **httpx**: Async HTTP client for testing APIs
- **respx**: HTTP request mocking

## Usage Examples

### Code Quality Workflow
```bash
# Format code
make format  # Uses black and ruff

# Check linting
make lint    # Uses ruff

# Type checking
make type-check  # Uses mypy

# Run all quality checks
make format lint type-check
```

### Testing Workflow
```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific test file
pytest tests/test_config.py

# Run tests in parallel
pytest -n auto

# Run only unit tests
pytest -m unit

# Run with benchmark reporting
pytest --benchmark-only
```

### Development Workflow
```bash
# Install in development mode
make install-dev

# Run tests continuously on file changes
pytest --watch

# Start IPython for interactive development
ipython

# Profile memory usage
mprof run python script.py
mprof plot
```

## Environment Setup for Contributors

1. **Clone and Setup**:
   ```bash
   git clone <repository>
   cd cartridge-warp
   make install-dev
   ```

2. **Install Pre-commit Hooks**:
   ```bash
   pre-commit install
   ```

3. **Verify Setup**:
   ```bash
   make test
   make lint
   make type-check
   ```

4. **Development Cycle**:
   ```bash
   # Make changes
   # Test changes
   make test
   
   # Format and check
   make format lint type-check
   
   # Commit (pre-commit hooks will run automatically)
   git commit -m "feat: your changes"
   ```

## IDE Configuration

### VS Code
The development dependencies work seamlessly with VS Code Python extension:
- Black formatting on save
- Ruff linting in real-time
- MyPy type checking
- Pytest test discovery and execution

### PyCharm
Configure external tools for:
- Black: `black $FilePath$`
- Ruff: `ruff check $FilePath$`
- MyPy: `mypy $FilePath$`

## Dependency Management

### Adding New Dependencies

1. **Runtime Dependencies**: Add to `dependencies` in `pyproject.toml`
2. **Development Tools**: Add to `dev` group in `optional-dependencies`
3. **Testing Tools**: Add to `test` group in `optional-dependencies`
4. **Connector-Specific**: Create new group (e.g., `mysql`, `bigquery`)

### Version Pinning Strategy
- **Lower bounds**: Set for compatibility
- **Upper bounds**: Avoid for flexibility unless known breaking changes
- **Lock files**: Use `pip freeze > requirements.lock` for reproducible builds

### Updating Dependencies
```bash
# Check for outdated packages
pip list --outdated

# Update all development dependencies
pip install --upgrade -e ".[dev,test]"

# Test after updates
make test
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you've installed with `-e` flag for editable mode
2. **Type Check Failures**: Install type stubs: `pip install types-PyYAML types-setuptools`
3. **Test Failures**: Check if testcontainers can access Docker
4. **Memory Issues**: Use `pytest-xdist` for parallel execution to distribute load

### Clean Installation
```bash
# Remove existing installation
pip uninstall cartridge-warp

# Clean install
make install-dev

# Verify
make test
```

This comprehensive development setup ensures all contributors have the same high-quality development experience with consistent tooling and workflows.
