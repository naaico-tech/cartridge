# GitHub Copilot Instructions for Cartridge

This document provides essential knowledge for AI coding agents to be immediately productive in the Cartridge codebase.

## 🏗️ Project Architecture

Cartridge is a **modular data engineering toolkit** with 5 independent but interconnected components:

### Core Components
1. **`cartridge-init/`** - Schema inference and dbt model generation (AI-powered)
2. **`cartridge-orchestrator/`** - Airflow DAG generation and workflow management
3. **`cartridge-analytics/`** - Data summary layer and reporting
4. **`cartridge-warp/`** - CDC streaming platform (MongoDB→PostgreSQL+)
5. **`cartridge-deployer/`** - Kubernetes and infrastructure bootstrapping

### Component Relationships
- **cartridge-init** outputs schema definitions that **cartridge-warp** uses for CDC configuration
- **cartridge-warp** streams data that **cartridge-analytics** summarizes
- **cartridge-orchestrator** coordinates workflows across all components
- **cartridge-deployer** manages deployment of the entire stack

## 🚀 Development Workflow

### Environment Setup (Always Required)
```bash
# 1. Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # macOS/Linux

# 2. Navigate to specific component
cd cartridge-{init|warp|orchestrator|analytics|deployer}/

# 3. Install with development dependencies
make install-dev
# OR: pip install -e ".[dev,test]"
```

### Code Quality Pipeline (Required Before Commits)
```bash
# Format code
make format

# Run linting
make lint

# Type checking
make type-check

# Run all quality checks
make format lint type-check

# Run tests with coverage
make test-coverage
```

### Testing Strategy
- **Unit tests**: Always required when modifying `src/` functions
- **Integration tests**: Required for database/API changes
- **End-to-end tests**: Required for CLI/workflow changes

```bash
# Unit tests only
pytest tests/unit/

# Integration tests (requires services)
docker-compose up -d
pytest tests/integration/

# All tests with coverage
make test-coverage
```

## 🔧 cartridge-warp Specific Patterns

### Configuration Architecture
- **Pydantic-based** with strict validation
- **Hierarchical**: Global → Schema → Table level configuration
- **Environment variables**: `CARTRIDGE_WARP_` prefix
- **YAML/JSON files** with schema validation

```python
# Configuration pattern
class WarpConfig(BaseSettings):
    mode: Literal["single", "multi"] = "single"
    source: SourceConfig
    destination: DestinationConfig
    schemas: List[SchemaConfig]
    
    class Config:
        env_prefix = "CARTRIDGE_WARP_"
```

### Execution Modes
1. **Single Schema Mode** (Production/K8s): One schema per process
2. **Multi-Schema Mode** (Development): Multiple schemas per process

### Connector Pattern
- **Protocol-based interfaces** for type safety
- **Factory pattern** for connector instantiation
- **Async/await** throughout for high concurrency

```python
# Connector interface pattern
from typing import Protocol

class SourceConnector(Protocol):
    async def connect(self) -> None: ...
    async def get_changes(self) -> AsyncGenerator[Change, None]: ...
    async def disconnect(self) -> None: ...
```

### Error Handling
- **Tenacity-based** retry with exponential backoff
- **Dead letter queues** for failed records
- **Structured logging** with correlation IDs

## 🔧 cartridge-init Specific Patterns

### AI Provider Integration
- **Multi-provider support**: OpenAI, Anthropic, Gemini
- **Factory pattern** for provider instantiation
- **Streaming responses** for large model outputs

### Configuration-Driven
- **Multi-database** scanning with single config file
- **Business context** integration for intelligent model generation
- **Schema evolution** detection and adaptation

## 📊 Technology Stack Conventions

### Python Standards
- **Python 3.9+** minimum requirement
- **Async/await** preferred over sync where possible
- **Type hints** required (mypy strict mode)
- **Pydantic v2** for all configuration and data models

### Database Patterns
- **SQLAlchemy 2.0+** for ORM operations
- **Asyncpg/AsyncPG** for PostgreSQL async operations
- **Motor** for MongoDB async operations
- **Connection pooling** always configured

### Monitoring & Observability
- **Prometheus metrics** for all components
- **Structured logging** with `structlog`
- **Rich** for CLI output formatting
- **Health checks** on all services

## 🐳 Docker & Deployment

### Container Standards
- **Multi-stage builds** for production optimization
- **Non-root user** execution
- **Health checks** in all Dockerfiles
- **Environment-based** configuration

### Kubernetes Patterns
- **Helm charts** for complex deployments
- **ConfigMaps** for configuration
- **Secrets** for sensitive data
- **Resource limits** always specified

## 🧪 Testing Conventions

### Test Structure
```
tests/
├── unit/           # Fast, isolated tests
├── integration/    # Database/API integration
└── performance/    # Load and performance tests
```

### Test Requirements
- **pytest-asyncio** for async test support
- **testcontainers** for integration tests
- **pytest-cov** for coverage reporting
- **Factory pattern** for test data generation

### Mocking Strategy
- **Mock external services** (databases, APIs) in unit tests
- **Real services** in integration tests via testcontainers
- **Dependency injection** to support both patterns

## 📝 Documentation Standards

### Code Documentation
- **Docstrings** required for all public functions/classes
- **Type hints** more important than docstring descriptions
- **Examples** in docstrings for complex functions

### Configuration Documentation
- **Schema examples** for all configuration options
- **Environment variable** documentation
- **Migration guides** for configuration changes

## 🔀 Git & CI/CD Patterns

### Branch Strategy
- **Feature branches** from master
- **No direct commits** to master
- **Squash merges** preferred

### Commit Messages
```
feat(warp): add MongoDB change stream support
fix(init): resolve BigQuery schema inference
docs(analytics): update API documentation
test(orchestrator): add integration tests for DAG generation
```

### Pre-commit Hooks
- **Black** formatting
- **Ruff** linting
- **mypy** type checking
- **pytest** on affected tests

## 🚨 Common Pitfalls & Solutions

### Configuration Issues
- **Environment variables** override file configuration
- **Schema validation** errors point to specific fields
- **Connection strings** must be URL-encoded

### Async Programming
- **Always use `await`** with async database operations
- **Connection pools** must be properly closed
- **Timeout handling** required for all network operations

### Database Connections
- **Connection pooling** prevents connection exhaustion
- **Proper cleanup** in finally blocks or context managers
- **Retry logic** for transient connection failures

## 🎯 Component-Specific Quick Reference

### cartridge-warp Development
```bash
# Start backing services
docker-compose up -d mongodb postgresql

# Run single schema mode
cartridge-warp run --mode single --config examples/mongodb-to-postgresql-single.yaml

# Run with environment variables
export CARTRIDGE_WARP_SOURCE__CONNECTION_STRING="mongodb://admin:password@localhost:27017"
cartridge-warp run --config config.yaml
```

### cartridge-init Development
```bash
# Scan single database
cartridge scan --connection-string "postgresql://user:pass@localhost:5432/db" --output schema.json

# Generate dbt models
cartridge generate --input schema.json --output-dir models/ --ai-provider openai
```

## 🔍 Debugging & Troubleshooting

### Logging Configuration
```python
import structlog

# Standard logging setup
logger = structlog.get_logger(__name__)
logger.info("Operation completed", table_name="users", records_processed=1000)
```

### Performance Monitoring
- **Prometheus metrics** exposed on `:8000/metrics`
- **Database query timing** logged automatically
- **Memory usage** tracked for large operations

### Common Debug Commands
```bash
# Check service health
curl http://localhost:8000/health

# View Prometheus metrics
curl http://localhost:8000/metrics

# Check configuration parsing
cartridge-warp validate --config config.yaml

# Dry run mode
cartridge-warp run --config config.yaml --dry-run
```

## 🎓 Key Learning Resources

- **PLAN.md** files in each component for architecture details
- **DEVELOPMENT.md** files for component-specific setup
- **examples/** directories for configuration patterns
- **tests/** directories for usage patterns and edge cases

---

*This document is automatically updated as the codebase evolves. Always refer to the latest version in the main branch.*
