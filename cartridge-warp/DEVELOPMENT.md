# Development Guide for Cartridge-Warp

This guide covers the development setup, architecture, and implementation details for cartridge-warp.

## Quick Start

### 1. Development Environment Setup

```bash
# Clone the repository
cd cartridge-warp

# Install development dependencies
make install-dev

# Run tests to verify setup
make test

# Start development services
docker-compose up -d mongodb postgresql prometheus grafana
```

### 2. Configuration and Testing

```bash
# Create a test configuration
make example-init

# Validate configuration
make example-validate

# Run with sample configuration (dry-run mode)
cartridge-warp run --config examples/mongodb-to-postgresql-single.yaml --dry-run
```

## Architecture Overview

Cartridge-warp follows a modular architecture designed for scalability and maintainability:

```
cartridge_warp/
├── core/
│   ├── config.py      # Configuration management
│   └── runner.py      # Main execution engine
├── connectors/
│   ├── factory.py     # Connector factory
│   ├── mongodb.py     # MongoDB source connector
│   └── postgresql.py  # PostgreSQL destination connector
├── monitoring/
│   └── metrics.py     # Prometheus metrics
├── metadata/
│   └── manager.py     # Metadata management
└── cli.py             # Command-line interface
```

### Key Components

#### 1. Configuration System (`core/config.py`)
- **Pydantic-based** configuration with validation
- **Environment variable** support with `CARTRIDGE_WARP_` prefix
- **YAML/JSON** file loading with schema validation
- **Hierarchical configuration** (global → schema → table level)

#### 2. Execution Engine (`core/runner.py`)
- **Dual execution modes**: single schema vs multi-schema
- **Async/await** based for high concurrency
- **Schema processors** for independent schema handling
- **Error handling** with configurable retry logic

#### 3. Connector System (`connectors/`)
- **Protocol-based interfaces** for type safety
- **Factory pattern** for connector creation
- **Pluggable architecture** for easy extension
- **Connection pooling** and resource management

#### 4. Metadata Management (`metadata/`)
- **Position tracking** for stream and batch modes
- **Schema evolution** history and version management
- **Error logging** and recovery tracking
- **Performance metrics** storage

#### 5. Monitoring (`monitoring/`)
- **Prometheus metrics** exposition
- **Structured logging** with correlation IDs
- **Health checks** and status reporting
- **Performance monitoring** and alerting

## Implementation Phases

### Phase 1: Core Framework ✅ (Current)
- [x] Configuration system with Pydantic validation
- [x] Basic runner architecture with async support
- [x] CLI interface with rich output formatting
- [x] Project structure and development tooling
- [x] Docker and Docker Compose setup
- [x] Testing framework setup

### Phase 2: MongoDB → PostgreSQL Connector (Next)
- [ ] MongoDB source connector implementation
  - [ ] Change stream support for real-time CDC
  - [ ] Batch mode with timestamp-based querying
  - [ ] Schema introspection and evolution detection
- [ ] PostgreSQL destination connector implementation
  - [ ] Upsert operations with conflict resolution
  - [ ] Schema creation and evolution
  - [ ] JSONB support for complex data types
- [ ] Metadata manager implementation
  - [ ] PostgreSQL-based metadata storage
  - [ ] Position and timestamp tracking
  - [ ] Schema version management

### Phase 3: Production Features
- [ ] Comprehensive error handling and recovery
- [ ] Dead letter queue implementation
- [ ] Performance optimization and connection pooling
- [ ] Comprehensive monitoring and alerting
- [ ] Schema evolution with intelligent type conversion
- [ ] Soft delete and hard delete support

### Phase 4: Extended Connectors
- [ ] MySQL source and destination connectors
- [ ] BigQuery destination connector
- [ ] Additional NoSQL sources (e.g., DynamoDB)

## Development Workflow

### 1. Code Quality Standards

```bash
# Format code
make format

# Run linting
make lint

# Type checking
make type-check

# Run all quality checks
make format lint type-check
```

### 2. Testing Strategy

```bash
# Unit tests
pytest tests/unit/

# Integration tests (requires services)
docker-compose up -d
pytest tests/integration/

# Coverage report
make test-coverage
```

### 3. Local Development

```bash
# Start backing services
docker-compose up -d mongodb postgresql

# Run cartridge-warp locally
export CARTRIDGE_WARP_SOURCE__CONNECTION_STRING="mongodb://admin:password@localhost:27017"
export CARTRIDGE_WARP_DESTINATION__CONNECTION_STRING="postgresql://cartridge:cartridge_password@localhost:5432/warehouse"

cartridge-warp run --config examples/mongodb-to-postgresql-single.yaml
```

### 4. Monitoring and Debugging

```bash
# View Prometheus metrics
open http://localhost:9090

# View Grafana dashboards
open http://localhost:3000
# Login: admin/admin

# Check application logs
docker-compose logs -f cartridge-warp

# Monitor database changes
docker-compose exec mongodb mongo
docker-compose exec postgresql psql -U cartridge -d warehouse
```

## Configuration Examples

### Single Schema Configuration
```yaml
mode: single
single_schema_name: "ecommerce"

source:
  type: mongodb
  connection_string: "mongodb://localhost:27017"
  database: "ecommerce"

destination:
  type: postgresql
  connection_string: "postgresql://localhost:5432/warehouse"

schemas:
  - name: "ecommerce"
    mode: "stream"
    tables:
      - name: "products"
        stream_batch_size: 500
        deletion_strategy: "soft"
```

### Multi-Schema Configuration
```yaml
mode: multi

schemas:
  - name: "tenant_1"
    mode: "stream"
    default_batch_size: 1000
    
  - name: "tenant_2" 
    mode: "batch"
    schedule: "*/5 * * * *"
    default_batch_size: 5000
```

## Extending Cartridge-Warp

### Adding a New Source Connector

1. **Create connector class**:
```python
# src/cartridge_warp/connectors/newsource.py
from .base import SourceConnector

class NewSourceConnector(SourceConnector):
    async def stream_changes(self, schema_name: str, last_position, batch_size: int):
        # Implementation
        pass
```

2. **Register in factory**:
```python
# src/cartridge_warp/connectors/factory.py
def create_source_connector(self, config: SourceConfig):
    if config.type == "newsource":
        return NewSourceConnector(config)
```

3. **Add configuration**:
```python
# src/cartridge_warp/core/config.py
class SourceConfig(BaseModel):
    type: Literal["mongodb", "mysql", "postgresql", "bigquery", "newsource"]
```

### Adding Custom Metrics

```python
# src/cartridge_warp/monitoring/metrics.py
class MetricsCollector:
    def _init_metrics(self):
        self.custom_metric = Counter(
            'cartridge_warp_custom_total',
            'Custom metric description',
            ['label1', 'label2']
        )
```

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify database connection strings
   - Check network connectivity
   - Ensure proper authentication

2. **Schema Evolution Issues**
   - Check metadata tables for schema history
   - Verify type conversion settings
   - Review error logs in metadata.error_log

3. **Performance Issues**
   - Monitor Prometheus metrics
   - Adjust batch sizes in configuration
   - Check database query performance

4. **Memory Issues**
   - Reduce batch sizes
   - Monitor queue sizes
   - Check for connection leaks

### Debug Mode

```bash
# Enable debug logging
export CARTRIDGE_WARP_MONITORING__LOG_LEVEL=DEBUG

# Dry run mode
cartridge-warp run --config config.yaml --dry-run

# Single table processing
cartridge-warp run --config config.yaml --schema specific_schema
```

## Contributing

1. **Fork the repository**
2. **Create a feature branch**
3. **Implement changes with tests**
4. **Run quality checks**: `make format lint type-check test`
5. **Submit a pull request**

See [CONTRIBUTING.md](../CONTRIBUTING.md) for detailed guidelines.

## Performance Benchmarks

Target performance metrics for Phase 1:
- **Throughput**: 10,000 records/second for simple schemas
- **Latency**: < 1 second for change detection
- **Memory**: < 1GB for typical workloads
- **CPU**: < 50% utilization under normal load

## Security Considerations

- **Connection strings** should use environment variables or secure vaults
- **Database permissions** should follow principle of least privilege
- **Metadata encryption** for sensitive schema information
- **Audit logging** for all data operations
