# Cartridge-Warp: CDC Streaming Platform

A modular Change Data Capture (CDC) streaming platform for real-time and batch data synchronization.

## Features

- **Dual Execution Modes**: Single schema per process (K8s optimized) or multi-schema per process
- **Schema Evolution**: Automatic schema change detection and intelligent type conversion
- **Modular Connectors**: Pluggable source and destination database adapters
- **Comprehensive Monitoring**: Prometheus metrics for throughput, health, and performance
- **Fault Tolerance**: Robust error handling with configurable retry strategies
- **Flexible Configuration**: Table-level performance tuning and behavior customization

## Quick Start

### Single Schema Mode
```bash
cartridge-warp \
  --mode single \
  --source mongodb://localhost:27017/ecommerce \
  --destination postgresql://localhost:5432/warehouse \
  --schema products \
  --config config.yaml
```

### Multi-Schema Mode
```bash
cartridge-warp \
  --mode multi \
  --config multi-schema-config.json
```

## Architecture

See [PLAN.md](./PLAN.md) for detailed architecture and implementation plan.

## Development Status

✅ **Core Framework Complete** - The foundational architecture has been implemented according to the plan in PLAN.md.

### ✅ Completed: Core Framework Implementation
- ✅ **Base connector interfaces and abstractions** - Protocol-based source/destination interfaces
- ✅ **Configuration management system** - Pydantic-based validation with YAML support
- ✅ **Connector factory with registration system** - Decorator-based plugin architecture
- ✅ **Runner architecture** - Async execution with single/multi-schema modes
- ✅ **Schema processor** - Independent table processing with change detection
- ✅ **Error handling and logging** - Structured logging with correlation IDs
- ✅ **Comprehensive test suite** - 11 tests covering core framework functionality

### 🚧 Next Phase: Database Connectors
- [ ] MongoDB source connector implementation
- [ ] PostgreSQL destination connector implementation
- [ ] Schema evolution and DDL change detection
- [ ] Full table sync capabilities
- [ ] Metadata storage layer

## Configuration

### Basic Configuration
```yaml
source:
  type: mongodb
  connection_string: "mongodb://localhost:27017"
  database: "ecommerce"
  
destination:
  type: postgresql
  connection_string: "postgresql://localhost:5432/warehouse"
  
schemas:
  - name: "products"
    mode: "stream"
    batch_size: 1000
  - name: "orders"
    mode: "batch"
    batch_size: 5000

monitoring:
  prometheus:
    enabled: true
    port: 8080
```

## Supported Connectors

### Phase 1 (In Progress)
- **Sources**: MongoDB
- **Destinations**: PostgreSQL

### Planned
- **Sources**: MySQL, BigQuery
- **Destinations**: BigQuery, MySQL

## Monitoring

Cartridge-warp exposes Prometheus metrics for:
- Record processing throughput
- Sync status and health
- Schema evolution events
- Error rates and types
- Performance metrics

## Development

### Quick Start
```bash
# Clone the repository
cd cartridge-warp

# Install development dependencies
make install-dev

# Run tests
make test

# Format code and run quality checks
make format lint type-check
```

### Installation Options
- **Development**: `make install-dev` - Core development tools
- **Complete**: `make install-all` - All optional dependencies
- **Custom**: `pip install -e ".[dev,test,bigquery]"` - Specific groups

See [DEVELOPMENT.md](./DEVELOPMENT.md) for detailed development setup and [docs/development-dependencies.md](./docs/development-dependencies.md) for dependency management guide.

## Contributing

See the main [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines.

## License

MIT License - see [LICENSE](../LICENSE) file for details.
