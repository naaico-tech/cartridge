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

🚧 **In Development** - This component is currently being built according to the plan outlined in PLAN.md.

### Current Phase: Core Framework
- [ ] Base connector interfaces and abstractions
- [ ] Configuration management system
- [ ] Metadata storage layer
- [ ] Basic MongoDB → PostgreSQL connector
- [ ] Single schema execution mode
- [ ] Basic error handling and logging

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

## Contributing

See the main [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines.

## License

MIT License - see [LICENSE](../LICENSE) file for details.
