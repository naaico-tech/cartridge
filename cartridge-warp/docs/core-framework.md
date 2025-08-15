# Cartridge-Warp Core Framework

This document describes the foundational architecture implemented for cartridge-warp CDC streaming platform as specified in [Issue #29](https://github.com/naaico-tech/cartridge/issues/29).

## Overview

The core framework provides the foundational interfaces, configuration system, and runner architecture for building a modular CDC streaming platform with support for multiple databases and deployment modes.

## Architecture Components

### 1. Base Connector Interfaces

#### SourceConnector Protocol
Defines the interface for reading data from source databases:

```python
from cartridge_warp.connectors import SourceConnector

async def get_schema(schema_name: str) -> DatabaseSchema
async def get_changes(schema_name: str, marker=None, batch_size=1000) -> AsyncIterator[ChangeEvent]
async def get_full_snapshot(schema_name: str, table_name: str, batch_size=10000) -> AsyncIterator[Record]
async def connect() -> None
async def disconnect() -> None
async def test_connection() -> bool
```

#### DestinationConnector Protocol
Defines the interface for writing data to destination databases:

```python
from cartridge_warp.connectors import DestinationConnector

async def write_batch(schema_name: str, records: List[Record]) -> None
async def apply_schema_changes(schema_name: str, changes: List[SchemaChange]) -> None
async def update_marker(schema_name: str, table_name: str, marker: Any) -> None
async def get_marker(schema_name: str, table_name: str) -> Optional[Any]
async def create_schema_if_not_exists(schema_name: str) -> None
async def create_table_if_not_exists(schema_name: str, table_schema: TableSchema) -> None
async def connect() -> None
async def disconnect() -> None
async def test_connection() -> bool
```

### 2. Abstract Base Classes

#### BaseSourceConnector
Provides common functionality for source connector implementations:

```python
from cartridge_warp.connectors import BaseSourceConnector

class MySourceConnector(BaseSourceConnector):
    async def get_schema(self, schema_name: str) -> DatabaseSchema:
        # Implementation specific to your database
        pass
    
    async def get_changes(self, schema_name: str, marker=None, batch_size=1000):
        # Implementation for change detection
        pass
```

#### BaseDestinationConnector
Provides common functionality for destination connector implementations:

```python
from cartridge_warp.connectors import BaseDestinationConnector

class MyDestinationConnector(BaseDestinationConnector):
    async def write_batch(self, schema_name: str, records: List[Record]):
        # Implementation for writing records
        pass
```

### 3. Connector Factory with Registration System

#### Registration
Use decorators to register connector implementations:

```python
from cartridge_warp.connectors import register_source_connector, register_destination_connector

@register_source_connector("mongodb")
class MongoDBSourceConnector(BaseSourceConnector):
    # Implementation
    pass

@register_destination_connector("postgresql")
class PostgreSQLDestinationConnector(BaseDestinationConnector):
    # Implementation
    pass
```

#### Factory Usage
Create connectors dynamically based on configuration:

```python
from cartridge_warp.connectors import ConnectorFactory

factory = ConnectorFactory()

# Create source connector
source_connector = await factory.create_source_connector(source_config)

# Create destination connector
dest_connector = await factory.create_destination_connector(dest_config)

# List available connector types
available = factory.list_available_connectors()
```

### 4. Configuration Management System

#### Type-Safe Configuration
Pydantic models provide type safety and validation:

```python
from cartridge_warp.core import WarpConfig

# Load from YAML file
config = WarpConfig.from_file("config.yaml")

# Create programmatically
config = WarpConfig(
    mode="single",
    single_schema_name="ecommerce",
    source=SourceConfig(
        type="mongodb",
        connection_string="mongodb://localhost:27017"
    ),
    destination=DestinationConfig(
        type="postgresql",
        connection_string="postgresql://localhost:5432/warehouse"
    ),
    schemas=[
        SchemaConfig(
            name="ecommerce",
            tables=[
                TableConfig(
                    name="users",
                    stream_batch_size=500
                )
            ]
        )
    ]
)
```

#### Environment Variable Support
Configuration supports environment variables with `CARTRIDGE_WARP_` prefix:

```bash
export CARTRIDGE_WARP_SOURCE__TYPE=mongodb
export CARTRIDGE_WARP_SOURCE__CONNECTION_STRING=mongodb://localhost:27017
export CARTRIDGE_WARP_DESTINATION__TYPE=postgresql
```

#### Hierarchical Configuration
Configuration cascades from global → schema → table level:

```yaml
# Global defaults
monitoring:
  log_level: "INFO"

schemas:
  - name: "ecommerce"
    default_batch_size: 1000  # Schema-level override
    tables:
      - name: "users"
        stream_batch_size: 500  # Table-level override
```

### 5. Core Runner Architecture

#### Async-Based Execution
The runner supports both single and multi-schema execution modes:

```python
from cartridge_warp.core import WarpRunner

# Initialize runner
runner = WarpRunner(config)

# Start processing
await runner.start()

# Stop gracefully
await runner.stop()
```

#### Single Schema Mode
Process one schema per runner instance (recommended for production):

```yaml
mode: "single"
single_schema_name: "ecommerce"
```

Benefits:
- Resource isolation
- Independent scaling
- Fault isolation
- Better monitoring granularity

#### Multi-Schema Mode
Process multiple schemas in one runner instance:

```yaml
mode: "multi"
schemas:
  - name: "schema1"
  - name: "schema2"
  - name: "schema3"
```

Benefits:
- Lower resource overhead
- Simplified deployment for development

### 6. Schema Processor

Independent processing for each schema with dedicated processors:

```python
from cartridge_warp.core import SchemaProcessor

processor = SchemaProcessor(
    schema_config,
    source_connector,
    destination_connector,
    metadata_manager,
    metrics_collector
)

# Start processing
await processor.start(full_resync=False)

# Stop processing
await processor.stop()

# Get status
status = processor.get_status()
```

## Data Models

### Core Data Types

```python
from cartridge_warp.connectors import (
    Record,
    ChangeEvent,
    SchemaChange,
    TableSchema,
    ColumnDefinition,
    OperationType,
    ColumnType
)

# Example record
record = Record(
    table_name="users",
    data={"id": 1, "name": "John", "email": "john@example.com"},
    operation=OperationType.INSERT,
    timestamp=datetime.now(),
    primary_key_values={"id": 1}
)

# Example change event
change_event = ChangeEvent(
    record=record,
    position_marker="lsn_12345",
    schema_name="ecommerce"
)
```

## Usage Examples

### Basic Setup

```python
import asyncio
from cartridge_warp.core import WarpConfig, WarpRunner

async def main():
    # Load configuration
    config = WarpConfig.from_file("config.yaml")
    
    # Create runner
    runner = WarpRunner(config)
    
    try:
        # Start processing
        await runner.start()
    except KeyboardInterrupt:
        # Graceful shutdown
        await runner.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

### Configuration Examples

See the `examples/` directory for complete configuration examples:
- `single-schema-config.yaml` - Single schema configuration
- `multi-schema-config.yaml` - Multi-schema configuration

### Custom Connector Implementation

```python
from cartridge_warp.connectors import BaseSourceConnector, register_source_connector

@register_source_connector("custom_db")
class CustomSourceConnector(BaseSourceConnector):
    async def connect(self):
        # Connect to your database
        pass
    
    async def get_schema(self, schema_name: str):
        # Return schema definition
        return DatabaseSchema(name=schema_name, tables=[...])
    
    async def get_changes(self, schema_name: str, marker=None, batch_size=1000):
        # Yield change events
        async def changes():
            # Your change detection logic
            yield ChangeEvent(...)
        return changes()
```

## Monitoring and Observability

### Structured Logging
Configure structured logging with correlation IDs:

```yaml
monitoring:
  log_level: "INFO"
  structured_logging: true
```

### Prometheus Metrics
Built-in metrics collection for monitoring:

```yaml
monitoring:
  prometheus:
    enabled: true
    port: 8080
    path: "/metrics"
```

Available metrics:
- `cartridge_warp_records_processed_total`
- `cartridge_warp_processing_rate`
- `cartridge_warp_lag_seconds`
- `cartridge_warp_sync_status`
- `cartridge_warp_error_count_total`

## Error Handling

### Graceful Shutdown
The runner handles shutdown signals gracefully:

```python
import signal

def signal_handler(signum, frame):
    asyncio.create_task(runner.stop())

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
```

### Error Recovery
Configurable error handling and retry logic:

```yaml
error_handling:
  max_retries: 3
  backoff_factor: 2.0
  max_backoff_seconds: 300
  ignore_type_conversion_errors: true
  log_conversion_warnings: true
```

## Testing

### Test Framework
Comprehensive tests are provided in `tests/test_core_framework.py`:

```bash
# Run tests
cd cartridge-warp
python -m pytest tests/test_core_framework.py -v
```

### Mock Connectors
Test connectors are available for unit testing:

```python
from tests.test_core_framework import TestSourceConnector, TestDestinationConnector

# Use in your tests
source = TestSourceConnector("test://connection")
```

## Next Steps

This core framework provides the foundation for:

1. **Phase 2**: Implementing specific database connectors (MongoDB, PostgreSQL, etc.)
2. **Phase 3**: Adding schema evolution engine
3. **Phase 4**: Performance optimizations and production readiness

## Success Criteria ✅

- [x] All connector interfaces defined with proper typing
- [x] Configuration system validates complex YAML files  
- [x] Runner can execute in both single and multi-schema modes
- [x] Comprehensive test coverage for core components
- [x] Abstract base classes for common connector functionality
- [x] Connector factory with registration system
- [x] Pydantic models for type-safe configuration
- [x] Environment variable support
- [x] Hierarchical configuration
- [x] Async-based execution engine
- [x] Schema processor for independent processing
- [x] Graceful shutdown handling
- [x] Structured logging with correlation IDs
