# PostgreSQL Destination Connector

The PostgreSQL destination connector provides comprehensive support for streaming and batch data synchronization to PostgreSQL databases with advanced features for production use.

## Features

### ✅ UPSERT Operations
- **ON CONFLICT clause support** for efficient conflict resolution
- **Primary key and unique constraint handling**
- **Bulk insert optimizations** with configurable batch sizes
- **Transaction management** with automatic rollback on failures
- **Configurable batch processing** for memory efficiency

### ✅ Schema Management
- **Automatic schema and table creation**
- **Dynamic column addition** for schema evolution
- **Comprehensive data type mapping** from source systems
- **JSONB support** for complex nested objects and arrays
- **Index management** with automatic creation and optimization

### ✅ Data Type Conversion
- **MongoDB BSON to PostgreSQL type mapping**
- **Object/Array to JSONB conversion** with structure preservation
- **Type widening support** (int → bigint, float → double)
- **Type narrowing with safety warnings**
- **NULL handling** for missing or undefined fields

### ✅ Soft/Hard Delete Support
- **Configurable deletion strategies** (soft, hard, or both)
- **is_deleted flag management** with automatic timestamp tracking
- **Audit trail** for deleted records with version tracking
- **Cascading delete handling** (planned)

### ✅ Performance Optimization
- **Connection pooling** with asyncpg for high concurrency
- **Prepared statement caching** for frequently executed queries
- **Batch write optimization** with configurable sizes
- **Memory usage monitoring** and automatic cleanup
- **Concurrent processing** support for multiple schemas

## Quick Start

### Basic Configuration

```yaml
destination:
  type: "postgresql"
  connection_string: "postgresql://user:password@localhost:5432/warehouse"
  metadata_schema: "cartridge_warp_metadata"
```

### Advanced Configuration

```yaml
destination:
  type: "postgresql"
  connection_string: "postgresql://user:password@localhost:5432/warehouse"
  metadata_schema: "cartridge_warp_metadata"
  
  # Connection settings
  max_connections: 20
  min_connections: 5
  connection_timeout: 30.0
  command_timeout: 60.0
  
  # Performance settings
  batch_size: 1000
  max_retries: 3
  
  # Delete strategy
  deletion_strategy: "soft"  # "soft", "hard", "both"
  enable_soft_deletes: true
  
  # UPSERT mode
  upsert_mode: "on_conflict"  # "on_conflict", "merge"
```

## Data Type Mapping

The PostgreSQL connector automatically maps source data types to appropriate PostgreSQL types:

| Source Type | PostgreSQL Type | Notes |
|-------------|-----------------|-------|
| String | TEXT / VARCHAR(n) | VARCHAR for strings ≤255 chars |
| Integer | INTEGER | 32-bit signed integer |
| BigInt | BIGINT | 64-bit signed integer |
| Float | REAL | Single precision |
| Double | DOUBLE PRECISION | Double precision |
| Boolean | BOOLEAN | True/false values |
| Timestamp | TIMESTAMP WITH TIME ZONE | UTC timestamps |
| Date | DATE | Date only |
| JSON/Object | JSONB | Structured data with indexing |
| Binary | BYTEA | Binary data |

### Type Conversion Examples

```python
# MongoDB document
{
    "_id": ObjectId("..."),
    "name": "John Doe",
    "age": 30,
    "salary": 75000.50,
    "is_active": True,
    "created_at": ISODate("2024-01-15T10:30:00Z"),
    "metadata": {
        "plan": "pro",
        "features": ["analytics", "api_access"]
    }
}

# PostgreSQL record
{
    "_id": "507f1f77bcf86cd799439011",  -- TEXT
    "name": "John Doe",                 -- TEXT
    "age": 30,                          -- INTEGER
    "salary": 75000.50,                 -- DOUBLE PRECISION
    "is_active": true,                  -- BOOLEAN
    "created_at": "2024-01-15T10:30:00+00:00",  -- TIMESTAMP WITH TIME ZONE
    "metadata": '{"plan":"pro","features":["analytics","api_access"]}'  -- JSONB
}
```

## UPSERT Operations

The connector supports intelligent conflict resolution using PostgreSQL's `ON CONFLICT` clause:

### Primary Key Conflicts

```sql
INSERT INTO schema.table (id, name, email, updated_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (id)
DO UPDATE SET 
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    updated_at = EXCLUDED.updated_at,
    _cartridge_version = _cartridge_version + 1
```

### Unique Constraint Conflicts

```sql
INSERT INTO schema.table (email, name, created_at)
VALUES ($1, $2, $3)
ON CONFLICT (email)
DO UPDATE SET 
    name = EXCLUDED.name,
    _cartridge_updated_at = NOW(),
    _cartridge_version = _cartridge_version + 1
```

## Schema Evolution

The connector supports safe schema evolution with automatic column addition:

### Adding New Columns

```python
# Schema change event
change = SchemaChange(
    schema_name="ecommerce",
    table_name="users", 
    change_type="add_column",
    details={
        "column_name": "phone_number",
        "column_type": "string", 
        "nullable": True,
        "default": None
    }
)
```

### Type Widening

Safe type widenings are automatically applied:

- `INTEGER` → `BIGINT`
- `REAL` → `DOUBLE PRECISION`
- `TEXT` → `JSONB`

Unsafe type changes (narrowing) are logged as warnings and skipped for data safety.

## Delete Strategies

### Soft Deletes (Recommended)

Soft deletes preserve data by marking records as deleted:

```sql
UPDATE schema.table 
SET is_deleted = true,
    deleted_at = NOW(),
    _cartridge_updated_at = NOW(),
    _cartridge_version = _cartridge_version + 1
WHERE id = $1 AND (is_deleted IS NULL OR is_deleted = false)
```

### Hard Deletes

Hard deletes permanently remove records:

```sql
DELETE FROM schema.table WHERE id = $1
```

### Both Strategies

You can enable both strategies to maintain an audit trail while cleaning up old records.

## Performance Characteristics

### Throughput Benchmarks

- **10,000+ records/second** for batch inserts
- **5,000+ records/second** for UPSERT operations
- **1,000+ records/second** for complex JSON documents

### Memory Usage

- **Configurable batch sizes** to control memory consumption
- **Connection pooling** to optimize resource usage
- **Automatic cleanup** of large result sets

### Scalability

- **Horizontal scaling** through multiple connector instances
- **Schema-level parallelism** for independent processing
- **Concurrent batch processing** within schemas

## Monitoring and Observability

### Prometheus Metrics

The connector exposes comprehensive metrics:

```python
# Record processing metrics
cartridge_warp_records_processed_total{schema="ecommerce", table="users", operation="insert"}
cartridge_warp_records_processed_total{schema="ecommerce", table="users", operation="update"}
cartridge_warp_records_processed_total{schema="ecommerce", table="users", operation="delete"}

# Performance metrics
cartridge_warp_batch_processing_duration_seconds{schema="ecommerce", table="users"}
cartridge_warp_connection_pool_active_connections{database="warehouse"}
cartridge_warp_connection_pool_idle_connections{database="warehouse"}

# Error metrics
cartridge_warp_errors_total{schema="ecommerce", table="users", error_type="connection"}
cartridge_warp_errors_total{schema="ecommerce", table="users", error_type="constraint_violation"}
```

### Structured Logging

All operations are logged with structured context:

```python
logger.info(
    "Batch processed successfully",
    schema="ecommerce",
    table="users",
    records=1000,
    duration=2.5,
    operations={"insert": 800, "update": 150, "delete": 50}
)
```

## Error Handling and Recovery

### Transactional Safety

- **All batch operations** are wrapped in transactions
- **Automatic rollback** on any failure within a batch
- **Partial batch recovery** with failed record identification

### Retry Logic

- **Exponential backoff** for transient connection errors
- **Configurable retry attempts** (default: 3)
- **Dead letter queue** for persistently failing records

### Connection Recovery

- **Automatic reconnection** for lost connections
- **Connection health checks** before processing
- **Pool recreation** for corrupted connection pools

## Best Practices

### Configuration

1. **Set appropriate batch sizes** based on your data volume and memory constraints
2. **Configure connection pools** based on your PostgreSQL server capacity
3. **Enable soft deletes** for better data lineage and recovery
4. **Use JSONB** for complex nested structures instead of flattening

### Performance

1. **Create appropriate indexes** on frequently queried columns
2. **Monitor connection pool utilization** and adjust limits
3. **Use table partitioning** for large tables with time-series data
4. **Enable prepared statements** for better query performance

### Schema Design

1. **Include primary keys** in all source tables for efficient UPSERTs
2. **Use meaningful column names** that match your analytics requirements
3. **Consider data types carefully** - avoid unnecessary type conversions
4. **Plan for schema evolution** by designing flexible table structures

## Troubleshooting

### Common Issues

#### Connection Pool Exhaustion

```python
# Symptoms: "connection pool exhausted" errors
# Solution: Increase max_connections or reduce batch_size

destination:
  max_connections: 30  # Increase from default
  batch_size: 500      # Reduce from default
```

#### Slow Performance

```python
# Check connection pool settings
destination:
  max_connections: 20
  min_connections: 5
  
# Optimize batch sizes
destination:
  batch_size: 2000  # Increase for better throughput
```

#### Schema Evolution Errors

```python
# Enable detailed logging for schema changes
import structlog
logger = structlog.get_logger(__name__)
logger.setLevel("DEBUG")
```

### Debugging

Enable detailed logging for troubleshooting:

```python
import structlog
import logging

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    logger_factory=structlog.WriteLoggerFactory(),
    cache_logger_on_first_use=True,
)
```

## API Reference

### PostgreSQLDestinationConnector

Main connector class implementing the DestinationConnector protocol.

#### Constructor Parameters

```python
PostgreSQLDestinationConnector(
    connection_string: str,
    metadata_schema: str = "cartridge_warp_metadata",
    batch_size: int = 1000,
    max_connections: int = 10,
    min_connections: int = 2,
    connection_timeout: float = 30.0,
    command_timeout: float = 60.0,
    enable_soft_deletes: bool = True,
    deletion_strategy: str = "soft",
    upsert_mode: str = "on_conflict",
    max_retries: int = 3,
)
```

#### Methods

```python
async def connect() -> None
async def disconnect() -> None
async def test_connection() -> bool
async def create_schema_if_not_exists(schema_name: str) -> None
async def create_table_if_not_exists(schema_name: str, table_schema: TableSchema) -> None
async def write_batch(schema_name: str, records: List[Record]) -> None
async def apply_schema_changes(schema_name: str, changes: List[SchemaChange]) -> None
async def update_marker(schema_name: str, table_name: str, marker: Any) -> None
async def get_marker(schema_name: str, table_name: str) -> Optional[Any]
```

## Migration Guide

### From Basic PostgreSQL Drivers

If you're migrating from basic PostgreSQL drivers:

1. **Update connection strings** to use asyncpg format
2. **Configure batch processing** instead of single-record operations
3. **Enable connection pooling** for better performance
4. **Add error handling** for transient failures

### From Other CDC Tools

Key differences when migrating from other CDC tools:

1. **Schema evolution** is handled automatically
2. **UPSERT operations** use native PostgreSQL features
3. **Soft deletes** are enabled by default
4. **Metadata tracking** is built-in

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines on contributing to the PostgreSQL destination connector.

## License

MIT License - see [LICENSE](../../LICENSE) for details.
