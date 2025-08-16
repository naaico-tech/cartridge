# Comprehensive Metadata Management System

The Cartridge-Warp metadata management system provides robust tracking and monitoring capabilities for CDC (Change Data Capture) operations. This system addresses all the requirements from issue #32 and provides a complete solution for production-grade data synchronization.

## Overview

The metadata management system consists of several key components:

- **Position Tracking**: Atomic tracking of CDC positions (LSN, resume tokens, timestamps)
- **Schema Registry**: Version control and evolution tracking for database schemas  
- **Sync Run Monitoring**: Complete lifecycle tracking of synchronization operations
- **Error Management**: Comprehensive error logging with retry logic and dead letter queue
- **Recovery Operations**: Automated recovery and cleanup mechanisms

## Core Components

### 1. MetadataManager

The central orchestrator that provides high-level APIs for all metadata operations:

```python
from cartridge_warp.metadata import MetadataManager
import asyncpg

# Initialize with database connection pool
pool = await asyncpg.create_pool("postgresql://localhost/warehouse")
manager = MetadataManager(
    connection_pool=pool,
    metadata_schema="cartridge_warp",  # Schema for metadata tables
    enable_cleanup=True,               # Enable automatic cleanup
    retention_days=30                  # Keep metadata for 30 days
)

# Initialize metadata tables
await manager.initialize()
```

### 2. Position Tracking

#### Stream Position Tracking
For real-time CDC using change streams, logical replication, etc:

```python
# Update stream position (MongoDB Change Stream example)
position_data = {
    "resume_token": {"_data": "8265A4F2EC000000012B0229296E04"},
    "cluster_time": "7123456789012345678",
    "timestamp": datetime.now(timezone.utc).isoformat()
}

await manager.update_stream_position(
    schema_name="ecommerce",
    position=position_data,
    table_name=None,  # Schema-level position
    sync_run_id=sync_run.id
)

# Retrieve current position
current_position = await manager.get_stream_position("ecommerce")
```

#### Batch Timestamp Tracking
For batch processing with timestamp-based change detection:

```python
# Update batch timestamp
last_sync_time = datetime(2023, 12, 1, 10, 30, 0, tzinfo=timezone.utc)
await manager.update_batch_timestamp(
    schema_name="ecommerce",
    timestamp=last_sync_time,
    table_name="orders"
)

# Retrieve current timestamp  
current_timestamp = await manager.get_batch_timestamp("ecommerce", "orders")
```

### 3. Schema Evolution Management

Track schema changes over time with full version history:

```python
from cartridge_warp.metadata import SchemaDefinition, EvolutionType

# Define initial schema
schema_def = SchemaDefinition(
    columns=[
        {"name": "id", "type": "INTEGER", "nullable": False, "primary_key": True},
        {"name": "name", "type": "TEXT", "nullable": True},
        {"name": "created_at", "type": "TIMESTAMP", "nullable": False}
    ],
    primary_keys=["id"],
    indexes=[
        {"name": "idx_created_at", "columns": ["created_at"]}
    ]
)

# Register schema version
registry = await manager.register_schema(
    schema_name="ecommerce",
    table_name="customers", 
    schema_definition=schema_def,
    evolution_type=EvolutionType.CREATE
)

# Later: evolve schema by adding columns
evolved_schema = SchemaDefinition(
    columns=[
        # ... existing columns ...
        {"name": "email", "type": "TEXT", "nullable": True},  # New column
        {"name": "phone", "type": "TEXT", "nullable": True}   # New column  
    ],
    primary_keys=["id"],
    indexes=[
        {"name": "idx_created_at", "columns": ["created_at"]},
        {"name": "idx_email", "columns": ["email"]}  # New index
    ]
)

# Register evolution
evolved_registry = await manager.register_schema(
    schema_name="ecommerce",
    table_name="customers",
    schema_definition=evolved_schema,
    evolution_type=EvolutionType.ADD_COLUMN
)

# Retrieve schema versions
latest = await manager.get_schema_version("ecommerce", "customers")
version_1 = await manager.get_schema_version("ecommerce", "customers", version=1)
```

### 4. Sync Run Lifecycle

Monitor complete synchronization operations from start to finish:

```python
from cartridge_warp.metadata import SyncMode, SyncStatus, SyncRunStatistics

# Start sync run
sync_run = await manager.start_sync_run(
    schema_name="ecommerce",
    sync_mode=SyncMode.STREAM,
    config_hash="abc123",  # Hash of current configuration
    source_info={"type": "mongodb", "host": "prod-db"},  # No credentials
    destination_info={"type": "postgresql", "host": "warehouse"},
    instance_id="pod-123",
    node_id="worker-01"
)

# ... perform synchronization work ...

# Complete with statistics
statistics = SyncRunStatistics(
    records_processed=10000,
    records_inserted=8500, 
    records_updated=1200,
    records_deleted=300,
    bytes_processed=5 * 1024 * 1024  # 5MB
)

await manager.complete_sync_run(
    sync_run_id=sync_run.id,
    status=SyncStatus.COMPLETED,
    statistics=statistics
)
```

### 5. Error Handling & Dead Letter Queue

Comprehensive error logging with automatic retry logic:

```python
from cartridge_warp.metadata import ErrorType, OperationType

# Log error with full context
error_log = await manager.log_error(
    schema_name="ecommerce",
    table_name="orders",
    error_type=ErrorType.VALIDATION,
    error_message="Invalid date format in order_date field",
    sync_run_id=sync_run.id,
    error_code="VAL_001",
    error_details={"field": "order_date", "value": "2023-13-45"},
    stack_trace="ValidationError: time data '2023-13-45' does not match format '%Y-%m-%d'",
    record_data={"id": 12345, "order_date": "2023-13-45"},
    operation_type=OperationType.INSERT,
    max_retries=3
)

# Add problematic record to dead letter queue
dlq_record = await manager.add_to_dead_letter_queue(
    schema_name="ecommerce",
    table_name="orders",
    operation_type=OperationType.INSERT,
    record_data={"id": 12345, "order_date": "2023-13-45"},
    sync_run_id=sync_run.id,
    error_log_id=error_log.id,
    source_record_id="mongo_obj_12345",
    error_message="Validation failed after 3 retries"
)
```

### 6. Recovery & Monitoring

Automated recovery and comprehensive monitoring:

```python
# Recover stuck sync runs
recovered_runs = await manager.recover_failed_runs(max_age_hours=24)
print(f"Recovered {len(recovered_runs)} stuck sync runs")

# Get comprehensive statistics
stats = await manager.get_sync_statistics(schema_name="ecommerce", hours=24)
print(f"Processed {stats['sync_runs']['total_records_processed']} records")
print(f"Success rate: {stats['sync_runs']['completed_runs'] / stats['sync_runs']['total_runs'] * 100:.1f}%")

# Get active position markers
markers = await manager.get_active_markers()
for marker in markers:
    print(f"Schema {marker.schema_name}: last updated {marker.last_updated}")

# Manual cleanup (usually runs automatically)
cleanup_stats = await manager.cleanup_old_metadata()
print(f"Cleaned up {sum(cleanup_stats.values())} old records")
```

## Database Schema

The system creates several metadata tables:

### sync_markers
Stores CDC position information:
- `id`: Unique identifier
- `schema_name`: Source schema name  
- `table_name`: Source table name (nullable for schema-level markers)
- `marker_type`: 'stream', 'batch', or 'initial'
- `position_data`: JSONB containing position information (LSN, tokens, timestamps)
- `last_updated`: When position was last updated
- `sync_run_id`: Associated sync run

### schema_registry  
Tracks schema evolution:
- `id`: Unique identifier
- `schema_name`, `table_name`: Source schema and table
- `version`: Schema version number
- `schema_definition`: Complete schema as JSONB
- `schema_hash`: SHA-256 hash for duplicate detection
- `evolution_type`: Type of change (create, add_column, etc.)
- `previous_version`: Link to previous schema version

### sync_runs
Monitors sync operations:
- `id`: Unique identifier  
- `schema_name`: Schema being synchronized
- `sync_mode`: 'stream', 'batch', or 'initial'
- `status`: 'running', 'completed', 'failed', 'cancelled'
- `started_at`, `completed_at`: Timing information
- `duration_ms`: Total run duration
- Statistics fields: `records_processed`, `records_inserted`, etc.
- Configuration: `config_hash`, `source_info`, `destination_info`
- Error information: `error_message`, `error_details`

### error_log
Comprehensive error tracking:
- `id`: Unique identifier
- `sync_run_id`: Associated sync run  
- `schema_name`, `table_name`: Source location
- `error_type`: Category of error (connection, validation, etc.)
- `error_message`, `error_details`: Error information
- `stack_trace`: Full stack trace
- `record_data`: The record that caused the error
- `retry_count`, `max_retries`: Retry logic
- `status`: 'open', 'resolved', 'ignored'

### dead_letter_queue
Failed record management:
- `id`: Unique identifier
- `sync_run_id`, `error_log_id`: Associated records
- `schema_name`, `table_name`: Destination location
- `operation_type`: 'insert', 'update', 'delete'
- `record_data`: The problematic record
- `error_count`: Number of failures
- `status`: 'pending', 'processing', 'resolved', 'discarded'

## Configuration

The MetadataManager accepts several configuration options:

```python
manager = MetadataManager(
    connection_pool=pool,              # AsyncPG connection pool
    metadata_schema="cartridge_warp",  # Schema for metadata tables
    enable_cleanup=True,               # Enable background cleanup
    retention_days=30                  # Days to retain historical data
)
```

## Performance Considerations

### Indexing
All metadata tables are properly indexed for common query patterns:
- Position lookups by schema/table/type
- Schema lookups by name and version
- Sync run queries by status and time range
- Error queries by type and status
- DLQ queries by status and error count

### Connection Pooling
The system uses AsyncPG connection pooling for optimal database performance.

### Caching
Frequently accessed data (positions, latest schemas) is cached in memory with automatic invalidation.

### Background Cleanup
Automatic cleanup prevents metadata tables from growing indefinitely:
- Completed sync runs older than retention period
- Resolved errors and DLQ records
- Old schema versions (keeps last 10 versions)

## Error Handling

The system provides comprehensive error handling:

1. **Automatic Retries**: Configurable retry logic with exponential backoff
2. **Dead Letter Queue**: Failed records are quarantined for manual intervention  
3. **Error Classification**: Errors are categorized for better debugging
4. **Context Preservation**: Full context (stack traces, record data) is preserved
5. **Recovery Mechanisms**: Stuck operations are automatically detected and recovered

## Monitoring & Alerting

The system provides rich monitoring capabilities:

- **Real-time Statistics**: Success rates, throughput, error rates
- **Position Tracking**: Current sync positions across all schemas
- **Performance Metrics**: Average run times, data volumes processed  
- **Error Trends**: Error patterns and retry success rates
- **Resource Usage**: Connection pool utilization, memory usage

## Production Deployment

### Database Setup
1. Create a dedicated database or schema for metadata
2. Ensure proper connection pooling configuration
3. Set up monitoring for metadata table sizes
4. Configure backup and restore procedures

### Configuration
1. Set appropriate retention periods based on compliance requirements
2. Configure cleanup schedules for off-peak hours
3. Set up alerting for error rate thresholds
4. Monitor disk usage for metadata tables

### Security
1. Use dedicated database credentials with minimal required permissions
2. Encrypt sensitive configuration data
3. Audit metadata access and modifications
4. Implement secure credential rotation

## Migration Guide

If upgrading from the basic metadata manager:

1. **Backup existing metadata** before migration
2. **Run schema migration scripts** to create new tables
3. **Migrate existing position data** to new format
4. **Update application code** to use new APIs
5. **Test recovery procedures** with new system
6. **Monitor performance** during initial deployment

## Troubleshooting

### Common Issues

**Stuck Sync Runs**: Use `recover_failed_runs()` to automatically detect and recover stuck operations.

**Schema Evolution Conflicts**: Check schema registry for version history and compatibility issues.

**High Error Rates**: Query error_log table for patterns and root causes.

**DLQ Growth**: Monitor dead letter queue and implement reprocessing workflows.

**Performance Issues**: Check connection pool settings and database indexes.

### Debugging Queries

```sql
-- Check sync run performance
SELECT schema_name, AVG(duration_ms) as avg_duration_ms, 
       COUNT(*) as runs, SUM(records_processed) as total_records
FROM cartridge_warp.sync_runs 
WHERE completed_at > NOW() - INTERVAL '24 hours'
GROUP BY schema_name;

-- Monitor error patterns  
SELECT error_type, COUNT(*) as error_count,
       COUNT(CASE WHEN status = 'open' THEN 1 END) as open_errors
FROM cartridge_warp.error_log
WHERE occurred_at > NOW() - INTERVAL '24 hours'
GROUP BY error_type;

-- Dead letter queue status
SELECT schema_name, table_name, status, COUNT(*) as record_count,
       AVG(error_count) as avg_errors
FROM cartridge_warp.dead_letter_queue
GROUP BY schema_name, table_name, status;
```

For more examples and advanced usage, see the `examples/metadata_system_demo.py` script.
