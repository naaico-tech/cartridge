# Cartridge-Warp: CDC Streaming Platform Plan

## Overview
A Change Data Capture (CDC) streaming platform that provides real-time and batch data synchronization between various source and destination systems with comprehensive schema evolution and monitoring capabilities.

## Architecture Goals

### Core Principles
- **Modular Design**: Pluggable source and destination connectors
- **Scalable Deployment**: Support for Docker and Kubernetes with per-schema resource allocation
- **Schema Evolution**: Automatic handling of schema changes with intelligent type conversion
- **Monitoring**: Comprehensive Prometheus metrics for observability
- **Fault Tolerance**: Robust error handling and recovery mechanisms

## Detailed Requirements

### 1. Execution Modes

#### Mode A: Single Schema per Process (Recommended for Production)
- **Use Case**: Dockerized and Kubernetes deployments
- **Benefits**: 
  - Resource isolation and allocation per schema
  - Independent scaling
  - Fault isolation
  - Better monitoring granularity
- **Configuration**: Command-line arguments or environment variables

#### Mode B: Multi-Schema per Process
- **Use Case**: Development and resource-constrained environments
- **Benefits**: 
  - Lower resource overhead
  - Simplified deployment for small workloads
- **Configuration**: JSON configuration file with schema definitions

### 2. Metadata Management

#### Marker Storage
- **Location**: Dedicated schema in destination database (`cartridge_warp_metadata`)
- **Tables**:
  - `sync_markers`: Track processing positions for each source table
  - `schema_registry`: Store schema evolution history
  - `sync_runs`: Track execution history and status
  - `error_log`: Store error details and recovery actions

#### Marker Types
- **Stream Markers**: LSN, timestamp, or sequence-based positions
- **Batch Markers**: Last processed `updated_at` or configurable timestamp column
- **Schema Markers**: Version tracking for schema changes

### 3. Schema Evolution Strategy

#### Type Conversion Rules
- **Object Types → JSONB/VARCHAR**: Automatic conversion of complex objects
- **Type Widening**: int → bigint, float → double (allowed)
- **Type Narrowing**: Log warning, attempt conversion, fallback to string
- **New Columns**: Add with NULL values
- **Dropped Columns**: Set to NULL, maintain in destination for data integrity

#### Conflict Resolution
- **Drastic Type Changes**: Log error, skip column value, continue processing
- **Invalid Data**: Quarantine records in error table for manual review

### 4. Database Connector Architecture

#### Connector Interface
```python
class SourceConnector(ABC):
    @abstractmethod
    def get_schema(self) -> Dict
    @abstractmethod
    def get_changes(self, marker: Any) -> Iterator[ChangeEvent]
    @abstractmethod
    def get_full_snapshot(self) -> Iterator[Record]

class DestinationConnector(ABC):
    @abstractmethod
    def write_batch(self, records: List[Record]) -> None
    @abstractmethod
    def apply_schema_changes(self, changes: List[SchemaChange]) -> None
    @abstractmethod
    def update_marker(self, marker: Any) -> None
```

#### Initial Implementation Priority
1. **Phase 1**: MongoDB → PostgreSQL
2. **Phase 2**: MySQL → PostgreSQL, MongoDB → BigQuery
3. **Phase 3**: BigQuery ↔ PostgreSQL, MySQL ↔ BigQuery

### 5. Performance Configuration

#### Batch Size Controls
- **Stream Batch Size**: Number of changes processed per batch (default: 1000)
- **Full Load Batch Size**: Number of records in initial snapshot (default: 10000)
- **Write Batch Size**: Number of records written to destination per transaction (default: 500)
- **Polling Interval**: Frequency of change detection (default: 5 seconds)

#### Table-Level Configuration
```yaml
tables:
  users:
    stream_batch_size: 500
    write_batch_size: 250
    polling_interval: 2s
  orders:
    stream_batch_size: 2000
    write_batch_size: 1000
    polling_interval: 10s
```

### 6. Change Detection Configuration

#### Default Strategies
- **Stream Mode**: Use database change streams, triggers, or log-based CDC
- **Batch Mode**: Use `updated_at` timestamp (configurable column)
- **Fallback**: Full table comparison with hash-based change detection

#### Configurable Options
```yaml
source:
  change_detection:
    column: "last_modified"  # Override default updated_at
    strategy: "timestamp"    # timestamp, log, trigger
    timezone: "UTC"
```

### 7. Data Deletion Handling

#### Hard Delete (Default)
- Delete corresponding row in destination
- Log deletion event in audit table

#### Soft Delete (Configurable)
- Add `is_deleted` flag to destination table
- Set flag to `true` instead of deleting
- Maintain historical data for analysis

#### Configuration
```yaml
deletion_strategy: "soft"  # hard, soft
soft_delete_column: "is_deleted"
```

### 8. Error Handling & Recovery

#### Column-Level Errors
- **Type Conversion Failures**: Log warning, set NULL, continue
- **Constraint Violations**: Quarantine record, continue processing
- **Data Truncation**: Log warning, truncate with indication

#### Row-Level Errors
- **Duplicate Key Violations**: Update existing record (upsert)
- **Foreign Key Violations**: Queue for retry after dependencies

#### System-Level Errors
- **Connection Failures**: Exponential backoff retry
- **Schema Lock Conflicts**: Wait and retry with jitter

### 9. Prometheus Monitoring

#### Metrics Categories

##### Throughput Metrics
- `cartridge_warp_records_processed_total{database, schema, table, operation}`
- `cartridge_warp_processing_rate{database, schema, table}`
- `cartridge_warp_lag_seconds{database, schema, table}`

##### Health Metrics
- `cartridge_warp_sync_status{database, schema, table}` (active=1, inactive=0, error=-1)
- `cartridge_warp_last_successful_sync{database, schema, table}`
- `cartridge_warp_error_count_total{database, schema, table, error_type}`

##### Performance Metrics
- `cartridge_warp_batch_processing_duration_seconds{database, schema, table}`
- `cartridge_warp_queue_size{database, schema, table}`
- `cartridge_warp_memory_usage_bytes{database, schema}`

##### Schema Evolution Metrics
- `cartridge_warp_schema_changes_total{database, schema, table, change_type}`
- `cartridge_warp_type_conversion_warnings_total{database, schema, table}`

## Implementation Phases

### Phase 1: Core Framework (Weeks 1-3)
- [ ] Base connector interfaces and abstractions
- [ ] Configuration management system
- [ ] Metadata storage layer
- [ ] Basic MongoDB → PostgreSQL connector
- [ ] Single schema execution mode
- [ ] Basic error handling and logging

### Phase 2: Enhanced Features (Weeks 4-6)
- [ ] Multi-schema execution mode
- [ ] Schema evolution engine
- [ ] Prometheus metrics integration
- [ ] Batch size configuration
- [ ] Soft delete support
- [ ] Comprehensive error recovery

### Phase 3: Production Readiness (Weeks 7-9)
- [ ] Docker containerization
- [ ] Kubernetes deployment manifests
- [ ] Performance optimization
- [ ] Comprehensive testing suite
- [ ] Documentation and examples
- [ ] CI/CD pipeline

### Phase 4: Extended Connectors (Weeks 10-12)
- [ ] MySQL source connector
- [ ] BigQuery destination connector
- [ ] Advanced schema evolution features
- [ ] Performance benchmarking
- [ ] Production deployment guides

## Technology Stack

### Core Components
- **Language**: Python 3.9+
- **Framework**: asyncio for async processing
- **Configuration**: YAML/JSON with Pydantic validation
- **Metrics**: Prometheus client library
- **Logging**: Structured logging with correlation IDs

### Database Libraries
- **MongoDB**: motor (async) or pymongo
- **PostgreSQL**: asyncpg or psycopg3
- **MySQL**: aiomysql
- **BigQuery**: google-cloud-bigquery

### Infrastructure
- **Containerization**: Docker with multi-stage builds
- **Orchestration**: Kubernetes with Helm charts
- **Monitoring**: Prometheus + Grafana
- **CI/CD**: GitHub Actions

## Configuration Examples

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

### Configuration File
```yaml
# config.yaml
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
    schedule: "*/15 * * * *"  # Every 15 minutes

monitoring:
  prometheus:
    enabled: true
    port: 8080
    
error_handling:
  max_retries: 3
  backoff_factor: 2
  dead_letter_queue: true
```

## Success Criteria

### Performance Benchmarks
- Process 10,000 records/second for simple schemas
- Handle schema changes within 30 seconds
- Achieve 99.9% uptime in production environments
- Memory usage under 1GB for typical workloads

### Reliability Metrics
- Zero data loss during normal operations
- Recovery from failures within 5 minutes
- Accurate change detection with sub-second latency

### Usability Goals
- One-command deployment to Kubernetes
- Self-service configuration for new schemas
- Comprehensive monitoring dashboards
- Clear error messages and troubleshooting guides

This plan provides a solid foundation for building cartridge-warp as a production-ready CDC streaming platform that meets all your specified requirements while maintaining scalability and reliability.
