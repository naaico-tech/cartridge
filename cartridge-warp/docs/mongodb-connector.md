# MongoDB Source Connector

The MongoDB source connector enables real-time and batch change data capture (CDC) from MongoDB databases to destination systems.

## Features

### Change Stream Support (Real-time CDC)
- ✅ MongoDB change stream integration using motor/pymongo
- ✅ Resume token management for fault tolerance
- ✅ Change event parsing and normalization
- ✅ Support for insert, update, delete, and replace operations
- ✅ Full document vs delta change handling
- ✅ Collection and database-level watch capabilities

### Batch Mode Support
- ✅ Timestamp-based incremental loading
- ✅ Configurable change detection column (default: updated_at)
- ✅ Query optimization with proper indexing
- ✅ Pagination for large datasets
- ✅ Support for custom query filters

### Schema Introspection
- ✅ Dynamic schema discovery from MongoDB collections
- ✅ BSON to SQL type mapping
- ✅ Nested document flattening strategies
- ✅ Array handling with JSONB conversion
- ✅ Schema evolution detection and tracking

### Connection Management
- ✅ Connection pooling and retry logic
- ✅ Authentication support (SCRAM, X.509, LDAP)
- ✅ SSL/TLS connection support
- ✅ Replica set and sharded cluster support
- ✅ Read preference configuration

### Error Handling
- ✅ Network failure recovery
- ✅ Invalid BSON handling
- ✅ Large document processing
- ✅ Connection timeout management
- ✅ Dead letter queue for failed records

## Configuration

### Basic Configuration

```yaml
source:
  type: mongodb
  connection_string: "mongodb://localhost:27017"
  database: "ecommerce"
  change_detection_column: "updated_at"
  change_detection_strategy: "timestamp"  # or "log" for change streams
  timezone: "UTC"
```

### Advanced Configuration

```yaml
source:
  type: mongodb
  connection_string: "mongodb://user:password@localhost:27017/admin?authSource=admin"
  database: "ecommerce"
  
  # Change detection settings
  change_detection_column: "updated_at"
  change_detection_strategy: "log"  # Use change streams
  timezone: "UTC"
  
  # MongoDB-specific options
  max_document_depth: 3
  use_change_streams: true
  full_document: "updateLookup"  # or "default"
  resume_token: null  # Auto-managed
```

### Connection String Examples

#### Basic Connection
```
mongodb://localhost:27017
```

#### Authenticated Connection
```
mongodb://username:password@localhost:27017/database?authSource=admin
```

#### Replica Set
```
mongodb://user:pass@host1:27017,host2:27017,host3:27017/database?replicaSet=myrs
```

#### SSL/TLS Connection
```
mongodb://user:pass@host:27017/database?ssl=true&authSource=admin
```

## Data Type Mapping

| MongoDB Type | SQL Type | Notes |
|--------------|----------|-------|
| ObjectId | STRING | Converted to string representation |
| String | STRING | Direct mapping |
| Int32 | INTEGER | 32-bit integers |
| Int64 | BIGINT | 64-bit integers |
| Double | DOUBLE | Floating point numbers |
| Boolean | BOOLEAN | Direct mapping |
| Date | TIMESTAMP | MongoDB dates to timestamps |
| Timestamp | TIMESTAMP | MongoDB timestamps |
| Array | JSON | Serialized as JSON string |
| Object | Flattened | Nested objects flattened with underscores |
| Binary | BINARY | Binary data |
| Null | NULL | Null values |

## Document Flattening

Nested MongoDB documents are flattened for relational storage:

### Input Document
```json
{
  "_id": "507f1f77bcf86cd799439011",
  "name": "John Doe",
  "address": {
    "street": "123 Main St",
    "city": "Anytown",
    "coordinates": {
      "lat": 40.7128,
      "lng": -74.0060
    }
  },
  "tags": ["user", "premium"]
}
```

### Flattened Output
```json
{
  "_id": "507f1f77bcf86cd799439011",
  "name": "John Doe",
  "address_street": "123 Main St",
  "address_city": "Anytown",
  "address_coordinates": "{\"lat\": 40.7128, \"lng\": -74.0060}",
  "tags": "[\"user\", \"premium\"]",
  "_created_at": "2023-01-01T00:00:00Z",
  "_updated_at": "2023-01-01T00:00:00Z"
}
```

## Change Detection Strategies

### Timestamp-based (Batch Mode)
- Uses a timestamp field (default: `updated_at`) to detect changes
- Suitable for batch processing and initial loads
- Requires documents to have a timestamp field
- Good for scenarios where change streams are not available

```yaml
source:
  change_detection_strategy: "timestamp"
  change_detection_column: "last_modified"
```

### Change Streams (Real-time CDC)
- Uses MongoDB change streams for real-time change detection
- Provides true CDC capabilities
- Requires MongoDB 3.6+ and replica set or sharded cluster
- Automatically handles resume tokens for fault tolerance

```yaml
source:
  change_detection_strategy: "log"
  use_change_streams: true
  full_document: "updateLookup"
```

## Metadata Fields

The connector automatically adds metadata fields to each record:

- `_id`: MongoDB document ID (string)
- `_created_at`: Record creation timestamp
- `_updated_at`: Record last update timestamp

## Performance Considerations

### Indexing
Ensure proper indexing on timestamp fields used for change detection:

```javascript
db.collection.createIndex({ "updated_at": 1 })
```

### Batch Sizes
Configure appropriate batch sizes based on document size and network capacity:

```yaml
tables:
  - name: "large_collection"
    stream_batch_size: 100
    full_load_batch_size: 1000
```

### Connection Pooling
The connector uses connection pooling by default:
- Max pool size: 10 connections
- Min pool size: 1 connection
- Server selection timeout: 5 seconds

## Error Handling

### Connection Failures
- Automatic reconnection with exponential backoff
- Configurable retry attempts and timeouts
- Graceful handling of network partitions

### Document Processing Errors
- Invalid BSON data is logged and skipped
- Large documents are handled with streaming
- Schema validation errors are reported but don't stop processing

### Change Stream Errors
- Resume token management for restart capability
- Automatic recovery from temporary failures
- Dead letter queue for problematic events

## Examples

### Complete Configuration Example

```yaml
# MongoDB to PostgreSQL - Single Schema Mode
mode: single

source:
  type: mongodb
  connection_string: "mongodb://localhost:27017"
  database: "ecommerce"
  change_detection_column: "updated_at"
  change_detection_strategy: "log"
  timezone: "UTC"
  max_document_depth: 3
  use_change_streams: true

destination:
  type: postgresql
  connection_string: "postgresql://user:password@localhost:5432/warehouse"
  database: "warehouse"

schemas:
  - name: "ecommerce"
    mode: "stream"
    tables:
      - name: "products"
        mode: "stream"
        stream_batch_size: 500
        enable_schema_evolution: true
      - name: "orders"
        mode: "stream"
        stream_batch_size: 1000
        enable_schema_evolution: true

single_schema_name: "ecommerce"
```

## Testing

### Unit Tests
```bash
python -m pytest tests/unit/connectors/test_mongodb_source.py -v
```

### Integration Tests
Requires a running MongoDB instance:

```bash
# Start MongoDB
docker run -d -p 27017:27017 --name mongo-test mongo:5.0

# Run integration tests
INTEGRATION_TESTS=1 python -m pytest tests/integration/test_mongodb_integration.py -v

# Cleanup
docker stop mongo-test && docker rm mongo-test
```

## Limitations

1. **Change Streams Requirements**: Real-time CDC requires MongoDB 3.6+ with replica set or sharded cluster
2. **Document Size**: Very large documents (>16MB) may cause performance issues
3. **Schema Evolution**: Complex schema changes may require manual intervention
4. **Nested Depth**: Documents with extreme nesting depth may hit flattening limits
5. **Array Processing**: Large arrays are serialized as JSON, which may impact query performance

## Troubleshooting

### Common Issues

#### Connection Refused
```
pymongo.errors.ServerSelectionTimeoutError: localhost:27017: [Errno 61] Connection refused
```
- Ensure MongoDB is running
- Check connection string and port
- Verify network connectivity

#### Authentication Failed
```
pymongo.errors.OperationFailure: Authentication failed
```
- Verify username and password
- Check authentication database in connection string
- Ensure user has proper permissions

#### Change Stream Not Available
```
pymongo.errors.OperationFailure: The $changeStream stage is only supported on replica sets
```
- Change streams require replica set or sharded cluster
- Convert standalone instance to replica set
- Use timestamp-based strategy instead

### Debugging

Enable debug logging:

```yaml
monitoring:
  log_level: "DEBUG"
  structured_logging: true
```

Check MongoDB logs:
```bash
# Docker
docker logs mongo-container

# MongoDB log file
tail -f /var/log/mongodb/mongod.log
```
