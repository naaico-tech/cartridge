# Cartridge-Warp Connection Flow Example

This document provides a comprehensive end-to-end example of how cartridge-warp establishes and maintains a CDC connection from MongoDB to PostgreSQL.

## Complete End-to-End Connection Flow

```mermaid
sequenceDiagram
    participant CLI as CLI Interface
    participant Config as Configuration Manager
    participant Factory as Connector Factory
    participant MongoConn as MongoDB Connector
    participant PGConn as PostgreSQL Connector
    participant MetaMgr as Metadata Manager
    participant SchemaProc as Schema Processor
    participant Metrics as Metrics Exporter
    participant MongoDB as MongoDB Database
    participant PostgreSQL as PostgreSQL Database

    %% Initialization Phase
    rect rgb(230, 245, 255)
        Note over CLI, PostgreSQL: Initialization Phase
        CLI->>Config: Load configuration
        Config->>Config: Parse YAML/JSON
        Config->>Config: Apply environment overrides
        Config->>Config: Validate settings
        Config-->>CLI: Configuration ready
        
        CLI->>Factory: Create source connector
        Factory->>MongoConn: Instantiate MongoDB connector
        CLI->>Factory: Create destination connector
        Factory->>PGConn: Instantiate PostgreSQL connector
        
        CLI->>MongoConn: Test connection
        MongoConn->>MongoDB: Connect and validate
        MongoDB-->>MongoConn: Connection confirmed
        MongoConn-->>CLI: Source ready
        
        CLI->>PGConn: Test connection
        PGConn->>PostgreSQL: Connect and validate
        PostgreSQL-->>PGConn: Connection confirmed
        PGConn-->>CLI: Destination ready
    end

    %% Schema Discovery Phase
    rect rgb(240, 255, 240)
        Note over CLI, PostgreSQL: Schema Discovery Phase
        CLI->>MongoConn: Discover schemas
        MongoConn->>MongoDB: Query databases and collections
        MongoDB-->>MongoConn: Schema information
        MongoConn-->>CLI: Source schemas discovered
        
        CLI->>PGConn: Initialize metadata store
        PGConn->>PostgreSQL: CREATE SCHEMA cartridge_warp_metadata
        PGConn->>PostgreSQL: CREATE TABLE sync_markers
        PGConn->>PostgreSQL: CREATE TABLE schema_registry
        PGConn->>PostgreSQL: CREATE TABLE sync_runs
        PostgreSQL-->>PGConn: Metadata tables created
        
        CLI->>MetaMgr: Initialize metadata manager
        MetaMgr->>PGConn: Query existing markers
        PGConn->>PostgreSQL: SELECT FROM sync_markers
        PostgreSQL-->>PGConn: Current positions
        PGConn-->>MetaMgr: Position data
        MetaMgr-->>CLI: Metadata manager ready
    end

    %% Schema Processing Phase
    rect rgb(255, 248, 240)
        Note over CLI, PostgreSQL: Schema Processing Phase
        CLI->>SchemaProc: Process ecommerce schema
        SchemaProc->>MongoConn: Get schema definition
        MongoConn->>MongoDB: Analyze collections structure
        MongoDB-->>MongoConn: Collection schemas (users, orders, products)
        MongoConn-->>SchemaProc: Schema definitions
        
        SchemaProc->>MetaMgr: Check schema evolution
        MetaMgr->>PGConn: Query schema_registry
        PGConn->>PostgreSQL: SELECT schema versions
        PostgreSQL-->>PGConn: Historical schemas
        PGConn-->>MetaMgr: Schema history
        MetaMgr-->>SchemaProc: Evolution analysis
        
        alt Schema evolution detected
            SchemaProc->>PGConn: Apply schema changes
            PGConn->>PostgreSQL: ALTER TABLE statements
            PostgreSQL-->>PGConn: Schema updated
            SchemaProc->>MetaMgr: Record schema version
            MetaMgr->>PGConn: INSERT INTO schema_registry
            PGConn->>PostgreSQL: Store new version
        else No schema changes
            SchemaProc->>SchemaProc: Use existing schema
        end
        
        SchemaProc->>PGConn: Ensure target schema exists
        PGConn->>PostgreSQL: CREATE SCHEMA IF NOT EXISTS ecommerce
        PGConn->>PostgreSQL: CREATE TABLES (users, orders, products)
        PostgreSQL-->>PGConn: Target schema ready
    end

    %% CDC Streaming Phase
    rect rgb(255, 240, 245)
        Note over CLI, PostgreSQL: CDC Streaming Phase
        CLI->>CLI: Start CDC processing loop
        
        loop Continuous CDC Processing
            CLI->>MongoConn: Get changes since last marker
            MongoConn->>MongoDB: Query change stream (resume token)
            MongoDB-->>MongoConn: Change events batch
            
            alt Changes available
                MongoConn-->>SchemaProc: Change events (INSERT, UPDATE, DELETE)
                SchemaProc->>SchemaProc: Transform data types
                SchemaProc->>SchemaProc: Apply business rules
                SchemaProc->>SchemaProc: Validate constraints
                
                SchemaProc->>PGConn: Write batch to destination
                PGConn->>PostgreSQL: BEGIN TRANSACTION
                PGConn->>PostgreSQL: INSERT/UPDATE/DELETE records
                PGConn->>PostgreSQL: COMMIT TRANSACTION
                PostgreSQL-->>PGConn: Batch written successfully
                
                PGConn->>MetaMgr: Update position marker
                MetaMgr->>PGConn: UPDATE sync_markers
                PGConn->>PostgreSQL: Store new resume token
                PostgreSQL-->>PGConn: Marker updated
                
                CLI->>Metrics: Update processing metrics
                Metrics->>Metrics: Record throughput stats
                Metrics->>Metrics: Update health status
            else No changes
                CLI->>CLI: Wait polling interval (5 seconds)
            end
        end
    end

    %% Error Handling Phase
    rect rgb(255, 235, 235)
        Note over CLI, PostgreSQL: Error Handling (When Errors Occur)
        alt Connection error occurs
            MongoConn->>MongoDB: Connection lost
            MongoDB--X MongoConn: Connection failed
            MongoConn->>CLI: Connection error reported
            CLI->>CLI: Implement exponential backoff
            CLI->>MongoConn: Retry connection
            MongoConn->>MongoDB: Reconnect attempt
            MongoDB-->>MongoConn: Connection restored
        else Data validation error
            SchemaProc->>SchemaProc: Validation failure detected
            SchemaProc->>MetaMgr: Log error details
            MetaMgr->>PGConn: INSERT INTO error_log
            PGConn->>PostgreSQL: Store error context
            SchemaProc->>CLI: Skip invalid record
            CLI->>CLI: Continue with next record
        else Schema conflict error  
            SchemaProc->>PGConn: Schema mismatch detected
            PGConn->>PostgreSQL: Schema validation failed
            PostgreSQL--X PGConn: Constraint violation
            PGConn->>MetaMgr: Log schema conflict
            MetaMgr->>PGConn: Record conflict details
            SchemaProc->>CLI: Trigger schema evolution
            CLI->>SchemaProc: Re-analyze schema
        end
    end

    %% Monitoring Phase
    rect rgb(248, 255, 248)
        Note over CLI, PostgreSQL: Continuous Monitoring
        par Metrics Collection
            CLI->>Metrics: Export Prometheus metrics
            Metrics->>Metrics: Records/sec processed
            Metrics->>Metrics: Bytes/sec transferred  
            Metrics->>Metrics: Connection health status
            Metrics->>Metrics: Error rate statistics
        and Health Checks
            CLI->>MongoConn: Health check
            MongoConn->>MongoDB: Ping database
            MongoDB-->>MongoConn: Health OK
            CLI->>PGConn: Health check
            PGConn->>PostgreSQL: Ping database
            PostgreSQL-->>PGConn: Health OK
        and Status Updates
            CLI->>MetaMgr: Update sync run status
            MetaMgr->>PGConn: UPDATE sync_runs
            PGConn->>PostgreSQL: Record progress stats
        end
    end
```

## Configuration Example for This Flow

```yaml
# MongoDB to PostgreSQL CDC Configuration
mode: "single"
single_schema_name: "ecommerce"

# Source Configuration
source:
  type: "mongodb"
  connection_string: "mongodb://admin:password@mongodb.example.com:27017"
  database: "ecommerce_db"
  change_detection_strategy: "change_stream"
  timezone: "UTC"
  
  # MongoDB-specific settings
  replica_set: "rs0"
  auth_source: "admin"
  ssl_enabled: true
  read_preference: "secondaryPreferred"

# Destination Configuration  
destination:
  type: "postgresql"
  connection_string: "postgresql://user:password@postgres.example.com:5432/warehouse"
  schema: "ecommerce"
  
  # PostgreSQL-specific settings
  pool_size: 10
  max_overflow: 20
  batch_size: 1000
  transaction_timeout: 300

# Schema Configuration
schemas:
  - name: "ecommerce"
    # Table filtering
    table_whitelist: ["users", "orders", "products", "categories"]
    # table_blacklist: ["temp_tables", "audit_logs"]
    
    # Parallelism settings
    max_parallel_streams: 3
    
    # Table-specific configuration
    tables:
      users:
        batch_size: 500
        polling_interval: 10
        full_load_enabled: true
      orders:
        batch_size: 1000
        polling_interval: 5
        full_load_enabled: true
      products:
        batch_size: 2000
        polling_interval: 15
        full_load_enabled: false
        
# Performance Configuration
performance:
  stream_batch_size: 1000
  write_batch_size: 500
  polling_interval: 5
  connection_timeout: 30
  read_timeout: 60

# Monitoring Configuration
monitoring:
  metrics_enabled: true
  metrics_port: 8080
  health_check_enabled: true
  prometheus_endpoint: "/metrics"
  
# Logging Configuration  
logging:
  level: "INFO"
  format: "structured"
  correlation_id_enabled: true
  sensitive_data_masking: true
```

## Key Connection Flow Features

### 1. **Robust Initialization**
- **Multi-layered Configuration**: Environment variables, YAML files, defaults
- **Connection Validation**: Test both source and destination before starting
- **Metadata Bootstrap**: Automatically create required tracking tables

### 2. **Schema Evolution Handling**  
- **Automatic Detection**: Compare current vs. historical schemas
- **Safe Evolution**: Non-destructive schema changes with rollback support
- **Version Tracking**: Complete audit trail of all schema changes

### 3. **Reliable CDC Processing**
- **Position Tracking**: Resume tokens, LSN, timestamps for precise positioning
- **Atomic Updates**: Transaction-safe batch processing with consistent markers
- **Backpressure Handling**: Configurable batch sizes and polling intervals

### 4. **Comprehensive Error Recovery**
- **Connection Resilience**: Automatic reconnection with exponential backoff
- **Data Validation**: Skip invalid records while maintaining processing continuity
- **Schema Conflicts**: Automatic resolution or manual intervention triggers

### 5. **Production Monitoring**
- **Real-time Metrics**: Prometheus-compatible metrics for observability
- **Health Monitoring**: Continuous connection and processing health checks
- **Audit Trail**: Complete processing history with performance statistics

This end-to-end flow ensures reliable, scalable, and maintainable CDC operations between MongoDB and PostgreSQL with comprehensive monitoring and error handling capabilities.
