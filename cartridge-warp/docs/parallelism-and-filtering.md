# Table Stream Load Parallelism and Filtering Configuration Guide

This document describes the new parallelism and table filtering features added to cartridge-warp.

## Table of Contents

1. [Parallelism Configuration](#parallelism-configuration)
2. [Table Filtering](#table-filtering)
3. [Environment Variable Overrides](#environment-variable-overrides)
4. [Configuration Examples](#configuration-examples)
5. [Best Practices](#best-practices)

## Parallelism Configuration

Cartridge-warp now supports configurable parallelism for table stream processing at three levels:

### Hierarchy (highest to lowest precedence)

1. **Table Level**: `max_parallel_streams` in table configuration
2. **Schema Level**: `default_max_parallel_streams` in schema configuration  
3. **Global Level**: `global_max_parallel_streams` in root configuration

### Configuration Options

#### Global Level
```yaml
# Global default for all tables across all schemas
global_max_parallel_streams: 2
```

#### Schema Level
```yaml
schemas:
  - name: "ecommerce"
    # Default for all tables in this schema
    default_max_parallel_streams: 3
```

#### Table Level
```yaml
tables:
  - name: "orders"
    # Specific setting for this table only
    max_parallel_streams: 5
```

### Environment Variable Override
```bash
export CARTRIDGE_WARP_GLOBAL_MAX_PARALLEL_STREAMS=4
```

## Table Filtering

Control which tables are processed using whitelist and blacklist configurations at global and schema levels.

### Filtering Rules

1. **Whitelist takes precedence**: If a whitelist is defined, only tables in the whitelist are processed
2. **Blacklist exclusion**: If no whitelist but blacklist exists, exclude tables in the blacklist
3. **Global + Schema filtering**: Both global and schema-level filters are applied (AND logic)

### Configuration Options

#### Global Filtering
```yaml
# Process only these tables across all schemas
global_table_whitelist: ["users", "orders", "products"]

# OR exclude these tables globally
global_table_blacklist: ["temp_tables", "debug_logs"]
```

#### Schema-Level Filtering
```yaml
schemas:
  - name: "ecommerce"
    # Further filter tables within this schema
    table_whitelist: ["users", "orders"]  # Subset of global whitelist
    
    # OR exclude specific tables in this schema
    table_blacklist: ["audit_logs"]
```

### Environment Variable Override
```bash
# Comma-separated lists
export CARTRIDGE_WARP_GLOBAL_TABLE_WHITELIST="users,orders,products"
export CARTRIDGE_WARP_GLOBAL_TABLE_BLACKLIST="temp_tables,logs"
```

## Environment Variable Overrides

All configuration can be overridden using environment variables with the `CARTRIDGE_WARP_` prefix.

### Common Overrides

```bash
# Connection strings (most common in production)
export CARTRIDGE_WARP_SOURCE__CONNECTION_STRING="mongodb+srv://user:pass@cluster.mongodb.net/prod"
export CARTRIDGE_WARP_DESTINATION__CONNECTION_STRING="postgresql://user:pass@prod-db:5432/warehouse"

# Parallelism settings
export CARTRIDGE_WARP_GLOBAL_MAX_PARALLEL_STREAMS=4

# Table filtering (comma-separated)
export CARTRIDGE_WARP_GLOBAL_TABLE_WHITELIST="users,orders,products"
export CARTRIDGE_WARP_GLOBAL_TABLE_BLACKLIST="temp_tables,logs"

# Monitoring and runtime
export CARTRIDGE_WARP_MONITORING__LOG_LEVEL=DEBUG
export CARTRIDGE_WARP_DRY_RUN=true
```

### Nested Configuration
Use double underscores (`__`) to access nested configuration:
```bash
export CARTRIDGE_WARP_MONITORING__PROMETHEUS__PORT=9090
export CARTRIDGE_WARP_ERROR_HANDLING__MAX_RETRIES=5
```

## Configuration Examples

### Basic Parallelism Configuration
```yaml
mode: "single"
single_schema_name: "ecommerce"

# Global default
global_max_parallel_streams: 2

schemas:
  - name: "ecommerce"
    # Schema override
    default_max_parallel_streams: 3
    
    tables:
      - name: "users"
        max_parallel_streams: 1  # Conservative for user data
        
      - name: "orders"
        max_parallel_streams: 5  # Aggressive for high-volume data
        
      - name: "products"
        # Uses schema default (3)
```

### Table Filtering Configuration
```yaml
# Global whitelist - only process these tables
global_table_whitelist: ["users", "orders", "products", "inventory"]

schemas:
  - name: "tenant_1"
    # Further restrict to subset of global whitelist
    table_whitelist: ["users", "orders"]
    
  - name: "tenant_2"
    # Exclude inventory despite being in global whitelist
    table_blacklist: ["inventory"]
```

### Production Environment Configuration
```yaml
# Base configuration file (config.yaml)
mode: "single"
single_schema_name: "production"

global_max_parallel_streams: 1  # Conservative default

source:
  type: "mongodb"
  connection_string: "mongodb://localhost:27017"  # Overridden by env var
  
destination:
  type: "postgresql" 
  connection_string: "postgresql://localhost:5432/warehouse"  # Overridden by env var

schemas:
  - name: "production"
    default_max_parallel_streams: 2
    
    tables:
      - name: "critical_table"
        max_parallel_streams: 1  # Single stream for critical data
```

```bash
# Production environment variables (.env)
CARTRIDGE_WARP_SOURCE__CONNECTION_STRING=mongodb+srv://prod-user:password@prod-cluster.mongodb.net/production
CARTRIDGE_WARP_DESTINATION__CONNECTION_STRING=postgresql://warehouse-user:password@warehouse.company.com:5432/data_warehouse
CARTRIDGE_WARP_GLOBAL_MAX_PARALLEL_STREAMS=3
CARTRIDGE_WARP_GLOBAL_TABLE_BLACKLIST=temp_tables,debug_logs,test_data
CARTRIDGE_WARP_MONITORING__LOG_LEVEL=INFO
```

## Best Practices

### Parallelism Guidelines

1. **Start Conservative**: Begin with low parallelism (1-2 streams) and increase based on performance testing
2. **Consider Data Consistency**: Use single stream (`max_parallel_streams: 1`) for tables requiring strict ordering
3. **High-Volume Tables**: Use higher parallelism (4-8 streams) for high-throughput tables like events or logs
4. **Monitor Resources**: Watch CPU, memory, and database connection usage when increasing parallelism
5. **Database Limits**: Consider destination database connection limits and concurrent write capabilities

### Table Filtering Guidelines

1. **Use Whitelist for Production**: Explicitly define which tables to process rather than excluding with blacklist
2. **Environment-Specific Filtering**: Use environment variables to adjust table filtering per environment
3. **Test Exclusions**: Verify that excluded tables don't break referential integrity in your warehouse
4. **Documentation**: Document why specific tables are included/excluded for future reference

### Environment Variable Guidelines

1. **Connection Strings**: Always override connection strings in production via environment variables
2. **Sensitive Data**: Use environment variables for passwords, API keys, and sensitive configuration
3. **Environment-Specific Settings**: Use different `.env` files for development, staging, and production
4. **Validation**: Test configuration with `--dry-run` flag after changing environment variables

### Performance Tuning

1. **Monitor Metrics**: Use Prometheus metrics to track processing rates and resource usage
2. **Batch Size Tuning**: Adjust `stream_batch_size` alongside parallelism for optimal performance
3. **Database Performance**: Monitor destination database performance and adjust parallelism accordingly
4. **Network Considerations**: Consider network bandwidth between source and destination when setting parallelism

### Error Handling

1. **Dead Letter Queue**: Enable dead letter queue for parallel processing to handle errors gracefully
2. **Retry Configuration**: Increase retry limits when using higher parallelism due to potential contention
3. **Monitoring Alerts**: Set up alerts for failed parallel streams and error rates
4. **Rollback Strategy**: Have a plan to quickly reduce parallelism if issues arise

## Migration from Previous Versions

### Backward Compatibility

The new configuration is fully backward compatible. Existing configurations will:
- Default to `global_max_parallel_streams: 1` (single stream, same as before)
- Process all tables (no filtering applied)
- Work with existing environment variable overrides

### Upgrading Existing Configurations

1. **Add Parallelism**: Start by adding `global_max_parallel_streams: 2` to test
2. **Add Table Filtering**: Use `global_table_blacklist` to exclude test/temp tables
3. **Fine-tune Per Table**: Add table-specific `max_parallel_streams` for high-volume tables
4. **Monitor and Adjust**: Use metrics to guide further parallelism increases
