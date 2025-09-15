# Schema Evolution Engine

The Schema Evolution Engine provides intelligent, automated schema management for cartridge-warp CDC streaming pipelines. It automatically detects schema changes in source databases and safely applies corresponding changes to destination schemas with configurable safety levels and approval workflows.

## Overview

The Schema Evolution Engine consists of four main components:

1. **Type Conversion Engine** - Handles safe type conversions and data transformations
2. **Schema Change Detector** - Monitors and identifies schema changes between source and destination  
3. **Migration Engine** - Executes schema changes with rollback capabilities
4. **Configuration System** - Provides fine-grained control over evolution behavior

## Key Features

### Automatic Schema Change Detection
- **Column changes**: Detects addition, removal, and type modifications
- **Table changes**: Identifies new and dropped tables
- **Constraint changes**: Monitors index and constraint modifications (optional)
- **Real-time monitoring**: Configurable detection intervals (default: 30 seconds)

### Intelligent Type Conversions
- **Safe widening**: Automatic conversions like `int → bigint`, `float → double`
- **Risky narrowing**: Controlled conversions like `bigint → int` with validation
- **String conversions**: Convert any type to string safely
- **Complex object handling**: Automatic JSON/JSONB conversion for objects
- **Fallback strategies**: VARCHAR fallback for unsupported types

### Safety-First Approach
- **Multiple safety levels**: SAFE, RISKY, DANGEROUS, INCOMPATIBLE
- **Data loss prevention**: Configurable maximum acceptable data loss thresholds
- **Approval workflows**: Require manual approval for risky changes
- **Automatic rollback**: Built-in rollback on migration failures
- **Validation**: Pre-migration validation with dry-run capabilities

### Flexible Configuration
- **Strategy modes**: STRICT, CONSERVATIVE, PERMISSIVE, AGGRESSIVE
- **Table-specific overrides**: Different rules per table
- **Exclusion lists**: Skip specific tables or columns
- **Performance tuning**: Concurrent migration limits, timeouts, batch sizes

## Usage

### Basic Configuration

```yaml
schema_evolution:
  enabled: true
  strategy: "conservative"
  detection_interval_seconds: 30
  enable_type_widening: true
  enable_rollback: true
  require_approval_for_risky_changes: true
```

### Advanced Configuration

```yaml
schema_evolution:
  enabled: true
  strategy: "conservative"
  
  # Change detection
  detect_column_additions: true
  detect_column_removals: true  
  detect_type_changes: true
  
  # Safety settings
  max_data_loss_percentage: 0.01
  require_approval_for_dangerous_changes: true
  
  # Table-specific rules
  table_configs:
    critical_table:
      strategy: "strict"
      allow_column_removals: false
      max_data_loss_percentage: 0.0
    analytics_table:
      strategy: "permissive"
      max_data_loss_percentage: 5.0
  
  # Exclusions
  excluded_tables: ["temp_data", "logs"]
  excluded_columns:
    users: ["internal_notes"]
```

### Programmatic Usage

```python
from cartridge_warp.schema_evolution import (
    SchemaEvolutionEngine,
    SchemaEvolutionConfig, 
    EvolutionStrategy
)

# Configure evolution
config = SchemaEvolutionConfig(
    enabled=True,
    strategy=EvolutionStrategy.CONSERVATIVE,
    detection_interval_seconds=60
)

# Initialize engine
engine = SchemaEvolutionEngine(
    config=config,
    source_connector=source_connector,
    destination_connector=destination_connector,
    metadata_manager=metadata_manager
)

# Start monitoring
await engine.start()

# Manual evolution
result = await engine.evolve_schema("my_schema")
if result.success:
    print(f"Applied {len(result.applied_changes)} changes")
else:
    print(f"Evolution failed: {result.errors}")
```

## Evolution Strategies

### STRICT Mode
- Blocks all potentially unsafe changes
- Requires manual intervention for any type conversions
- Best for critical production systems
- Zero tolerance for data loss

### CONSERVATIVE Mode (Recommended)
- Allows safe changes automatically
- Requires approval for risky changes
- Blocks dangerous changes by default
- Balanced approach for most use cases

### PERMISSIVE Mode  
- Applies most changes automatically
- Warns about risky operations
- Still requires approval for dangerous changes
- Good for development environments

### AGGRESSIVE Mode
- Attempts all possible conversions
- Uses fallback values for failed conversions
- Maximum automation with higher risk
- Suitable for non-critical analytics data

## Type Conversion Rules

### Safe Conversions (Automatic)
- `INTEGER` → `BIGINT`
- `FLOAT` → `DOUBLE` 
- Any type → `STRING/VARCHAR`
- Objects → `JSON/JSONB`

### Risky Conversions (Approval Required)
- `BIGINT` → `INTEGER` (with range validation)
- `DOUBLE` → `FLOAT` (precision loss possible)
- `INTEGER` → `BOOLEAN`

### Dangerous Conversions (Approval Required)
- `STRING` → `INTEGER/FLOAT` (parsing required)
- `STRING` → `BOOLEAN` (interpretation needed)
- Complex objects → Primitive types

### Custom Conversion Rules

```python
from cartridge_warp.schema_evolution import ConversionRule, ConversionSafety

# Add custom conversion rule
custom_rule = ConversionRule(
    source_type=ColumnType.STRING,
    target_type=ColumnType.DATE,
    safety=ConversionSafety.RISKY,
    conversion_function=lambda x: datetime.strptime(x, "%Y-%m-%d"),
    validation_function=lambda x: bool(re.match(r'\d{4}-\d{2}-\d{2}', x)),
    requires_approval=True
)

engine.type_converter.add_rule(custom_rule)
```

## Monitoring and Observability

### Health Checks
```python
health = await engine.health_check()
print(f"Engine running: {health['running']}")
print(f"Strategy: {health['strategy']}")
print(f"Changes detected: {health['metrics']['total_changes_detected']}")
```

### Metrics Tracking
- Total changes detected and applied
- Success/failure rates by change type
- Processing times and performance metrics
- Safety level distributions
- Rollback frequency

### Logging
All schema evolution events are logged with structured context:
- Change detection events
- Migration execution steps  
- Approval requirements
- Rollback operations
- Error conditions

## Best Practices

### Production Deployment
1. Start with CONSERVATIVE strategy
2. Configure approval workflows for critical tables
3. Set low data loss thresholds (< 0.1%)
4. Enable comprehensive logging
5. Monitor metrics and health checks
6. Test rollback procedures

### Development Environment
1. Use PERMISSIVE strategy for faster iteration
2. Higher data loss tolerance acceptable
3. Disable approval requirements
4. Enable all change detection types
5. Use shorter detection intervals

### Critical Systems
1. Use STRICT strategy
2. Zero data loss tolerance
3. Require approval for all changes
4. Exclude critical tables if needed
5. Manual validation steps
6. Comprehensive backup strategies

## Troubleshooting

### Common Issues

**Evolution engine not starting**
- Check `enabled: true` in configuration
- Verify connector permissions
- Review metadata manager connectivity

**Changes not detected**
- Confirm detection interval settings
- Check excluded tables/columns lists
- Verify source schema access

**Migrations failing**
- Review destination connector permissions  
- Check SQL syntax in generated migrations
- Verify type conversion compatibility
- Enable rollback for automatic recovery

**Approval required unexpectedly**
- Review safety level assessments
- Check table-specific configuration overrides
- Adjust strategy mode if appropriate

### Debug Mode
Enable detailed logging for troubleshooting:

```python
import logging
logging.getLogger('cartridge_warp.schema_evolution').setLevel(logging.DEBUG)
```

## Migration and Rollback

### Automatic Rollback
When enabled, the engine automatically rolls back failed migrations:
- Tracks all applied changes
- Generates rollback SQL commands
- Executes rollback in reverse order
- Logs rollback operations

### Manual Rollback
```python
# Get rollback commands for a failed evolution
result = await engine.evolve_schema("schema_name", dry_run=True)
rollback_commands = result.rollback_commands

# Execute manual rollback if needed
for command in reversed(rollback_commands):
    await destination_connector.execute_sql(command)
```

### Migration History
All migrations are tracked in the metadata system:
- Migration timestamps
- Applied changes
- Rollback commands  
- Success/failure status
- Performance metrics

## Integration with Cartridge-Warp

The Schema Evolution Engine integrates seamlessly with cartridge-warp:

1. **Schema Processor Integration**: Automatically checks for schema changes during startup
2. **Configuration**: Added to schema-level configuration in cartridge-warp config
3. **Metrics**: Evolution metrics included in overall cartridge-warp monitoring
4. **Lifecycle**: Started and stopped with schema processors

This provides transparent schema evolution without requiring changes to existing cartridge-warp workflows.
