"""Schema migration engine for executing schema changes."""

import asyncio
from datetime import datetime
from typing import List, Optional, Dict, Any

import structlog

from ..connectors.base import ColumnDefinition, ColumnType, DestinationConnector
from .config import SchemaEvolutionConfig
from .types import (
    ConversionSafety, 
    EvolutionResult, 
    SchemaChangeType, 
    SchemaEvolutionEvent,
    MigrationRecord,
    ValidationResult,
    MigrationResult
)
from .type_converter import TypeConversionEngine

logger = structlog.get_logger(__name__)


class SchemaMigrationEngine:
    """Engine for executing schema migrations safely."""
    
    def __init__(
        self, 
        config: SchemaEvolutionConfig,
        destination_connector: DestinationConnector,
        type_converter: TypeConversionEngine
    ):
        """Initialize the schema migration engine.
        
        Args:
            config: Schema evolution configuration
            destination_connector: Destination database connector
            type_converter: Type conversion engine
        """
        self.config = config
        self.destination_connector = destination_connector
        self.type_converter = type_converter
        self.logger = logger.bind(component="schema_migrator")
        
        # Track ongoing migrations
        self._active_migrations: Dict[str, asyncio.Task] = {}
        self._migration_history: List[MigrationRecord] = []
        
    async def execute_migrations(
        self, 
        events: List[SchemaEvolutionEvent],
        schema_name: str,
        dry_run: bool = False
    ) -> EvolutionResult:
        """Execute a list of schema evolution events.
        
        Args:
            events: List of schema evolution events to execute
            schema_name: Target schema name
            dry_run: If True, only validate and plan migrations without executing
            
        Returns:
            Result of the evolution operation
        """
        start_time = datetime.now()
        result = EvolutionResult(
            success=False,
            events=events,
            applied_changes=[],
            warnings=[],
            errors=[],
            rollback_commands=[],
            processing_time_seconds=0.0
        )
        
        if not events:
            result.success = True
            result.processing_time_seconds = (datetime.now() - start_time).total_seconds()
            return result
            
        # Check if we can perform these migrations
        validation_result = await self._validate_migrations(events, schema_name)
        if not validation_result["valid"]:
            result.errors.extend(validation_result["errors"])
            result.processing_time_seconds = (datetime.now() - start_time).total_seconds()
            return result
            
        result.warnings.extend(validation_result["warnings"])
        
        # Sort events by priority (safe changes first)
        sorted_events = self._sort_events_by_priority(events)
        
        # Check for approval requirements
        needs_approval = [e for e in sorted_events if e.requires_approval]
        if needs_approval and not dry_run:
            if self.config.require_approval_for_risky_changes:
                self.logger.warning("Migration requires approval", 
                                   events_requiring_approval=len(needs_approval))
                result.errors.append(f"{len(needs_approval)} changes require manual approval")
                result.processing_time_seconds = (datetime.now() - start_time).total_seconds()
                return result
                
        try:
            # Execute migrations
            for event in sorted_events:
                if dry_run:
                    # Generate SQL but don't execute
                    sql_commands = await self._generate_migration_sql(event, schema_name)
                    result.applied_changes.extend([f"DRY RUN: {cmd}" for cmd in sql_commands])
                else:
                    # Execute the migration
                    migration_result = await self._execute_single_migration(event, schema_name)
                    result.applied_changes.extend(migration_result["commands"])
                    result.rollback_commands.extend(migration_result["rollback"])
                    result.warnings.extend(migration_result["warnings"])
                    
                    if migration_result["errors"]:
                        result.errors.extend(migration_result["errors"])
                        # Attempt rollback
                        if self.config.enable_rollback:
                            await self._execute_rollback(result.rollback_commands, schema_name)
                        break
                        
            result.success = len(result.errors) == 0
            
        except Exception as e:
            self.logger.error("Migration execution failed", error=str(e))
            result.errors.append(f"Migration execution failed: {str(e)}")
            
            # Attempt rollback
            if self.config.enable_rollback and not dry_run:
                try:
                    await self._execute_rollback(result.rollback_commands, schema_name)
                    result.warnings.append("Rollback completed successfully")
                except Exception as rollback_error:
                    result.errors.append(f"Rollback failed: {str(rollback_error)}")
                    
        result.processing_time_seconds = (datetime.now() - start_time).total_seconds()
        
        # Log the result
        if result.success:
            self.logger.info("Schema migration completed successfully",
                           schema=schema_name,
                           changes_applied=len(result.applied_changes),
                           processing_time=result.processing_time_seconds)
        else:
            self.logger.error("Schema migration failed",
                            schema=schema_name,
                            errors=len(result.errors),
                            processing_time=result.processing_time_seconds)
                            
        return result
        
    async def _validate_migrations(self, events: List[SchemaEvolutionEvent], schema_name: str) -> ValidationResult:
        """Validate that migrations can be safely executed."""
        result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        # Check strategy constraints
        dangerous_events = [e for e in events if e.safety_level == ConversionSafety.DANGEROUS]
        risky_events = [e for e in events if e.safety_level == ConversionSafety.RISKY]
        
        if self.config.strategy.value == "strict" and (dangerous_events or risky_events):
            result["valid"] = False
            result["errors"].append(f"Strict mode: {len(dangerous_events + risky_events)} unsafe changes blocked")
            
        elif self.config.strategy.value == "conservative" and dangerous_events:
            result["valid"] = False
            result["errors"].append(f"Conservative mode: {len(dangerous_events)} dangerous changes blocked")
            
        # Check for concurrent migration limits
        if len(self._active_migrations) >= self.config.max_concurrent_migrations:
            result["valid"] = False
            result["errors"].append("Maximum concurrent migrations reached")
            
        # Validate type conversions
        for event in events:
            if event.change_type == SchemaChangeType.MODIFY_COLUMN_TYPE:
                if not self._validate_type_conversion(event):
                    result["valid"] = False
                    result["errors"].append(f"Invalid type conversion for {event.column_name}")
                    
        # Convert to proper ValidationResult format
        validation_result: ValidationResult = {
            "valid": result["valid"],
            "warnings": result["warnings"],
            "errors": result["errors"],
            "estimated_duration_seconds": 0.0,  # Could be calculated based on events
            "risk_level": "high" if not result["valid"] else "low"
        }
        
        return validation_result
        
    def _sort_events_by_priority(self, events: List[SchemaEvolutionEvent]) -> List[SchemaEvolutionEvent]:
        """Sort events by execution priority (safe changes first)."""
        
        # Priority order: safe additions, safe modifications, risky changes, dangerous changes
        def priority_key(event: SchemaEvolutionEvent) -> tuple:
            safety_order = {
                ConversionSafety.SAFE: 0,
                ConversionSafety.RISKY: 1, 
                ConversionSafety.DANGEROUS: 2,
                ConversionSafety.INCOMPATIBLE: 3
            }
            
            change_order = {
                SchemaChangeType.ADD_TABLE: 0,
                SchemaChangeType.ADD_COLUMN: 1,
                SchemaChangeType.MODIFY_COLUMN_TYPE: 2,
                SchemaChangeType.RENAME_COLUMN: 3,
                SchemaChangeType.DROP_COLUMN: 4,
                SchemaChangeType.DROP_TABLE: 5
            }
            
            return (
                safety_order.get(event.safety_level, 999),
                change_order.get(event.change_type, 999)
            )
            
        return sorted(events, key=priority_key)
        
    async def _execute_single_migration(self, event: SchemaEvolutionEvent, schema_name: str) -> Dict[str, Any]:
        """Execute a single migration event."""
        result = {
            "commands": [],
            "rollback": [],
            "warnings": [],
            "errors": []
        }
        
        try:
            migration_id = f"{schema_name}_{event.table_name}_{event.change_type.value}_{datetime.now().isoformat()}"
            
            # Generate SQL commands
            sql_commands = await self._generate_migration_sql(event, schema_name)
            rollback_commands = await self._generate_rollback_sql(event, schema_name)
            
            # Execute commands
            for sql_command in sql_commands:
                try:
                    # Execute through destination connector
                    await self._execute_sql_command(sql_command, schema_name)
                    result["commands"].append(sql_command)
                    self.logger.debug("Executed migration command", sql=sql_command)
                    
                except Exception as e:
                    error_msg = f"Failed to execute: {sql_command}. Error: {str(e)}"
                    result["errors"].append(error_msg)
                    self.logger.error("Migration command failed", sql=sql_command, error=str(e))
                    break
                    
            result["rollback"] = rollback_commands
            
            # Record migration in history
            migration_record: MigrationRecord = {
                "id": migration_id,
                "schema_name": schema_name,
                "timestamp": datetime.now().isoformat(),
                "event_type": event.change_type.value,
                "success": len(result["errors"]) == 0,
                "sql_executed": str(result["commands"]),
                "rollback_sql": str(result["rollback"]) if result["rollback"] else None,
                "processing_time_seconds": 0.0  # Could be calculated if needed
            }
            self._migration_history.append(migration_record)
            
        except Exception as e:
            result["errors"].append(f"Migration execution error: {str(e)}")
            
        return result
        
    async def _generate_migration_sql(self, event: SchemaEvolutionEvent, schema_name: str) -> List[str]:
        """Generate SQL commands for a migration event."""
        commands = []
        
        if event.change_type == SchemaChangeType.ADD_TABLE:
            # Generate CREATE TABLE statement
            table_def = event.new_definition
            if not table_def:
                raise ValueError("ADD_TABLE event missing new_definition")
                
            columns_sql = []
            
            for col_def in table_def["columns"]:
                col_sql = f"{col_def['name']} {self._column_type_to_sql(col_def)}"
                if not col_def.get("nullable", True):
                    col_sql += " NOT NULL"
                if col_def.get("default") is not None:
                    col_sql += f" DEFAULT {self._format_default_value(col_def['default'])}"
                columns_sql.append(col_sql)
                
            # Add primary key constraint
            if table_def.get("primary_keys"):
                pk_cols = ", ".join(table_def["primary_keys"])
                columns_sql.append(f"PRIMARY KEY ({pk_cols})")
                
            table_sql = f"CREATE TABLE {schema_name}.{table_def['name']} ({', '.join(columns_sql)})"
            commands.append(table_sql)
            
        elif event.change_type == SchemaChangeType.ADD_COLUMN:
            col_def = event.new_definition
            if not col_def:
                raise ValueError("ADD_COLUMN event missing new_definition")
                
            col_sql = f"ALTER TABLE {schema_name}.{event.table_name} ADD COLUMN {col_def['name']} {self._column_type_to_sql(col_def)}"
            
            if not col_def.get("nullable", True):
                col_sql += " NOT NULL"
            if col_def.get("default") is not None:
                col_sql += f" DEFAULT {self._format_default_value(col_def['default'])}"
                
            commands.append(col_sql)
            
        elif event.change_type == SchemaChangeType.MODIFY_COLUMN_TYPE:
            old_def = event.old_definition
            new_def = event.new_definition
            if not old_def or not new_def:
                raise ValueError("MODIFY_COLUMN_TYPE event missing old_definition or new_definition")
            if not event.column_name:
                raise ValueError("MODIFY_COLUMN_TYPE event missing column_name")
            
            # PostgreSQL ALTER COLUMN TYPE syntax
            alter_sql = (f"ALTER TABLE {schema_name}.{event.table_name} "
                        f"ALTER COLUMN {event.column_name} TYPE {self._column_type_to_sql(new_def)}")
                        
            # Add USING clause for complex conversions
            if self._needs_using_clause(old_def, new_def):
                using_clause = self._generate_using_clause(event.column_name, old_def, new_def)
                alter_sql += f" USING {using_clause}"
                
            commands.append(alter_sql)
            
        elif event.change_type == SchemaChangeType.DROP_COLUMN:
            if not event.column_name:
                raise ValueError("DROP_COLUMN event missing column_name")
            drop_sql = f"ALTER TABLE {schema_name}.{event.table_name} DROP COLUMN {event.column_name}"
            commands.append(drop_sql)
            
        elif event.change_type == SchemaChangeType.DROP_TABLE:
            drop_sql = f"DROP TABLE {schema_name}.{event.table_name}"
            commands.append(drop_sql)
            
        return commands
        
    async def _generate_rollback_sql(self, event: SchemaEvolutionEvent, schema_name: str) -> List[str]:
        """Generate rollback SQL commands for a migration event."""
        commands = []
        
        if event.change_type == SchemaChangeType.ADD_TABLE:
            commands.append(f"DROP TABLE IF EXISTS {schema_name}.{event.table_name}")
            
        elif event.change_type == SchemaChangeType.ADD_COLUMN:
            if not event.column_name:
                raise ValueError("ADD_COLUMN event missing column_name")
            commands.append(f"ALTER TABLE {schema_name}.{event.table_name} DROP COLUMN IF EXISTS {event.column_name}")
            
        elif event.change_type == SchemaChangeType.MODIFY_COLUMN_TYPE:
            if event.old_definition and event.column_name:
                old_def = event.old_definition
                rollback_sql = (f"ALTER TABLE {schema_name}.{event.table_name} "
                              f"ALTER COLUMN {event.column_name} TYPE {self._column_type_to_sql(old_def)}")
                commands.append(rollback_sql)
                
        elif event.change_type == SchemaChangeType.DROP_COLUMN:
            if event.old_definition and event.column_name:
                # Recreate the dropped column
                old_def = event.old_definition
                add_sql = f"ALTER TABLE {schema_name}.{event.table_name} ADD COLUMN {old_def['name']} {self._column_type_to_sql(old_def)}"
                commands.append(add_sql)
                
        return commands
        
    async def _execute_sql_command(self, sql_command: str, schema_name: str) -> None:
        """Execute a SQL command through the destination connector."""
        self.logger.info("Executing SQL command", sql=sql_command, schema=schema_name)
        
        try:
            # Use the destination connector's apply_schema_changes method
            # We need to convert our SQL command to a SchemaChange object
            from ..connectors.base import SchemaChange
            from datetime import datetime
            
            # Create a schema change object representing our migration
            schema_change = SchemaChange(
                schema_name=schema_name,
                table_name="",  # This could be extracted from SQL if needed
                change_type="migration_sql",
                details={"sql_command": sql_command},
                timestamp=datetime.now()
            )
            
            await self.destination_connector.apply_schema_changes(schema_name, [schema_change])
            self.logger.info("SQL command executed successfully", sql=sql_command[:100])
            
        except Exception as e:
            self.logger.error("Failed to execute SQL command", 
                            sql=sql_command[:100], 
                            error=str(e))
            raise
        
    async def _execute_rollback(self, rollback_commands: List[str], schema_name: str) -> None:
        """Execute rollback commands."""
        self.logger.warning("Executing rollback", commands=len(rollback_commands))
        
        for command in reversed(rollback_commands):  # Execute in reverse order
            try:
                await self._execute_sql_command(command, schema_name)
                self.logger.debug("Executed rollback command", sql=command)
            except Exception as e:
                self.logger.error("Rollback command failed", sql=command, error=str(e))
                
    def _column_type_to_sql(self, col_def: dict) -> str:
        """Convert column definition to SQL type string."""
        col_type = col_def["type"]
        
        # Map to PostgreSQL types
        type_mapping = {
            "string": "VARCHAR",
            "integer": "INTEGER", 
            "bigint": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
            "json": "JSONB",
            "binary": "BYTEA"
        }
        
        sql_type = type_mapping.get(col_type, "VARCHAR")
        
        # Add length for VARCHAR
        if sql_type == "VARCHAR" and col_def.get("max_length"):
            sql_type += f"({col_def['max_length']})"
        elif sql_type == "VARCHAR":
            sql_type += "(255)"  # Default length
            
        return sql_type
        
    def _format_default_value(self, default_value: Any) -> str:
        """Format a default value for SQL."""
        if default_value is None:
            return "NULL"
        elif isinstance(default_value, str):
            return f"'{default_value}'"
        elif isinstance(default_value, bool):
            return "TRUE" if default_value else "FALSE"
        else:
            return str(default_value)
            
    def _needs_using_clause(self, old_def: dict, new_def: dict) -> bool:
        """Check if a type conversion needs a USING clause."""
        old_type = old_def["type"]
        new_type = new_def["type"]
        
        # String to numeric conversions need USING
        if old_type == "string" and new_type in ["integer", "bigint", "float", "double"]:
            return True
            
        return False
        
    def _generate_using_clause(self, column_name: str, old_def: dict, new_def: dict) -> str:
        """Generate USING clause for complex type conversions."""
        old_type = old_def["type"]
        new_type = new_def["type"]
        
        if old_type == "string" and new_type in ["integer", "bigint"]:
            return f"{column_name}::INTEGER"
        elif old_type == "string" and new_type in ["float", "double"]:
            return f"{column_name}::DOUBLE PRECISION"
            
        return f"{column_name}::{self._column_type_to_sql(new_def)}"
        
    def _validate_type_conversion(self, event: SchemaEvolutionEvent) -> bool:
        """Validate that a type conversion is possible."""
        if not event.old_definition or not event.new_definition:
            return False
        
        # For column type changes, the definitions contain direct column data
        old_type_str = event.old_definition.get("type", "")
        new_type_str = event.new_definition.get("type", "")
        
        if not old_type_str or not new_type_str:
            return False
        
        try:
            old_type = ColumnType(old_type_str)
            new_type = ColumnType(new_type_str)
            return self.type_converter.can_convert(old_type, new_type)
        except ValueError:
            return False
