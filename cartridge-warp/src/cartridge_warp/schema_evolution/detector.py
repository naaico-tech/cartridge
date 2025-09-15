"""Schema change detection engine."""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import structlog

from ..connectors.base import ColumnDefinition, ColumnType, DatabaseSchema, TableSchema
from .config import SchemaEvolutionConfig
from .types import SchemaChangeType, SchemaEvolutionEvent, ConversionSafety

logger = structlog.get_logger(__name__)


class SchemaChangeDetector:
    """Detects changes between source and destination schemas."""
    
    def __init__(self, config: SchemaEvolutionConfig):
        """Initialize the schema change detector.
        
        Args:
            config: Schema evolution configuration
        """
        self.config = config
        self.logger = logger.bind(component="schema_detector")
        
        # Cache for previous schema snapshots
        self._schema_cache: Dict[str, DatabaseSchema] = {}
        self._last_detection_time: Dict[str, datetime] = {}
        
    async def detect_changes(
        self, 
        schema_name: str,
        current_schema: DatabaseSchema,
        previous_schema: Optional[DatabaseSchema] = None
    ) -> List[SchemaEvolutionEvent]:
        """Detect changes between current and previous schema states.
        
        Args:
            schema_name: Name of the schema to check
            current_schema: Current schema definition
            previous_schema: Previous schema definition (if None, uses cached)
            
        Returns:
            List of detected schema evolution events
        """
        if previous_schema is None:
            previous_schema = self._schema_cache.get(schema_name)
            
        if previous_schema is None:
            # First time seeing this schema - cache it and return no changes
            self._schema_cache[schema_name] = current_schema
            self._last_detection_time[schema_name] = datetime.now()
            self.logger.info("Caching initial schema", schema=schema_name, tables=len(current_schema.tables))
            return []
            
        events = []
        
        # Detect table-level changes
        table_events = await self._detect_table_changes(schema_name, current_schema, previous_schema)
        events.extend(table_events)
        
        # Detect column-level changes for existing tables
        column_events = await self._detect_column_changes(schema_name, current_schema, previous_schema)
        events.extend(column_events)
        
        # Update cache
        self._schema_cache[schema_name] = current_schema
        self._last_detection_time[schema_name] = datetime.now()
        
        if events:
            self.logger.info("Schema changes detected", 
                           schema=schema_name, 
                           changes=len(events),
                           change_types=[e.change_type.value for e in events])
        
        return events
        
    async def _detect_table_changes(
        self, 
        schema_name: str,
        current_schema: DatabaseSchema, 
        previous_schema: DatabaseSchema
    ) -> List[SchemaEvolutionEvent]:
        """Detect table additions and removals."""
        events = []
        
        current_tables = {table.name: table for table in current_schema.tables}
        previous_tables = {table.name: table for table in previous_schema.tables}
        
        # Check for new tables
        for table_name in current_tables:
            if table_name not in previous_tables:
                if table_name not in self.config.excluded_tables:
                    events.append(SchemaEvolutionEvent(
                        change_type=SchemaChangeType.ADD_TABLE,
                        schema_name=schema_name,
                        table_name=table_name,
                        new_definition=self._table_to_dict(current_tables[table_name]),
                        safety_level=ConversionSafety.SAFE,
                        estimated_impact=f"New table '{table_name}' with {len(current_tables[table_name].columns)} columns"
                    ))
                    
        # Check for removed tables  
        for table_name in previous_tables:
            if table_name not in current_tables:
                if table_name not in self.config.excluded_tables:
                    events.append(SchemaEvolutionEvent(
                        change_type=SchemaChangeType.DROP_TABLE,
                        schema_name=schema_name,
                        table_name=table_name,
                        old_definition=self._table_to_dict(previous_tables[table_name]),
                        safety_level=ConversionSafety.DANGEROUS,
                        requires_approval=True,
                        estimated_impact=f"Table '{table_name}' with {len(previous_tables[table_name].columns)} columns will be dropped"
                    ))
                    
        return events
        
    async def _detect_column_changes(
        self,
        schema_name: str, 
        current_schema: DatabaseSchema,
        previous_schema: DatabaseSchema
    ) -> List[SchemaEvolutionEvent]:
        """Detect column-level changes within tables."""
        events = []
        
        current_tables = {table.name: table for table in current_schema.tables}
        previous_tables = {table.name: table for table in previous_schema.tables}
        
        # Check changes in existing tables
        for table_name in current_tables:
            if table_name in previous_tables and table_name not in self.config.excluded_tables:
                table_events = await self._detect_table_column_changes(
                    schema_name,
                    table_name,
                    current_tables[table_name],
                    previous_tables[table_name]
                )
                events.extend(table_events)
                
        return events
        
    async def _detect_table_column_changes(
        self,
        schema_name: str,
        table_name: str,
        current_table: TableSchema,
        previous_table: TableSchema
    ) -> List[SchemaEvolutionEvent]:
        """Detect column changes within a specific table."""
        events = []
        
        current_columns = {col.name: col for col in current_table.columns}
        previous_columns = {col.name: col for col in previous_table.columns}
        
        excluded_columns = self.config.excluded_columns.get(table_name, [])
        
        # Check for new columns
        if self.config.detect_column_additions:
            for col_name in current_columns:
                if col_name not in previous_columns and col_name not in excluded_columns:
                    events.append(SchemaEvolutionEvent(
                        change_type=SchemaChangeType.ADD_COLUMN,
                        schema_name=schema_name,
                        table_name=table_name,
                        column_name=col_name,
                        new_definition=self._column_to_dict(current_columns[col_name]),
                        safety_level=ConversionSafety.SAFE,
                        estimated_impact=f"New column '{col_name}' of type {current_columns[col_name].type.value}"
                    ))
                    
        # Check for removed columns
        if self.config.detect_column_removals:
            for col_name in previous_columns:
                if col_name not in current_columns and col_name not in excluded_columns:
                    events.append(SchemaEvolutionEvent(
                        change_type=SchemaChangeType.DROP_COLUMN,
                        schema_name=schema_name,
                        table_name=table_name,
                        column_name=col_name,
                        old_definition=self._column_to_dict(previous_columns[col_name]),
                        safety_level=ConversionSafety.DANGEROUS,
                        requires_approval=True,
                        estimated_impact=f"Column '{col_name}' of type {previous_columns[col_name].type.value} will be dropped"
                    ))
                    
        # Check for type changes in existing columns
        if self.config.detect_type_changes:
            for col_name in current_columns:
                if (col_name in previous_columns and 
                    col_name not in excluded_columns and
                    current_columns[col_name].type != previous_columns[col_name].type):
                    
                    old_col = previous_columns[col_name]
                    new_col = current_columns[col_name]
                    
                    # Determine safety level based on type conversion
                    safety_level = self._assess_type_change_safety(old_col.type, new_col.type)
                    
                    events.append(SchemaEvolutionEvent(
                        change_type=SchemaChangeType.MODIFY_COLUMN_TYPE,
                        schema_name=schema_name,
                        table_name=table_name,
                        column_name=col_name,
                        old_definition=self._column_to_dict(old_col),
                        new_definition=self._column_to_dict(new_col),
                        safety_level=safety_level,
                        requires_approval=safety_level in [ConversionSafety.RISKY, ConversionSafety.DANGEROUS],
                        estimated_impact=f"Column '{col_name}' type change: {old_col.type.value} → {new_col.type.value}"
                    ))
                    
        return events
        
    def _assess_type_change_safety(self, old_type: ColumnType, new_type: ColumnType) -> ConversionSafety:
        """Assess the safety level of a type change."""
        if old_type == new_type:
            return ConversionSafety.SAFE
            
        # Safe widening conversions
        safe_widenings = [
            (ColumnType.INTEGER, ColumnType.BIGINT),
            (ColumnType.FLOAT, ColumnType.DOUBLE),
            (ColumnType.INTEGER, ColumnType.FLOAT),
            (ColumnType.INTEGER, ColumnType.DOUBLE),
        ]
        
        if (old_type, new_type) in safe_widenings:
            return ConversionSafety.SAFE
            
        # Conversions to string are generally safe
        if new_type == ColumnType.STRING:
            return ConversionSafety.SAFE
            
        # Risky narrowing conversions
        risky_narrowings = [
            (ColumnType.BIGINT, ColumnType.INTEGER),
            (ColumnType.DOUBLE, ColumnType.FLOAT),
        ]
        
        if (old_type, new_type) in risky_narrowings:
            return ConversionSafety.RISKY
            
        # String to numeric conversions are dangerous
        if old_type == ColumnType.STRING and new_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.FLOAT, ColumnType.DOUBLE]:
            return ConversionSafety.DANGEROUS
            
        # Default to dangerous for unknown conversions
        return ConversionSafety.DANGEROUS
        
    def _table_to_dict(self, table: TableSchema) -> dict:
        """Convert table schema to dictionary."""
        return {
            "name": table.name,
            "columns": [self._column_to_dict(col) for col in table.columns],
            "primary_keys": table.primary_keys,
            "indexes": table.indexes or []
        }
        
    def _column_to_dict(self, column: ColumnDefinition) -> dict:
        """Convert column definition to dictionary.""" 
        return {
            "name": column.name,
            "type": column.type.value,
            "nullable": column.nullable,
            "default": column.default,
            "max_length": column.max_length,
            "precision": column.precision,
            "scale": column.scale
        }
        
    def get_schema_cache(self, schema_name: str) -> Optional[DatabaseSchema]:
        """Get cached schema for a given schema name."""
        return self._schema_cache.get(schema_name)
        
    def clear_cache(self, schema_name: Optional[str] = None) -> None:
        """Clear schema cache for a specific schema or all schemas."""
        if schema_name:
            self._schema_cache.pop(schema_name, None)
            self._last_detection_time.pop(schema_name, None)
        else:
            self._schema_cache.clear()
            self._last_detection_time.clear()
            
    def get_detection_stats(self) -> Dict[str, dict]:
        """Get statistics about schema detection."""
        stats = {}
        for schema_name in self._schema_cache:
            cache_entry = self._schema_cache[schema_name]
            last_detection = self._last_detection_time.get(schema_name)
            
            stats[schema_name] = {
                "cached_tables": len(cache_entry.tables),
                "last_detection": last_detection.isoformat() if last_detection else None,
                "total_columns": sum(len(table.columns) for table in cache_entry.tables)
            }
            
        return stats
