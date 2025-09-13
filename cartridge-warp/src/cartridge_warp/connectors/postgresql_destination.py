"""PostgreSQL destination connector for cartridge-warp.

Implements comprehensive PostgreSQL destination functionality including:
- UPSERT operations with ON CONFLICT handling
- Schema evolution and automatic table creation
- JSONB support for complex nested objects
- Connection pooling and performance optimization
- Soft/hard delete support
- Type mapping from source systems
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Union, Tuple

import asyncpg
import structlog
from asyncpg import Connection, Pool
from asyncpg.exceptions import (
    ConnectionDoesNotExistError,
    PostgresError,
    UniqueViolationError,
)
from dateutil.parser import isoparse
from bson import ObjectId, Timestamp

from .base import (
    BaseDestinationConnector,
    ColumnDefinition,
    ColumnType,
    OperationType,
    Record,
    SchemaChange,
    TableSchema,
)
from .factory import register_destination_connector

logger = structlog.get_logger(__name__)


class MongoDBJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for MongoDB types."""
    
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Timestamp):
            return obj.as_datetime().isoformat()
        return super().default(obj)


def safe_json_dumps(obj):
    """JSON dumps with MongoDB type support."""
    return json.dumps(obj, cls=MongoDBJSONEncoder)


class PostgreSQLTypeMapper:
    """Maps source database types to PostgreSQL types."""

    TYPE_MAPPING = {
        ColumnType.STRING: "TEXT",
        ColumnType.INTEGER: "INTEGER",
        ColumnType.BIGINT: "BIGINT",
        ColumnType.FLOAT: "REAL",
        ColumnType.DOUBLE: "DOUBLE PRECISION",
        ColumnType.BOOLEAN: "BOOLEAN",
        ColumnType.TIMESTAMP: "TIMESTAMP WITH TIME ZONE",
        ColumnType.DATE: "DATE",
        ColumnType.JSON: "JSONB",
        ColumnType.BINARY: "BYTEA",
    }

    @classmethod
    def get_postgresql_type(cls, column_type: ColumnType, max_length: Optional[int] = None) -> str:
        """Get PostgreSQL type for a given column type.
        
        Args:
            column_type: Source column type
            max_length: Maximum length for string types
            
        Returns:
            PostgreSQL type string
        """
        pg_type = cls.TYPE_MAPPING.get(column_type, "JSONB")
        
        # Handle variable length types
        if column_type == ColumnType.STRING and max_length:
            if max_length <= 255:
                return f"VARCHAR({max_length})"
            elif max_length <= 65535:
                return "TEXT"
        
        return pg_type

    @classmethod
    def convert_value(cls, value: Any, target_type: ColumnType) -> Any:
        """Convert a value to be compatible with PostgreSQL.
        
        Args:
            value: Source value
            target_type: Target column type
            
        Returns:
            Converted value suitable for PostgreSQL
        """
        if value is None:
            return None
            
        if target_type == ColumnType.JSON:
            if isinstance(value, (dict, list)):
                return safe_json_dumps(value)
            elif isinstance(value, str):
                # Assume it's already JSON string
                return value
            else:
                # Convert other types to JSON
                return safe_json_dumps(value)
                
        elif target_type == ColumnType.TIMESTAMP:
            if isinstance(value, str):
                # Use dateutil for robust ISO 8601 parsing
                try:
                    return isoparse(value)
                except ValueError:
                    logger.warning("Failed to parse timestamp", value=value)
                    return value
            elif isinstance(value, datetime):
                # Ensure timezone awareness
                if value.tzinfo is None:
                    return value.replace(tzinfo=timezone.utc)
                return value
                
        elif target_type in (ColumnType.INTEGER, ColumnType.BIGINT):
            if isinstance(value, str):
                try:
                    return int(value)
                except ValueError:
                    logger.warning("Failed to convert to int", value=value)
                    return value
            elif isinstance(value, (int, float)):
                return int(value)
                
        elif target_type in (ColumnType.FLOAT, ColumnType.DOUBLE):
            if isinstance(value, str):
                try:
                    return float(value)
                except ValueError:
                    logger.warning("Failed to convert to float", value=value)
                    return None
            elif isinstance(value, (int, float)):
                return float(value)
                
        elif target_type == ColumnType.BOOLEAN:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            elif isinstance(value, (int, float)):
                return bool(value)
            elif isinstance(value, bool):
                return value
        
        return value


@register_destination_connector("postgresql")
class PostgreSQLDestinationConnector(BaseDestinationConnector):
    """PostgreSQL destination connector with advanced features.
    
    Features:
    - UPSERT operations with conflict resolution
    - Automatic schema and table creation
    - Schema evolution with column addition
    - JSONB support for complex objects
    - Connection pooling for performance
    - Configurable batch processing
    - Soft/hard delete support
    - Transaction management with rollback
    """

    def __init__(
        self,
        connection_string: str,
        metadata_schema: str = "cartridge_warp_metadata",
        batch_size: int = 1000,
        max_connections: int = 10,
        min_connections: int = 2,
        connection_timeout: float = 30.0,
        command_timeout: float = 60.0,
        enable_soft_deletes: bool = True,
        deletion_strategy: str = "soft",  # "soft", "hard", "both"
        upsert_mode: str = "on_conflict",  # "on_conflict", "merge"
        max_retries: int = 3,
        soft_delete_flag_column: str = "is_deleted",
        soft_delete_timestamp_column: str = "deleted_at",
        safe_type_conversions: Optional[Set[Tuple[str, str]]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize PostgreSQL destination connector.
        
        Args:
            connection_string: PostgreSQL connection string
            metadata_schema: Schema for metadata tables
            batch_size: Records per batch for bulk operations
            max_connections: Maximum connections in pool
            min_connections: Minimum connections in pool
            connection_timeout: Connection timeout in seconds
            command_timeout: Command timeout in seconds
            enable_soft_deletes: Enable soft delete functionality
            deletion_strategy: How to handle deletes ("soft", "hard", "both")
            upsert_mode: UPSERT strategy to use
            max_retries: Maximum retry attempts for failed operations
            soft_delete_flag_column: Column name for soft delete flag
            soft_delete_timestamp_column: Column name for soft delete timestamp
            safe_type_conversions: Set of safe type conversion tuples
            **kwargs: Additional configuration options
        """
        super().__init__(connection_string, metadata_schema, **kwargs)
        
        # Validate batch_size parameter
        if not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size!r}")
        
        self.batch_size = batch_size
        self.max_connections = max_connections
        self.min_connections = min_connections
        self.connection_timeout = connection_timeout
        self.command_timeout = command_timeout
        self.enable_soft_deletes = enable_soft_deletes
        self.deletion_strategy = deletion_strategy
        self.upsert_mode = upsert_mode
        self.max_retries = max_retries
        self.soft_delete_flag_column = soft_delete_flag_column
        self.soft_delete_timestamp_column = soft_delete_timestamp_column
        
        # Configure safe type conversions
        default_safe_conversions = {
            ("integer", "bigint"),
            ("float", "double"),
            ("string", "json"),
        }
        self.safe_type_conversions = safe_type_conversions or default_safe_conversions
        
        self.pool: Optional[Pool] = None
        self.type_mapper = PostgreSQLTypeMapper()
        
        # Track created schemas and tables for performance
        self._created_schemas: Set[str] = set()
        self._created_tables: Set[str] = set()
        self._table_schemas: Dict[str, TableSchema] = {}

    async def connect(self) -> None:
        """Establish connection pool to PostgreSQL."""
        if self.connected:
            return
            
        try:
            logger.info(
                "Establishing PostgreSQL connection pool",
                min_connections=self.min_connections,
                max_connections=self.max_connections,
            )
            
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=self.min_connections,
                max_size=self.max_connections,
                timeout=self.connection_timeout,
                command_timeout=self.command_timeout,
            )
            
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            
            self.connected = True
            logger.info("PostgreSQL connection pool established successfully")
            
        except Exception as e:
            logger.error("Failed to establish PostgreSQL connection", error=str(e))
            raise

    async def disconnect(self) -> None:
        """Close connection pool."""
        if not self.connected or not self.pool:
            return
            
        try:
            logger.info("Closing PostgreSQL connection pool")
            await self.pool.close()
            self.pool = None
            self.connected = False
            logger.info("PostgreSQL connection pool closed")
            
        except Exception as e:
            logger.error("Error closing PostgreSQL connection pool", error=str(e))

    async def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        if not self.connected or not self.pool:
            return False
            
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error("PostgreSQL connection test failed", error=str(e))
            return False

    def acquire(self):
        """Acquire a connection from the pool.
        
        Returns:
            Async context manager for connection acquisition
        """
        if not self.pool:
            raise RuntimeError("Connection pool not initialized. Call connect() first.")
        return self.pool.acquire()
    
    @property
    def connection_pool(self) -> Optional[Pool]:
        """Get the connection pool.
        
        Returns:
            The asyncpg connection pool or None if not connected
        """
        return self.pool

    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        """Create schema if it doesn't exist."""
        if schema_name in self._created_schemas:
            return
            
        try:
            async with self.pool.acquire() as conn:  # type: ignore[union-attr]
                # Use quoted identifier to handle special characters safely
                # asyncpg properly handles quoted identifiers for SQL injection protection
                query = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'
                await conn.execute(query)
                
                self._created_schemas.add(schema_name)
                logger.debug("Schema created or verified", schema=schema_name)
                
        except Exception as e:
            logger.error("Failed to create schema", schema=schema_name, error=str(e))
            raise

    async def create_table_if_not_exists(
        self, schema_name: str, table_schema: TableSchema
    ) -> None:
        """Create table if it doesn't exist."""
        table_key = f"{schema_name}.{table_schema.name}"
        
        if table_key in self._created_tables:
            return
            
        try:
            # Ensure schema exists first
            await self.create_schema_if_not_exists(schema_name)
            
            # Build CREATE TABLE statement
            columns = []
            for col in table_schema.columns:
                pg_type = self.type_mapper.get_postgresql_type(col.type, col.max_length)
                nullable = "NULL" if col.nullable else "NOT NULL"
                
                column_def = f'"{col.name}" {pg_type} {nullable}'
                
                if col.default is not None:
                    if col.type == ColumnType.STRING:
                        column_def += f" DEFAULT '{col.default}'"
                    else:
                        column_def += f" DEFAULT {col.default}"
                        
                columns.append(column_def)
            
            # Add soft delete column if enabled
            if self.enable_soft_deletes:
                columns.append(f'"{self.soft_delete_flag_column}" BOOLEAN DEFAULT FALSE')
                columns.append(f'"{self.soft_delete_timestamp_column}" TIMESTAMP WITH TIME ZONE')
            
            # Add metadata columns
            columns.extend([
                '"_cartridge_created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW()',
                '"_cartridge_updated_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW()',
                '"_cartridge_version" INTEGER DEFAULT 1',
            ])
            
            columns_sql = ",\n    ".join(columns)
            
            # Add primary key constraint if specified
            if table_schema.primary_keys:
                pk_columns = ", ".join(f'"{pk}"' for pk in table_schema.primary_keys)
                columns_sql += f",\n    PRIMARY KEY ({pk_columns})"
            
            query = f'''
                CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_schema.name}" (
                    {columns_sql}
                )
            '''
            
            async with self.pool.acquire() as conn:  # type: ignore[union-attr]
                await conn.execute(query)
                
                # Create indexes if specified
                if table_schema.indexes:
                    await self._create_indexes(conn, schema_name, table_schema)
                
                # Create performance indexes for soft deletes
                if self.enable_soft_deletes:
                    await self._create_soft_delete_indexes(conn, schema_name, table_schema)
                
                self._created_tables.add(table_key)
                self._table_schemas[table_key] = table_schema
                
                logger.info(
                    "Table created or verified",
                    schema=schema_name,
                    table=table_schema.name,
                    columns=len(table_schema.columns),
                )
                
        except Exception as e:
            logger.error(
                "Failed to create table",
                schema=schema_name,
                table=table_schema.name,
                error=str(e)
            )
            raise

    async def _create_indexes(
        self, conn: Connection, schema_name: str, table_schema: TableSchema
    ) -> None:
        """Create indexes for a table."""
        for index_def in table_schema.indexes or []:
            try:
                index_name = index_def.get("name")
                columns = index_def.get("columns", [])
                unique = index_def.get("unique", False)
                
                if not index_name or not columns:
                    logger.warning("Skipping invalid index definition", index=index_def)
                    continue
                
                unique_clause = "UNIQUE " if unique else ""
                columns_clause = ", ".join(f'"{col}"' for col in columns)
                
                query = f'''
                    CREATE {unique_clause}INDEX IF NOT EXISTS "{index_name}"
                    ON "{schema_name}"."{table_schema.name}" ({columns_clause})
                '''
                
                await conn.execute(query)
                logger.debug("Index created", index=index_name)
                
            except Exception as e:
                logger.warning("Failed to create index", index=index_def, error=str(e))

    async def _create_soft_delete_indexes(
        self, conn: Connection, schema_name: str, table_schema: TableSchema
    ) -> None:
        """Create performance indexes for soft delete operations."""
        try:
            # Create index for active (non-deleted) records
            index_name = f"idx_{table_schema.name}_{self.soft_delete_flag_column}_active"
            query = f'''
                CREATE INDEX IF NOT EXISTS "{index_name}"
                ON "{schema_name}"."{table_schema.name}" ("{self.soft_delete_flag_column}")
                WHERE "{self.soft_delete_flag_column}" IS NULL OR "{self.soft_delete_flag_column}" = FALSE
            '''
            await conn.execute(query)
            logger.debug("Soft delete index created", index=index_name)
            
        except Exception as e:
            logger.warning("Failed to create soft delete indexes", table=table_schema.name, error=str(e))

    async def write_batch(self, schema_name: str, records: List[Record]) -> None:
        """Write a batch of records using optimized UPSERT operations."""
        if not records:
            return
            
        # Group records by table for efficient processing
        records_by_table: Dict[str, List[Record]] = {}
        for record in records:
            table_name = record.table_name
            if table_name not in records_by_table:
                records_by_table[table_name] = []
            records_by_table[table_name].append(record)
        
        # Process each table's records
        for table_name, table_records in records_by_table.items():
            await self._write_table_batch(schema_name, table_name, table_records)

    async def _write_table_batch(
        self, schema_name: str, table_name: str, records: List[Record]
    ) -> None:
        """Write records for a specific table."""
        table_key = f"{schema_name}.{table_name}"
        table_schema = self._table_schemas.get(table_key)
        
        if not table_schema:
            logger.warning("Table schema not found, skipping batch", table=table_key)
            return
        
        try:
            async with self.pool.acquire() as conn:  # type: ignore[union-attr]
                async with conn.transaction():
                    # Process records in smaller batches for memory efficiency
                    for i in range(0, len(records), self.batch_size):
                        batch = records[i:i + self.batch_size]
                        await self._process_record_batch(conn, schema_name, table_schema, batch)
                        
            logger.debug(
                "Batch processed successfully",
                schema=schema_name,
                table=table_name,
                records=len(records)
            )
            
        except Exception as e:
            logger.error(
                "Failed to process batch",
                schema=schema_name,
                table=table_name,
                records=len(records),
                error=str(e)
            )
            raise

    async def _process_record_batch(
        self,
        conn: Connection,
        schema_name: str,
        table_schema: TableSchema,
        records: List[Record]
    ) -> None:
        """Process a batch of records with appropriate operations."""
        inserts = []
        updates = []
        deletes = []
        
        # Categorize records by operation type
        for record in records:
            if record.operation == OperationType.INSERT:
                inserts.append(record)
            elif record.operation == OperationType.UPDATE:
                updates.append(record)
            elif record.operation == OperationType.DELETE:
                deletes.append(record)
        
        # Process each operation type
        if inserts:
            await self._process_inserts(conn, schema_name, table_schema, inserts)
        if updates:
            await self._process_updates(conn, schema_name, table_schema, updates)
        if deletes:
            await self._process_deletes(conn, schema_name, table_schema, deletes)

    async def _process_inserts(
        self,
        conn: Connection,
        schema_name: str,
        table_schema: TableSchema,
        records: List[Record]
    ) -> None:
        """Process INSERT operations using UPSERT."""
        if not records:
            return
        
        # Build UPSERT query
        columns = [col.name for col in table_schema.columns]
        
        # Add metadata columns
        if self.enable_soft_deletes:
            columns.extend([self.soft_delete_flag_column, self.soft_delete_timestamp_column])
        columns.extend(["_cartridge_created_at", "_cartridge_updated_at", "_cartridge_version"])
        
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        columns_clause = ", ".join(f'"{col}"' for col in columns)
        
        # Build conflict resolution
        conflict_columns = table_schema.primary_keys or ["_cartridge_created_at"]
        conflict_clause = ", ".join(f'"{col}"' for col in conflict_columns)
        
        # Update clause for conflicts
        update_sets = []
        for col in columns:
            if col not in conflict_columns:
                update_sets.append(f'"{col}" = EXCLUDED."{col}"')
        update_clause = ", ".join(update_sets)
        
        query = f'''
            INSERT INTO "{schema_name}"."{table_schema.name}" ({columns_clause})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_clause})
            DO UPDATE SET {update_clause}
        '''
        
        # Prepare data for batch insert
        batch_data = []
        for record in records:
            row_data = []
            
            # Process each column
            for col in table_schema.columns:
                value = record.data.get(col.name)
                converted_value = self.type_mapper.convert_value(value, col.type)
                row_data.append(converted_value)
            
            # Add metadata values
            if self.enable_soft_deletes:
                row_data.extend([False, None])  # is_deleted, deleted_at
            
            now = datetime.now(timezone.utc)
            row_data.extend([now, now, 1])  # created_at, updated_at, version
            
            batch_data.append(row_data)
        
        # Execute batch insert - use copy for large batches, executemany for smaller ones
        if len(batch_data) > 100:  # Use copy for bulk operations
            await self._bulk_copy_insert(conn, schema_name, table_schema, columns, batch_data)
        else:
            await conn.executemany(query, batch_data)

    async def _bulk_copy_insert(
        self,
        conn: Connection,
        schema_name: str,
        table_schema: TableSchema,
        columns: List[str],
        batch_data: List[List[Any]]
    ) -> None:
        """Use COPY for bulk insert operations for better performance."""
        try:
            # Create a temporary table for COPY operation
            temp_table = f"temp_{table_schema.name}_{uuid.uuid4().hex[:8]}"
            
            # Create temporary table with same structure
            columns_def = []
            for col_name in columns:
                # Find column definition from schema
                col_def = None
                for schema_col in table_schema.columns:
                    if schema_col.name == col_name:
                        col_def = schema_col
                        break
                
                if col_def:
                    pg_type = self.type_mapper.get_postgresql_type(col_def.type, col_def.max_length)
                    columns_def.append(f'"{col_name}" {pg_type}')
                else:
                    # Metadata columns
                    if col_name in ["_cartridge_created_at", "_cartridge_updated_at"]:
                        columns_def.append(f'"{col_name}" TIMESTAMP WITH TIME ZONE')
                    elif col_name == "_cartridge_version":
                        columns_def.append(f'"{col_name}" INTEGER')
                    elif col_name == self.soft_delete_flag_column:
                        columns_def.append(f'"{col_name}" BOOLEAN')
                    elif col_name == self.soft_delete_timestamp_column:
                        columns_def.append(f'"{col_name}" TIMESTAMP WITH TIME ZONE')
            
            create_temp_query = f'''
                CREATE TEMP TABLE "{temp_table}" ({", ".join(columns_def)})
            '''
            
            await conn.execute(create_temp_query)
            
            # Use copy_records_to_table for bulk insert
            columns_tuple = tuple(columns)
            await conn.copy_records_to_table(temp_table, records=batch_data, columns=columns_tuple)
            
            # Insert from temp table to main table with UPSERT logic
            main_columns = ", ".join(f'"{col}"' for col in columns)
            temp_columns = ", ".join(f'temp."{col}"' for col in columns)
            
            # Build conflict resolution
            conflict_columns = table_schema.primary_keys or ["_cartridge_created_at"]
            conflict_clause = ", ".join(f'"{col}"' for col in conflict_columns)
            
            # Update clause for conflicts
            update_sets = []
            for col in columns:
                if col not in conflict_columns:
                    update_sets.append(f'"{col}" = EXCLUDED."{col}"')
            update_clause = ", ".join(update_sets)
            
            upsert_query = f'''
                INSERT INTO "{schema_name}"."{table_schema.name}" ({main_columns})
                SELECT {temp_columns} FROM "{temp_table}" temp
                ON CONFLICT ({conflict_clause})
                DO UPDATE SET {update_clause}
            '''
            
            await conn.execute(upsert_query)
            
            # Drop temporary table
            await conn.execute(f'DROP TABLE "{temp_table}"')
            
        except Exception as e:
            logger.error(
                "Bulk copy insert failed for table '%s.%s' with %d records. "
                "Falling back to executemany, which may significantly impact performance. "
                "Exception: %s",
                schema_name,
                table_schema.name,
                len(batch_data),
                str(e)
            )
            # Fallback to regular executemany
            query = f'''
                INSERT INTO "{schema_name}"."{table_schema.name}" ({", ".join(f'"{col}"' for col in columns)})
                VALUES ({", ".join(f"${i+1}" for i in range(len(columns)))})
                ON CONFLICT ({", ".join(f'"{col}"' for col in (table_schema.primary_keys or ["_cartridge_created_at"]))})
                DO UPDATE SET {", ".join(f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in (table_schema.primary_keys or ["_cartridge_created_at"]))}
            '''
            await conn.executemany(query, batch_data)

    async def _process_updates(
        self,
        conn: Connection,
        schema_name: str,
        table_schema: TableSchema,
        records: List[Record]
    ) -> None:
        """Process UPDATE operations."""
        for record in records:
            await self._process_single_update(conn, schema_name, table_schema, record)

    async def _process_single_update(
        self,
        conn: Connection,
        schema_name: str,
        table_schema: TableSchema,
        record: Record
    ) -> None:
        """Process a single UPDATE operation."""
        # Build UPDATE query
        set_clauses = []
        values = []
        param_idx = 1
        
        # Add data columns
        for col in table_schema.columns:
            if col.name in record.data:
                value = record.data[col.name]
                converted_value = self.type_mapper.convert_value(value, col.type)
                set_clauses.append(f'"{col.name}" = ${param_idx}')
                values.append(converted_value)
                param_idx += 1
        
        # Add metadata columns
        set_clauses.append(f'"_cartridge_updated_at" = ${param_idx}')
        values.append(datetime.now(timezone.utc))
        param_idx += 1
        
        set_clauses.append(f'"_cartridge_version" = "_cartridge_version" + 1')
        
        # Build WHERE clause
        where_clauses = []
        for pk_col, pk_value in record.primary_key_values.items():
            where_clauses.append(f'"{pk_col}" = ${param_idx}')
            values.append(pk_value)
            param_idx += 1
        
        set_clause = ", ".join(set_clauses)
        where_clause = " AND ".join(where_clauses)
        
        query = f'''
            UPDATE "{schema_name}"."{table_schema.name}"
            SET {set_clause}
            WHERE {where_clause}
        '''
        
        await conn.execute(query, *values)

    async def _process_deletes(
        self,
        conn: Connection,
        schema_name: str,
        table_schema: TableSchema,
        records: List[Record]
    ) -> None:
        """Process DELETE operations based on deletion strategy."""
        for record in records:
            if self.deletion_strategy == "soft" or (
                self.deletion_strategy == "both" and self.enable_soft_deletes
            ):
                await self._process_soft_delete(conn, schema_name, table_schema, record)
            
            if self.deletion_strategy == "hard" or self.deletion_strategy == "both":
                await self._process_hard_delete(conn, schema_name, table_schema, record)

    async def _process_soft_delete(
        self,
        conn: Connection,
        schema_name: str,
        table_schema: TableSchema,
        record: Record
    ) -> None:
        """Process soft delete by setting is_deleted flag."""
        values = []
        param_idx = 1
        
        # Build WHERE clause
        where_clauses = []
        for pk_col, pk_value in record.primary_key_values.items():
            where_clauses.append(f'"{pk_col}" = ${param_idx}')
            values.append(pk_value)
            param_idx += 1
        
        where_clause = " AND ".join(where_clauses)
        
        # Add deletion timestamp
        values.extend([True, datetime.now(timezone.utc), datetime.now(timezone.utc)])
        
        query = f'''
            UPDATE "{schema_name}"."{table_schema.name}"
            SET "{self.soft_delete_flag_column}" = ${param_idx}, 
                "{self.soft_delete_timestamp_column}" = ${param_idx + 1},
                "_cartridge_updated_at" = ${param_idx + 2},
                "_cartridge_version" = "_cartridge_version" + 1
            WHERE {where_clause} AND ("{self.soft_delete_flag_column}" IS NULL OR "{self.soft_delete_flag_column}" = FALSE)
        '''
        
        await conn.execute(query, *values)

    async def _process_hard_delete(
        self,
        conn: Connection,
        schema_name: str,
        table_schema: TableSchema,
        record: Record
    ) -> None:
        """Process hard delete by removing the record."""
        values = []
        param_idx = 1
        
        # Build WHERE clause
        where_clauses = []
        for pk_col, pk_value in record.primary_key_values.items():
            where_clauses.append(f'"{pk_col}" = ${param_idx}')
            values.append(pk_value)
            param_idx += 1
        
        where_clause = " AND ".join(where_clauses)
        
        query = f'DELETE FROM "{schema_name}"."{table_schema.name}" WHERE {where_clause}'
        
        await conn.execute(query, *values)

    async def apply_schema_changes(
        self, schema_name: str, changes: List[SchemaChange]
    ) -> None:
        """Apply schema changes to PostgreSQL."""
        if not changes:
            return
        
        try:
            async with self.pool.acquire() as conn:  # type: ignore[union-attr]
                for change in changes:
                    await self._apply_single_schema_change(conn, change)
                    
            logger.info("Schema changes applied", changes=len(changes))
            
        except Exception as e:
            logger.error("Failed to apply schema changes", error=str(e))
            raise

    async def _apply_single_schema_change(
        self, conn: Connection, change: SchemaChange
    ) -> None:
        """Apply a single schema change."""
        change_type = change.change_type
        schema_name = change.schema_name
        table_name = change.table_name
        details = change.details
        
        try:
            if change_type == "add_column":
                await self._add_column(conn, schema_name, table_name, details)
            elif change_type == "modify_column":
                await self._modify_column(conn, schema_name, table_name, details)
            elif change_type == "add_table":
                await self._add_table(conn, schema_name, details)
            elif change_type == "drop_column":
                logger.warning("Column dropping not supported for safety", change=change)
            elif change_type == "drop_table":
                logger.warning("Table dropping not supported for safety", change=change)
            else:
                logger.warning("Unsupported schema change type", change_type=change_type)
                
        except Exception as e:
            logger.error("Failed to apply schema change", change=change, error=str(e))
            raise

    async def _add_column(
        self, conn: Connection, schema_name: str, table_name: str, details: Dict[str, Any]
    ) -> None:
        """Add a new column to an existing table."""
        column_name = details.get("column_name")
        column_type = ColumnType(details.get("column_type", "string"))
        nullable = details.get("nullable", True)
        default = details.get("default")
        
        if not column_name:
            raise ValueError("Column name is required for add_column operation")
        
        pg_type = self.type_mapper.get_postgresql_type(column_type)
        nullable_clause = "NULL" if nullable else "NOT NULL"
        
        query = f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN IF NOT EXISTS "{column_name}" {pg_type} {nullable_clause}'
        
        if default is not None:
            if column_type == ColumnType.STRING:
                query += f" DEFAULT '{default}'"
            else:
                query += f" DEFAULT {default}"
        
        await conn.execute(query)
        logger.info("Column added", schema=schema_name, table=table_name, column=column_name)

    async def _modify_column(
        self, conn: Connection, schema_name: str, table_name: str, details: Dict[str, Any]
    ) -> None:
        """Modify an existing column (type widening only for safety)."""
        column_name = details.get("column_name")
        new_type = ColumnType(details.get("new_type", "string"))
        old_type = ColumnType(details.get("old_type", "string"))
        
        if not column_name:
            raise ValueError("Column name is required for modify_column operation")
        
        # Only allow safe type widening based on configuration
        if (old_type.value, new_type.value) not in self.safe_type_conversions:
            logger.warning(
                "Unsafe column type change skipped",
                column=column_name,
                old_type=old_type.value,
                new_type=new_type.value
            )
            return
        
        pg_type = self.type_mapper.get_postgresql_type(new_type)
        query = f'ALTER TABLE "{schema_name}"."{table_name}" ALTER COLUMN "{column_name}" TYPE {pg_type}'
        
        await conn.execute(query)
        logger.info(
            "Column type modified",
            schema=schema_name,
            table=table_name,
            column=column_name,
            new_type=new_type.value
        )

    async def _add_table(
        self, conn: Connection, schema_name: str, details: Dict[str, Any]
    ) -> None:
        """Add a new table based on schema change details."""
        table_schema = TableSchema(
            name=details["table_name"],
            columns=[
                ColumnDefinition(
                    name=col["name"],
                    type=ColumnType(col["type"]),
                    nullable=col.get("nullable", True),
                    default=col.get("default"),
                )
                for col in details.get("columns", [])
            ],
            primary_keys=details.get("primary_keys", []),
            indexes=details.get("indexes"),
        )
        
        await self.create_table_if_not_exists(schema_name, table_schema)

    async def update_marker(
        self, schema_name: str, table_name: str, marker: Any
    ) -> None:
        """Update processing position marker for a table."""
        try:
            # Ensure metadata schema exists
            await self.create_schema_if_not_exists(self.metadata_schema)
            
            # Create markers table if needed
            await self._create_markers_table()
            
            marker_value = safe_json_dumps(marker) if marker is not None else None
            
            query = f'''
                INSERT INTO "{self.metadata_schema}".processing_markers 
                (schema_name, table_name, marker_value, updated_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (schema_name, table_name)
                DO UPDATE SET 
                    marker_value = EXCLUDED.marker_value,
                    updated_at = EXCLUDED.updated_at
            '''
            
            async with self.pool.acquire() as conn:  # type: ignore[union-attr]
                await conn.execute(
                    query,
                    schema_name,
                    table_name,
                    marker_value,
                    datetime.now(timezone.utc)
                )
                
        except Exception as e:
            logger.error(
                "Failed to update marker",
                schema=schema_name,
                table=table_name,
                error=str(e)
            )
            raise

    async def get_marker(self, schema_name: str, table_name: str) -> Optional[Any]:
        """Get current processing position marker for a table."""
        try:
            # Ensure metadata schema and table exist
            await self.create_schema_if_not_exists(self.metadata_schema)
            await self._create_markers_table()
            
            query = f'''
                SELECT marker_value 
                FROM "{self.metadata_schema}".processing_markers
                WHERE schema_name = $1 AND table_name = $2
            '''
            
            async with self.pool.acquire() as conn:  # type: ignore[union-attr]
                row = await conn.fetchrow(query, schema_name, table_name)
                
                if row and row["marker_value"]:
                    return json.loads(row["marker_value"])
                return None
                
        except Exception as e:
            logger.error(
                "Failed to get marker",
                schema=schema_name,
                table=table_name,
                error=str(e)
            )
            return None

    async def _create_markers_table(self) -> None:
        """Create processing markers table if it doesn't exist."""
        query = f'''
            CREATE TABLE IF NOT EXISTS "{self.metadata_schema}".processing_markers (
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                marker_value JSONB,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (schema_name, table_name)
            )
        '''
        
        async with self.pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(query)
