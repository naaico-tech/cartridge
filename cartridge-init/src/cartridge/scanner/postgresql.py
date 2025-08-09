"""PostgreSQL database connector for schema scanning."""

import asyncio
from typing import Dict, List, Any, Optional
import asyncpg
from asyncpg import Connection

from cartridge.scanner.base import (
    DatabaseConnector, DatabaseInfo, TableInfo, ColumnInfo, ConstraintInfo, 
    IndexInfo, DataType
)
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize PostgreSQL connector."""
        super().__init__(connection_config)
        self.connection: Optional[Connection] = None
    
    async def connect(self) -> None:
        """Establish PostgreSQL connection."""
        try:
            self.connection = await asyncpg.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["username"],
                password=self.config["password"],
                server_settings={
                    'application_name': 'cartridge_scanner'
                }
            )
            self.logger.info("Connected to PostgreSQL database")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from PostgreSQL")
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test PostgreSQL connection."""
        try:
            await self.connect()
            
            # Test with a simple query
            result = await self.connection.fetchval("SELECT version()")
            
            return {
                "status": "success",
                "message": "Connection successful",
                "database_version": result,
                "database_type": "postgresql"
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "message": f"Connection failed: {str(e)}",
                "error": str(e)
            }
        finally:
            await self.disconnect()
    
    async def get_database_info(self) -> DatabaseInfo:
        """Get PostgreSQL database information."""
        # Get version
        version = await self.connection.fetchval("SELECT version()")
        
        # Get database size
        db_size_query = """
            SELECT pg_database_size(current_database())
        """
        db_size = await self.connection.fetchval(db_size_query)
        
        # Count tables and views
        count_query = """
            SELECT 
                COUNT(CASE WHEN table_type = 'BASE TABLE' THEN 1 END) as tables,
                COUNT(CASE WHEN table_type = 'VIEW' THEN 1 END) as views
            FROM information_schema.tables 
            WHERE table_schema = $1
        """
        counts = await self.connection.fetchrow(count_query, self.config["schema"])
        
        return DatabaseInfo(
            database_type="postgresql",
            version=version.split()[1] if version else "unknown",
            host=self.config["host"],
            port=self.config["port"],
            database_name=self.config["database"],
            schema_name=self.config["schema"],
            total_tables=counts["tables"] or 0,
            total_views=counts["views"] or 0,
            total_size_bytes=db_size
        )
    
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of table names."""
        schema = schema or self.config["schema"]
        
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = $1 
            AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_name
        """
        
        rows = await self.connection.fetch(query, schema)
        return [row["table_name"] for row in rows]
    
    async def get_table_info(self, table_name: str, schema: Optional[str] = None) -> TableInfo:
        """Get detailed table information."""
        schema = schema or self.config["schema"]
        
        # Get basic table info
        table_info_query = """
            SELECT 
                table_type,
                obj_description(c.oid) as comment
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE t.table_name = $1 AND t.table_schema = $2
        """
        table_row = await self.connection.fetchrow(table_info_query, table_name, schema)
        
        if not table_row:
            raise ValueError(f"Table {schema}.{table_name} not found")
        
        table_type = "table" if table_row["table_type"] == "BASE TABLE" else "view"
        
        # Get table size and row count for actual tables
        row_count = None
        size_bytes = None
        
        if table_type == "table":
            try:
                # Estimate row count (faster than COUNT(*))
                row_count_query = """
                    SELECT n_tup_ins - n_tup_del as estimate
                    FROM pg_stat_user_tables 
                    WHERE schemaname = $1 AND relname = $2
                """
                row_count_result = await self.connection.fetchval(row_count_query, schema, table_name)
                row_count = max(0, row_count_result) if row_count_result else None
                
                # If estimate is 0 or None, do actual count for small tables
                if not row_count:
                    actual_count = await self.connection.fetchval(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
                    row_count = actual_count
                
                # Get table size
                size_query = """
                    SELECT pg_total_relation_size(c.oid) as size
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = $1 AND n.nspname = $2
                """
                size_bytes = await self.connection.fetchval(size_query, table_name, schema)
                
            except Exception as e:
                self.logger.warning(f"Failed to get table stats for {table_name}: {e}")
        
        # Get columns
        columns = await self._get_columns(table_name, schema)
        
        # Get constraints
        constraints = await self._get_constraints(table_name, schema)
        
        # Get indexes
        indexes = await self._get_indexes(table_name, schema)
        
        return TableInfo(
            name=table_name,
            schema=schema,
            table_type=table_type,
            columns=columns,
            constraints=constraints,
            indexes=indexes,
            row_count=row_count,
            size_bytes=size_bytes,
            comment=table_row["comment"]
        )
    
    async def _get_columns(self, table_name: str, schema: str) -> List[ColumnInfo]:
        """Get column information for a table."""
        query = """
            SELECT 
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                col_description(pgc.oid, c.ordinal_position) as comment,
                
                -- Check if column is part of primary key
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
                
                -- Check if column is part of a unique constraint
                CASE WHEN uk.column_name IS NOT NULL THEN true ELSE false END as is_unique,
                
                -- Foreign key information
                fk.foreign_table_name,
                fk.foreign_column_name
                
            FROM information_schema.columns c
            LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
            LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema
            
            -- Primary key check
            LEFT JOIN (
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = $1 AND tc.table_schema = $2 
                AND tc.constraint_type = 'PRIMARY KEY'
            ) pk ON pk.column_name = c.column_name
            
            -- Unique constraint check
            LEFT JOIN (
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = $1 AND tc.table_schema = $2 
                AND tc.constraint_type = 'UNIQUE'
            ) uk ON uk.column_name = c.column_name
            
            -- Foreign key information
            LEFT JOIN (
                SELECT 
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
                WHERE tc.table_name = $1 AND tc.table_schema = $2 
                AND tc.constraint_type = 'FOREIGN KEY'
            ) fk ON fk.column_name = c.column_name
            
            WHERE c.table_name = $1 AND c.table_schema = $2
            ORDER BY c.ordinal_position
        """
        
        rows = await self.connection.fetch(query, table_name, schema)
        columns = []
        
        for row in rows:
            # Check if column is indexed
            is_indexed = await self._is_column_indexed(table_name, schema, row["column_name"])
            
            column = ColumnInfo(
                name=row["column_name"],
                data_type=self.normalize_data_type(row["udt_name"]),
                raw_type=row["data_type"],
                nullable=row["is_nullable"] == "YES",
                default_value=row["column_default"],
                max_length=row["character_maximum_length"],
                precision=row["numeric_precision"],
                scale=row["numeric_scale"],
                is_primary_key=row["is_primary_key"],
                is_foreign_key=row["foreign_table_name"] is not None,
                foreign_key_table=row["foreign_table_name"],
                foreign_key_column=row["foreign_column_name"],
                is_unique=row["is_unique"],
                is_indexed=is_indexed,
                comment=row["comment"]
            )
            
            # Get data quality metrics
            try:
                await self._add_column_statistics(column, table_name, schema)
            except Exception as e:
                self.logger.warning(f"Failed to get statistics for column {column.name}: {e}")
            
            columns.append(column)
        
        return columns
    
    async def _is_column_indexed(self, table_name: str, schema: str, column_name: str) -> bool:
        """Check if a column is indexed."""
        query = """
            SELECT COUNT(*) > 0 as is_indexed
            FROM pg_indexes 
            WHERE schemaname = $1 AND tablename = $2 
            AND indexdef LIKE '%' || $3 || '%'
        """
        
        result = await self.connection.fetchval(query, schema, table_name, column_name)
        return bool(result)
    
    async def _add_column_statistics(self, column: ColumnInfo, table_name: str, schema: str) -> None:
        """Add data quality statistics to column info."""
        try:
            # Get null count and unique count
            stats_query = f'''
                SELECT 
                    COUNT(*) as total_count,
                    COUNT("{column.name}") as non_null_count,
                    COUNT(DISTINCT "{column.name}") as unique_count
                FROM "{schema}"."{table_name}"
            '''
            
            stats = await self.connection.fetchrow(stats_query)
            
            if stats:
                total_count = stats["total_count"]
                non_null_count = stats["non_null_count"]
                
                column.null_count = total_count - non_null_count
                column.unique_count = stats["unique_count"]
                
                # Get min/max/avg for numeric columns
                if column.data_type in [DataType.INTEGER, DataType.BIGINT, DataType.DECIMAL, DataType.FLOAT, DataType.DOUBLE]:
                    minmax_query = f'''
                        SELECT 
                            MIN("{column.name}") as min_val,
                            MAX("{column.name}") as max_val,
                            AVG("{column.name}") as avg_val
                        FROM "{schema}"."{table_name}"
                        WHERE "{column.name}" IS NOT NULL
                    '''
                    
                    minmax = await self.connection.fetchrow(minmax_query)
                    if minmax:
                        column.min_value = minmax["min_val"]
                        column.max_value = minmax["max_val"]
                        column.avg_value = float(minmax["avg_val"]) if minmax["avg_val"] else None
                
                # Get sample values
                sample_query = f'''
                    SELECT DISTINCT "{column.name}" 
                    FROM "{schema}"."{table_name}" 
                    WHERE "{column.name}" IS NOT NULL 
                    ORDER BY "{column.name}" 
                    LIMIT 10
                '''
                
                sample_rows = await self.connection.fetch(sample_query)
                column.sample_values = [row[0] for row in sample_rows]
                
        except Exception as e:
            self.logger.warning(f"Failed to get column statistics: {e}")
    
    async def _get_constraints(self, table_name: str, schema: str) -> List[ConstraintInfo]:
        """Get constraint information for a table."""
        query = """
            SELECT 
                tc.constraint_name,
                tc.constraint_type,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
                ccu.table_name as referenced_table,
                array_agg(ccu.column_name ORDER BY kcu.ordinal_position) as referenced_columns,
                rc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            LEFT JOIN information_schema.check_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            WHERE tc.table_name = $1 AND tc.table_schema = $2
            GROUP BY tc.constraint_name, tc.constraint_type, ccu.table_name, rc.check_clause
            ORDER BY tc.constraint_type, tc.constraint_name
        """
        
        rows = await self.connection.fetch(query, table_name, schema)
        constraints = []
        
        for row in rows:
            constraint = ConstraintInfo(
                name=row["constraint_name"],
                type=row["constraint_type"],
                columns=list(row["columns"]) if row["columns"] else [],
                referenced_table=row["referenced_table"],
                referenced_columns=list(row["referenced_columns"]) if row["referenced_columns"] else None,
                definition=row["check_clause"]
            )
            constraints.append(constraint)
        
        return constraints
    
    async def _get_indexes(self, table_name: str, schema: str) -> List[IndexInfo]:
        """Get index information for a table."""
        query = """
            SELECT 
                indexname as name,
                indexdef as definition,
                array_agg(attname ORDER BY attnum) as columns
            FROM pg_indexes
            LEFT JOIN pg_class ON pg_class.relname = indexname
            LEFT JOIN pg_index ON pg_index.indexrelid = pg_class.oid
            LEFT JOIN pg_attribute ON pg_attribute.attrelid = pg_index.indrelid 
                AND pg_attribute.attnum = ANY(pg_index.indkey)
            WHERE schemaname = $1 AND tablename = $2
            GROUP BY indexname, indexdef
            ORDER BY indexname
        """
        
        rows = await self.connection.fetch(query, schema, table_name)
        indexes = []
        
        for row in rows:
            # Parse index properties from definition
            definition = row["definition"] or ""
            is_unique = "UNIQUE" in definition
            is_primary = "PRIMARY KEY" in definition
            
            # Determine index type
            index_type = "btree"  # Default
            if "USING gin" in definition.lower():
                index_type = "gin"
            elif "USING gist" in definition.lower():
                index_type = "gist"
            elif "USING hash" in definition.lower():
                index_type = "hash"
            
            index = IndexInfo(
                name=row["name"],
                columns=list(row["columns"]) if row["columns"] else [],
                is_unique=is_unique,
                is_primary=is_primary,
                type=index_type,
                definition=definition
            )
            indexes.append(index)
        
        return indexes
    
    async def get_sample_data(self, table_name: str, schema: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get sample data from a table."""
        schema = schema or self.config["schema"]
        
        query = f'SELECT * FROM "{schema}"."{table_name}" LIMIT $1'
        
        rows = await self.connection.fetch(query, limit)
        
        # Convert rows to dictionaries
        sample_data = []
        for row in rows:
            row_dict = {}
            for key, value in row.items():
                # Convert special types to JSON-serializable formats
                if hasattr(value, 'isoformat'):  # datetime objects
                    row_dict[key] = value.isoformat()
                elif isinstance(value, (bytes, bytearray)):  # binary data
                    row_dict[key] = f"<binary data: {len(value)} bytes>"
                else:
                    row_dict[key] = value
            sample_data.append(row_dict)
        
        return sample_data
    
    def normalize_data_type(self, raw_type: str) -> DataType:
        """Convert PostgreSQL type to standard DataType."""
        type_mapping = {
            # Integer types
            "int2": DataType.SMALLINT,
            "int4": DataType.INTEGER,
            "int8": DataType.BIGINT,
            "smallint": DataType.SMALLINT,
            "integer": DataType.INTEGER,
            "bigint": DataType.BIGINT,
            
            # Numeric types
            "numeric": DataType.NUMERIC,
            "decimal": DataType.DECIMAL,
            "real": DataType.REAL,
            "float4": DataType.REAL,
            "float8": DataType.DOUBLE,
            "double": DataType.DOUBLE,
            
            # String types
            "varchar": DataType.VARCHAR,
            "char": DataType.CHAR,
            "text": DataType.TEXT,
            "bpchar": DataType.CHAR,
            
            # Date/time types
            "date": DataType.DATE,
            "time": DataType.TIME,
            "timestamp": DataType.TIMESTAMP,
            "timestamptz": DataType.TIMESTAMPTZ,
            "interval": DataType.INTERVAL,
            
            # Boolean
            "bool": DataType.BOOLEAN,
            "boolean": DataType.BOOLEAN,
            
            # Binary
            "bytea": DataType.BINARY,
            
            # JSON
            "json": DataType.JSON,
            "jsonb": DataType.JSONB,
            
            # UUID
            "uuid": DataType.UUID,
            
            # Array
            "_int4": DataType.ARRAY,
            "_text": DataType.ARRAY,
            "_varchar": DataType.ARRAY,
        }
        
        # Handle array types
        if raw_type.startswith("_"):
            return DataType.ARRAY
        
        return type_mapping.get(raw_type.lower(), DataType.UNKNOWN)