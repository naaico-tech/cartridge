"""Base classes for database schema scanning."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class DataType(str, Enum):
    """Standard data types across different databases."""
    
    # Numeric types
    INTEGER = "integer"
    BIGINT = "bigint"
    SMALLINT = "smallint"
    DECIMAL = "decimal"
    NUMERIC = "numeric"
    FLOAT = "float"
    DOUBLE = "double"
    REAL = "real"
    
    # String types
    VARCHAR = "varchar"
    CHAR = "char"
    TEXT = "text"
    
    # Date/time types
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    INTERVAL = "interval"
    
    # Boolean
    BOOLEAN = "boolean"
    
    # Binary
    BINARY = "binary"
    VARBINARY = "varbinary"
    BLOB = "blob"
    
    # JSON
    JSON = "json"
    JSONB = "jsonb"
    
    # Array
    ARRAY = "array"
    
    # Other
    UUID = "uuid"
    UNKNOWN = "unknown"


@dataclass
class ColumnInfo:
    """Information about a database column."""
    
    name: str
    data_type: DataType
    raw_type: str  # Original database-specific type
    nullable: bool
    default_value: Optional[str] = None
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: Optional[str] = None
    foreign_key_column: Optional[str] = None
    is_unique: bool = False
    is_indexed: bool = False
    comment: Optional[str] = None
    
    # Data quality metrics
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    avg_value: Optional[float] = None
    sample_values: Optional[List[Any]] = None


@dataclass
class ConstraintInfo:
    """Information about database constraints."""
    
    name: str
    type: str  # PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
    columns: List[str]
    referenced_table: Optional[str] = None
    referenced_columns: Optional[List[str]] = None
    definition: Optional[str] = None


@dataclass
class IndexInfo:
    """Information about database indexes."""
    
    name: str
    columns: List[str]
    is_unique: bool
    is_primary: bool
    type: str  # btree, hash, gin, gist, etc.
    definition: Optional[str] = None


@dataclass
class TableInfo:
    """Information about a database table."""
    
    name: str
    schema: str
    table_type: str  # table, view, materialized_view
    columns: List[ColumnInfo]
    constraints: List[ConstraintInfo]
    indexes: List[IndexInfo]
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    comment: Optional[str] = None
    
    # Sample data
    sample_data: Optional[List[Dict[str, Any]]] = None
    
    def get_primary_key_columns(self) -> List[str]:
        """Get primary key column names."""
        pk_columns = [col.name for col in self.columns if col.is_primary_key]
        if pk_columns:
            return pk_columns
        
        # Look for primary key constraint
        for constraint in self.constraints:
            if constraint.type == "PRIMARY KEY":
                return constraint.columns
        
        return []
    
    def get_foreign_key_relationships(self) -> List[Tuple[str, str, str]]:
        """Get foreign key relationships as (column, referenced_table, referenced_column)."""
        relationships = []
        
        # From column metadata
        for col in self.columns:
            if col.is_foreign_key and col.foreign_key_table:
                relationships.append((col.name, col.foreign_key_table, col.foreign_key_column or "id"))
        
        # From constraint metadata
        for constraint in self.constraints:
            if constraint.type == "FOREIGN KEY" and constraint.referenced_table:
                for i, col in enumerate(constraint.columns):
                    ref_col = constraint.referenced_columns[i] if constraint.referenced_columns else "id"
                    relationships.append((col, constraint.referenced_table, ref_col))
        
        return relationships


@dataclass
class DatabaseInfo:
    """Information about the database."""
    
    database_type: str
    version: str
    host: str
    port: int
    database_name: str
    schema_name: str
    total_tables: int
    total_views: int
    total_size_bytes: Optional[int] = None


@dataclass
class ScanResult:
    """Complete schema scan result."""
    
    database_info: DatabaseInfo
    tables: List[TableInfo]
    scan_duration_seconds: float
    scan_timestamp: str
    errors: List[str] = None
    
    def get_table_by_name(self, table_name: str, schema: Optional[str] = None) -> Optional[TableInfo]:
        """Get table by name and optional schema."""
        for table in self.tables:
            if table.name == table_name:
                if schema is None or table.schema == schema:
                    return table
        return None
    
    def get_relationships(self) -> List[Dict[str, Any]]:
        """Get all foreign key relationships in the database."""
        relationships = []
        
        for table in self.tables:
            for col, ref_table, ref_col in table.get_foreign_key_relationships():
                relationships.append({
                    "from_table": f"{table.schema}.{table.name}",
                    "from_column": col,
                    "to_table": ref_table,
                    "to_column": ref_col
                })
        
        return relationships


class DatabaseConnector(ABC):
    """Abstract base class for database connectors."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize connector with connection configuration."""
        self.config = connection_config
        self.connection = None
        self.logger = get_logger(f"{self.__class__.__name__}")
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """Test database connection and return status."""
        pass
    
    @abstractmethod
    async def get_database_info(self) -> DatabaseInfo:
        """Get general database information."""
        pass
    
    @abstractmethod
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of table names in the database."""
        pass
    
    @abstractmethod
    async def get_table_info(self, table_name: str, schema: Optional[str] = None) -> TableInfo:
        """Get detailed information about a specific table."""
        pass
    
    @abstractmethod
    async def get_sample_data(self, table_name: str, schema: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get sample data from a table."""
        pass
    
    @abstractmethod
    def normalize_data_type(self, raw_type: str) -> DataType:
        """Convert database-specific type to standard DataType."""
        pass
    
    async def scan_schema(
        self, 
        tables: Optional[List[str]] = None,
        include_sample_data: bool = True,
        sample_size: int = 100
    ) -> ScanResult:
        """Scan database schema and return complete information."""
        import time
        
        start_time = time.time()
        errors = []
        
        try:
            await self.connect()
            
            # Get database info
            db_info = await self.get_database_info()
            
            # Get tables to scan
            if tables is None:
                tables = await self.get_tables(schema=db_info.schema_name)
            
            # Scan each table
            table_infos = []
            for table_name in tables:
                try:
                    self.logger.info(f"Scanning table: {table_name}")
                    table_info = await self.get_table_info(table_name, schema=db_info.schema_name)
                    
                    # Get sample data if requested
                    if include_sample_data:
                        try:
                            sample_data = await self.get_sample_data(table_name, schema=db_info.schema_name, limit=sample_size)
                            table_info.sample_data = sample_data
                        except Exception as e:
                            self.logger.warning(f"Failed to get sample data for {table_name}: {e}")
                            errors.append(f"Sample data error for {table_name}: {str(e)}")
                    
                    table_infos.append(table_info)
                    
                except Exception as e:
                    self.logger.error(f"Failed to scan table {table_name}: {e}")
                    errors.append(f"Table scan error for {table_name}: {str(e)}")
            
            # Update database info with actual counts
            db_info.total_tables = len([t for t in table_infos if t.table_type == "table"])
            db_info.total_views = len([t for t in table_infos if t.table_type in ["view", "materialized_view"]])
            
            scan_duration = time.time() - start_time
            
            return ScanResult(
                database_info=db_info,
                tables=table_infos,
                scan_duration_seconds=scan_duration,
                scan_timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                errors=errors if errors else None
            )
            
        finally:
            await self.disconnect()


class SchemaAnalyzer:
    """Analyzes database schema and provides insights."""
    
    def __init__(self, scan_result: ScanResult):
        """Initialize analyzer with scan result."""
        self.scan_result = scan_result
        self.logger = get_logger(__name__)
    
    def detect_fact_tables(self) -> List[str]:
        """Detect potential fact tables based on naming and structure."""
        fact_tables = []
        
        for table in self.scan_result.tables:
            # Check naming patterns
            if any(pattern in table.name.lower() for pattern in ['fact', 'sales', 'order', 'transaction', 'event']):
                fact_tables.append(table.name)
                continue
            
            # Check structure - many foreign keys, numeric measures
            fk_count = len(table.get_foreign_key_relationships())
            numeric_cols = len([col for col in table.columns if col.data_type in [
                DataType.INTEGER, DataType.BIGINT, DataType.DECIMAL, DataType.FLOAT, DataType.DOUBLE
            ]])
            
            if fk_count >= 2 and numeric_cols >= 2:
                fact_tables.append(table.name)
        
        return fact_tables
    
    def detect_dimension_tables(self) -> List[str]:
        """Detect potential dimension tables."""
        dimension_tables = []
        
        for table in self.scan_result.tables:
            # Check naming patterns
            if any(pattern in table.name.lower() for pattern in ['dim', 'customer', 'product', 'user', 'category']):
                dimension_tables.append(table.name)
                continue
            
            # Check structure - primary key, descriptive columns
            pk_cols = table.get_primary_key_columns()
            text_cols = len([col for col in table.columns if col.data_type in [
                DataType.VARCHAR, DataType.CHAR, DataType.TEXT
            ]])
            
            if len(pk_cols) == 1 and text_cols >= 2:
                dimension_tables.append(table.name)
        
        return dimension_tables
    
    def detect_bridge_tables(self) -> List[str]:
        """Detect bridge/junction tables for many-to-many relationships."""
        bridge_tables = []
        
        for table in self.scan_result.tables:
            fk_relationships = table.get_foreign_key_relationships()
            
            # Bridge tables typically have 2+ foreign keys and few other columns
            if len(fk_relationships) >= 2 and len(table.columns) <= len(fk_relationships) + 2:
                bridge_tables.append(table.name)
        
        return bridge_tables
    
    def suggest_staging_models(self) -> List[Dict[str, Any]]:
        """Suggest staging models for each table."""
        staging_models = []
        
        for table in self.scan_result.tables:
            if table.table_type == "table":  # Only create staging for actual tables
                model_name = f"stg_{table.name}"
                
                staging_models.append({
                    "name": model_name,
                    "source_table": table.name,
                    "source_schema": table.schema,
                    "description": f"Staging model for {table.schema}.{table.name}",
                    "columns": [col.name for col in table.columns],
                    "primary_key": table.get_primary_key_columns(),
                    "tests": self._suggest_column_tests(table)
                })
        
        return staging_models
    
    def suggest_mart_models(self) -> List[Dict[str, Any]]:
        """Suggest mart models based on detected patterns."""
        mart_models = []
        
        fact_tables = self.detect_fact_tables()
        dimension_tables = self.detect_dimension_tables()
        
        # Create fact marts
        for fact_table in fact_tables:
            table_info = self.scan_result.get_table_by_name(fact_table)
            if table_info:
                model_name = f"fct_{fact_table}"
                
                mart_models.append({
                    "name": model_name,
                    "type": "fact",
                    "base_table": fact_table,
                    "description": f"Fact table for {fact_table}",
                    "joins": self._suggest_joins(table_info),
                    "measures": self._suggest_measures(table_info),
                    "tests": ["not_null", "unique"] if table_info.get_primary_key_columns() else ["not_null"]
                })
        
        # Create dimension marts
        for dim_table in dimension_tables:
            table_info = self.scan_result.get_table_by_name(dim_table)
            if table_info:
                model_name = f"dim_{dim_table}"
                
                mart_models.append({
                    "name": model_name,
                    "type": "dimension",
                    "base_table": dim_table,
                    "description": f"Dimension table for {dim_table}",
                    "natural_key": table_info.get_primary_key_columns(),
                    "attributes": [col.name for col in table_info.columns if not col.is_primary_key],
                    "tests": ["unique", "not_null"] if table_info.get_primary_key_columns() else ["not_null"]
                })
        
        return mart_models
    
    def _suggest_column_tests(self, table: TableInfo) -> List[Dict[str, Any]]:
        """Suggest dbt tests for table columns."""
        tests = []
        
        for col in table.columns:
            column_tests = []
            
            # Not null test for non-nullable columns
            if not col.nullable:
                column_tests.append("not_null")
            
            # Unique test for primary keys and unique columns
            if col.is_primary_key or col.is_unique:
                column_tests.append("unique")
            
            # Relationship test for foreign keys
            if col.is_foreign_key and col.foreign_key_table:
                column_tests.append({
                    "relationships": {
                        "to": f"ref('stg_{col.foreign_key_table}')",
                        "field": col.foreign_key_column or "id"
                    }
                })
            
            if column_tests:
                tests.append({
                    "column": col.name,
                    "tests": column_tests
                })
        
        return tests
    
    def _suggest_joins(self, table: TableInfo) -> List[Dict[str, Any]]:
        """Suggest joins for a fact table."""
        joins = []
        
        for col, ref_table, ref_col in table.get_foreign_key_relationships():
            joins.append({
                "table": f"stg_{ref_table}",
                "on": f"{table.name}.{col} = stg_{ref_table}.{ref_col}",
                "type": "left"
            })
        
        return joins
    
    def _suggest_measures(self, table: TableInfo) -> List[str]:
        """Suggest measures for a fact table."""
        measures = []
        
        for col in table.columns:
            if col.data_type in [DataType.INTEGER, DataType.BIGINT, DataType.DECIMAL, DataType.FLOAT, DataType.DOUBLE]:
                if not col.is_foreign_key and not col.is_primary_key:
                    # Common measure patterns
                    col_lower = col.name.lower()
                    if any(pattern in col_lower for pattern in ['amount', 'total', 'sum', 'count', 'quantity', 'price', 'cost']):
                        measures.append(col.name)
        
        return measures