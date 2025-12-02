"""BigQuery database connector for schema scanning."""

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

from google.cloud import bigquery
from google.oauth2 import service_account, credentials as google_credentials
from google.auth import default
from google.api_core import exceptions as google_exceptions

from cartridge.scanner.base import (
    DatabaseConnector, DatabaseInfo, TableInfo, ColumnInfo, ConstraintInfo, 
    IndexInfo, DataType
)
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class BigQueryConnector(DatabaseConnector):
    """
    BigQuery database connector.
    Supports authentication via service account file, service account JSON, Google Auth token (access_token), or ADC.
    """

    def __init__(self, connection_config: Dict[str, Any]):
        """
        Initialize BigQuery connector.
        Args:
            connection_config (Dict[str, Any]): Configuration dict. Supports:
                - project_id
                - dataset_id or database
                - location
                - credentials_path
                - credentials_json
                - access_token (Google OAuth2 access token)
        """
        super().__init__(connection_config)
        self.client = None
        self.project_id = connection_config.get("project_id")
        self.dataset_id = connection_config.get("dataset_id") or connection_config.get("database")
        self.location = connection_config.get("location", "US")

        # Authentication configuration
        self.credentials_path = connection_config.get("credentials_path")
        self.credentials_json = connection_config.get("credentials_json")
        self.access_token = connection_config.get("access_token")
    
    async def connect(self) -> None:
        """
        Establish BigQuery connection using the best available authentication method.
        Order of precedence:
            1. access_token (Google OAuth2 token)
            2. credentials_json (service account JSON string/dict)
            3. credentials_path (service account file)
            4. ADC (Application Default Credentials)
        """
        try:
            credentials = None

            # 1. Google Auth token (OAuth2 access token)
            if self.access_token:
                credentials = google_credentials.Credentials(token=self.access_token)
            # 2. Service account key as JSON string/dict
            elif self.credentials_json:
                if isinstance(self.credentials_json, str):
                    credentials_info = json.loads(self.credentials_json)
                else:
                    credentials_info = self.credentials_json
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
            # 3. Service account key file path
            elif self.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
            # 4. Default credentials (ADC)
            else:
                try:
                    credentials, default_project = default()
                    if not self.project_id:
                        self.project_id = default_project
                except Exception:
                    credentials = None

            # Create BigQuery client
            if credentials:
                self.client = bigquery.Client(
                    project=self.project_id,
                    credentials=credentials,
                    location=self.location
                )
            else:
                self.client = bigquery.Client(
                    project=self.project_id,
                    location=self.location
                )

            self.logger.info(f"Connected to BigQuery project: {self.project_id}")

        except Exception as e:
            self.logger.error(f"Failed to connect to BigQuery: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close BigQuery connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.logger.info("Disconnected from BigQuery")
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test BigQuery connection and return status."""
        try:
            await self.connect()
            
            # Test connection by listing datasets
            datasets = list(self.client.list_datasets(project=self.project_id, max_results=1))
            
            return {
                "status": "success",
                "message": f"Successfully connected to BigQuery project {self.project_id}",
                "project_id": self.project_id,
                "location": self.location
            }
            
        except google_exceptions.Forbidden as e:
            return {
                "status": "failed",
                "message": f"Access denied to BigQuery project {self.project_id}",
                "error": str(e)
            }
        except google_exceptions.NotFound as e:
            return {
                "status": "failed", 
                "message": f"BigQuery project {self.project_id} not found",
                "error": str(e)
            }
        except Exception as e:
            return {
                "status": "failed",
                "message": f"Failed to connect to BigQuery: {str(e)}",
                "error": str(e)
            }
        finally:
            await self.disconnect()
    
    async def get_database_info(self) -> DatabaseInfo:
        """Get BigQuery database (dataset) information."""
        if not self.client:
            await self.connect()
        
        try:
            # Get dataset information
            dataset_ref = self.client.dataset(self.dataset_id, project=self.project_id)
            dataset = self.client.get_dataset(dataset_ref)
            
            # Count tables and views
            tables = list(self.client.list_tables(dataset_ref))
            table_count = sum(1 for t in tables if t.table_type == "TABLE")
            view_count = sum(1 for t in tables if t.table_type == "VIEW")
            
            return DatabaseInfo(
                database_type="bigquery",
                version="BigQuery Cloud Service",
                host=f"{self.project_id}.{self.location}",
                port=443,  # HTTPS port
                database_name=self.dataset_id,
                schema_name=self.dataset_id,  # In BigQuery, dataset is similar to schema
                total_tables=table_count,
                total_views=view_count,
                total_size_bytes=None  # Would require additional queries to calculate
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get database info: {e}")
            raise
    
    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """Get list of table names in the dataset."""
        if not self.client:
            await self.connect()
        
        try:
            dataset_id = schema or self.dataset_id
            dataset_ref = self.client.dataset(dataset_id, project=self.project_id)
            
            tables = []
            for table in self.client.list_tables(dataset_ref):
                tables.append(table.table_id)
            
            return tables
            
        except Exception as e:
            self.logger.error(f"Failed to get tables: {e}")
            raise
    
    async def get_table_info(self, table_name: str, schema: Optional[str] = None) -> TableInfo:
        """Get detailed information about a specific table."""
        if not self.client:
            await self.connect()
        
        try:
            dataset_id = schema or self.dataset_id
            table_ref = self.client.dataset(dataset_id, project=self.project_id).table(table_name)
            table = self.client.get_table(table_ref)
            
            # Get column information
            columns = await self._get_columns(table_name, dataset_id)
            
            # Get constraints (BigQuery has limited constraint support)
            constraints = await self._get_constraints(table_name, dataset_id)
            
            # BigQuery doesn't have traditional indexes, but we can get partitioning info
            indexes = await self._get_partitioning_info(table)
            
            # Get row count
            row_count = None
            if table.num_rows is not None:
                row_count = int(table.num_rows)
            
            # Get table size
            size_bytes = None
            if table.num_bytes is not None:
                size_bytes = int(table.num_bytes)
            
            return TableInfo(
                name=table_name,
                schema=dataset_id,
                table_type="view" if table.table_type == "VIEW" else "table",
                columns=columns,
                constraints=constraints,
                indexes=indexes,
                row_count=row_count,
                size_bytes=size_bytes,
                comment=table.description
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {e}")
            raise
    
    async def _get_columns(self, table_name: str, dataset_id: str) -> List[ColumnInfo]:
        """Get column information for a table."""
        try:
            table_ref = self.client.dataset(dataset_id, project=self.project_id).table(table_name)
            table = self.client.get_table(table_ref)
            
            columns = []
            for field in table.schema:
                column_info = self._process_field(field)
                columns.append(column_info)
            
            return columns
            
        except Exception as e:
            self.logger.error(f"Failed to get columns for {table_name}: {e}")
            raise
    
    def _process_field(self, field: bigquery.SchemaField, prefix: str = "") -> ColumnInfo:
        """Process a BigQuery schema field into ColumnInfo."""
        field_name = f"{prefix}{field.name}" if prefix else field.name
        
        # Handle nested fields (STRUCT)
        if field.field_type == "RECORD":
            # For STRUCT fields, we create a flattened representation
            data_type = DataType.JSON  # Use JSON as closest equivalent
            raw_type = f"STRUCT<{', '.join([f'{subfield.name}: {subfield.field_type}' for subfield in field.fields])}>"
        else:
            data_type = self.normalize_data_type(field.field_type)
            raw_type = field.field_type
        
        # Handle ARRAY types
        if field.mode == "REPEATED":
            data_type = DataType.ARRAY
            raw_type = f"ARRAY<{field.field_type}>"
        
        return ColumnInfo(
            name=field_name,
            data_type=data_type,
            raw_type=raw_type,
            nullable=(field.mode != "REQUIRED"),
            default_value=None,  # BigQuery doesn't support column defaults
            max_length=None,
            precision=field.precision if hasattr(field, 'precision') else None,
            scale=field.scale if hasattr(field, 'scale') else None,
            is_primary_key=False,  # BigQuery doesn't have traditional primary keys
            is_foreign_key=False,  # BigQuery doesn't have foreign key constraints
            is_unique=False,
            is_indexed=False,
            comment=field.description
        )
    
    async def _get_constraints(self, table_name: str, dataset_id: str) -> List[ConstraintInfo]:
        """Get constraint information (limited in BigQuery)."""
        # BigQuery has very limited constraint support
        # Primary keys and foreign keys are not enforced constraints
        constraints = []
        
        # Note: BigQuery does support NOT NULL constraints (REQUIRED mode)
        # but these are already captured in the column nullable property
        
        return constraints
    
    async def _get_partitioning_info(self, table: bigquery.Table) -> List[IndexInfo]:
        """Get partitioning and clustering information as index-like structures."""
        indexes = []
        
        # Partitioning information
        if table.time_partitioning:
            indexes.append(IndexInfo(
                name=f"partition_{table.time_partitioning.field or '_PARTITIONTIME'}",
                columns=[table.time_partitioning.field or "_PARTITIONTIME"],
                is_unique=False,
                is_primary=False,
                type="time_partition",
                definition=f"PARTITION BY {table.time_partitioning.type_}"
            ))
        
        if table.range_partitioning:
            indexes.append(IndexInfo(
                name=f"range_partition_{table.range_partitioning.field}",
                columns=[table.range_partitioning.field],
                is_unique=False,
                is_primary=False,
                type="range_partition",
                definition=f"PARTITION BY RANGE({table.range_partitioning.field})"
            ))
        
        # Clustering information
        if table.clustering_fields:
            indexes.append(IndexInfo(
                name="clustering",
                columns=list(table.clustering_fields),
                is_unique=False,
                is_primary=False,
                type="clustering",
                definition=f"CLUSTER BY {', '.join(table.clustering_fields)}"
            ))
        
        return indexes
    
    async def get_sample_data(self, table_name: str, schema: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get sample data from a table."""
        if not self.client:
            await self.connect()
        
        try:
            dataset_id = schema or self.dataset_id
            
            # Construct fully qualified table name
            full_table_name = f"`{self.project_id}.{dataset_id}.{table_name}`"
            
            # Query sample data
            query = f"""
            SELECT *
            FROM {full_table_name}
            LIMIT {limit}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Convert results to list of dictionaries
            sample_data = []
            for row in results:
                # Convert Row to dict, handling nested structures
                row_dict = {}
                for key, value in row.items():
                    row_dict[key] = self._serialize_value(value)
                sample_data.append(row_dict)
            
            return sample_data
            
        except Exception as e:
            self.logger.error(f"Failed to get sample data for {table_name}: {e}")
            raise
    
    def _serialize_value(self, value: Any) -> Any:
        """Serialize BigQuery values for JSON compatibility."""
        if value is None:
            return None
        elif isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (list, tuple)):
            return [self._serialize_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        else:
            # Convert other types to string
            return str(value)
    
    def normalize_data_type(self, raw_type: str) -> DataType:
        """Convert BigQuery-specific type to standard DataType."""
        type_mapping = {
            # Numeric types
            "INTEGER": DataType.BIGINT,
            "INT64": DataType.BIGINT,
            "FLOAT": DataType.FLOAT,
            "FLOAT64": DataType.DOUBLE,
            "NUMERIC": DataType.NUMERIC,
            "DECIMAL": DataType.DECIMAL,
            "BIGNUMERIC": DataType.NUMERIC,
            "BIGDECIMAL": DataType.DECIMAL,
            
            # String types
            "STRING": DataType.TEXT,
            "BYTES": DataType.BINARY,
            
            # Date/time types
            "DATE": DataType.DATE,
            "TIME": DataType.TIME,
            "DATETIME": DataType.TIMESTAMP,
            "TIMESTAMP": DataType.TIMESTAMPTZ,
            
            # Boolean
            "BOOLEAN": DataType.BOOLEAN,
            "BOOL": DataType.BOOLEAN,
            
            # JSON
            "JSON": DataType.JSON,
            
            # Complex types
            "RECORD": DataType.JSON,  # STRUCT - map to JSON
            "STRUCT": DataType.JSON,
            "ARRAY": DataType.ARRAY,
            "REPEATED": DataType.ARRAY,
            
            # Geographic
            "GEOGRAPHY": DataType.TEXT,  # No specific geographic type in our enum
            
            # Range types (BigQuery specific)
            "RANGE": DataType.TEXT,
            
            # Interval
            "INTERVAL": DataType.INTERVAL,
        }
        
        return type_mapping.get(raw_type.upper(), DataType.UNKNOWN)
