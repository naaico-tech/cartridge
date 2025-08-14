"""Tests for BigQuery database connector."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
import json

from cartridge.scanner.bigquery import BigQueryConnector
from cartridge.scanner.base import DataType, DatabaseInfo, TableInfo, ColumnInfo


class TestBigQueryConnector:
    """Test BigQuery database connector."""

    @pytest.fixture
    def bigquery_config(self):
        """BigQuery connection configuration for testing."""
        return {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "location": "US",
            "credentials_json": json.dumps({
                "type": "service_account",
                "project_id": "test-project",
                "private_key_id": "key-id",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKB\n-----END PRIVATE KEY-----\n",
                "client_email": "test@test-project.iam.gserviceaccount.com",
                "client_id": "123456789",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token"
            })
        }

    @pytest.fixture
    def connector(self, bigquery_config):
        """Create BigQuery connector instance."""
        return BigQueryConnector(bigquery_config)

    def test_init(self, bigquery_config):
        """Test BigQuery connector initialization."""
        connector = BigQueryConnector(bigquery_config)
        
        assert connector.project_id == "test-project"
        assert connector.dataset_id == "test_dataset"
        assert connector.location == "US"
        assert connector.client is None

    def test_init_with_credentials_path(self):
        """Test initialization with credentials file path."""
        config = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "credentials_path": "/path/to/credentials.json"
        }
        
        connector = BigQueryConnector(config)
        assert connector.credentials_path == "/path/to/credentials.json"

    def test_init_with_database_alias(self):
        """Test initialization with database field as alias for dataset_id."""
        config = {
            "type": "bigquery",
            "project_id": "test-project",
            "database": "test_dataset_alias"
        }
        
        connector = BigQueryConnector(config)
        assert connector.dataset_id == "test_dataset_alias"

    @pytest.mark.asyncio
    @patch('cartridge.scanner.bigquery.bigquery.Client')
    @patch('cartridge.scanner.bigquery.service_account.Credentials.from_service_account_info')
    async def test_connect_with_service_account_json(self, mock_credentials, mock_client, connector):
        """Test connection with service account JSON."""
        mock_creds = MagicMock()
        mock_credentials.return_value = mock_creds
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        await connector.connect()
        
        mock_credentials.assert_called_once()
        mock_client.assert_called_once_with(
            project="test-project",
            credentials=mock_creds,
            location="US"
        )
        assert connector.client == mock_client_instance

    @pytest.mark.asyncio
    @patch('cartridge.scanner.bigquery.bigquery.Client')
    @patch('cartridge.scanner.bigquery.service_account.Credentials.from_service_account_file')
    async def test_connect_with_credentials_file(self, mock_credentials, mock_client):
        """Test connection with service account file."""
        config = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "credentials_path": "/path/to/credentials.json"
        }
        
        connector = BigQueryConnector(config)
        mock_creds = MagicMock()
        mock_credentials.return_value = mock_creds
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        await connector.connect()
        
        mock_credentials.assert_called_once_with("/path/to/credentials.json")
        mock_client.assert_called_once_with(
            project="test-project",
            credentials=mock_creds,
            location="US"
        )

    @pytest.mark.asyncio
    @patch('cartridge.scanner.bigquery.bigquery.Client')
    @patch('cartridge.scanner.bigquery.default')
    async def test_connect_with_default_credentials(self, mock_default, mock_client):
        """Test connection with default credentials (ADC)."""
        config = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset"
        }
        
        connector = BigQueryConnector(config)
        mock_creds = MagicMock()
        mock_default.return_value = (mock_creds, "default-project")
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        await connector.connect()
        
        mock_default.assert_called_once()
        mock_client.assert_called_once_with(
            project="test-project",
            credentials=mock_creds,
            location="US"
        )

    @pytest.mark.asyncio
    async def test_disconnect(self, connector):
        """Test disconnection."""
        mock_client = MagicMock()
        connector.client = mock_client
        
        await connector.disconnect()
        
        mock_client.close.assert_called_once()
        assert connector.client is None

    @pytest.mark.asyncio
    @patch('cartridge.scanner.bigquery.bigquery.Client')
    async def test_test_connection_success(self, mock_client, connector):
        """Test successful connection test."""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        mock_client_instance.list_datasets.return_value = [MagicMock()]
        
        with patch.object(connector, 'connect', new_callable=AsyncMock):
            with patch.object(connector, 'disconnect', new_callable=AsyncMock):
                result = await connector.test_connection()
        
        assert result["status"] == "success"
        assert result["project_id"] == "test-project"
        assert "Successfully connected" in result["message"]

    @pytest.mark.asyncio
    async def test_test_connection_failure(self, connector):
        """Test connection test failure."""
        with patch.object(connector, 'connect', side_effect=Exception("Connection failed")):
            with patch.object(connector, 'disconnect', new_callable=AsyncMock):
                result = await connector.test_connection()
        
        assert result["status"] == "failed"
        assert "Connection failed" in result["error"]

    @pytest.mark.asyncio
    async def test_get_database_info(self, connector):
        """Test getting database information."""
        mock_client = MagicMock()
        connector.client = mock_client
        
        # Mock dataset
        mock_dataset = MagicMock()
        mock_client.dataset.return_value = mock_dataset
        mock_client.get_dataset.return_value = mock_dataset
        
        # Mock tables
        mock_table1 = MagicMock()
        mock_table1.table_type = "TABLE"
        mock_table2 = MagicMock()
        mock_table2.table_type = "VIEW"
        mock_table3 = MagicMock()
        mock_table3.table_type = "TABLE"
        
        mock_client.list_tables.return_value = [mock_table1, mock_table2, mock_table3]
        
        result = await connector.get_database_info()
        
        assert isinstance(result, DatabaseInfo)
        assert result.database_type == "bigquery"
        assert result.database_name == "test_dataset"
        assert result.schema_name == "test_dataset"
        assert result.total_tables == 2
        assert result.total_views == 1

    @pytest.mark.asyncio
    async def test_get_tables(self, connector):
        """Test getting list of tables."""
        mock_client = MagicMock()
        connector.client = mock_client
        
        mock_dataset = MagicMock()
        mock_client.dataset.return_value = mock_dataset
        
        # Mock tables
        mock_table1 = MagicMock()
        mock_table1.table_id = "table1"
        mock_table2 = MagicMock()
        mock_table2.table_id = "table2"
        
        mock_client.list_tables.return_value = [mock_table1, mock_table2]
        
        tables = await connector.get_tables()
        
        assert tables == ["table1", "table2"]

    @pytest.mark.asyncio
    async def test_get_table_info(self, connector):
        """Test getting detailed table information."""
        mock_client = MagicMock()
        connector.client = mock_client
        
        # Mock table
        mock_table = MagicMock()
        mock_table.table_type = "TABLE"
        mock_table.num_rows = 1000
        mock_table.num_bytes = 50000
        mock_table.description = "Test table"
        mock_table.schema = []  # Will be mocked in _get_columns
        mock_table.time_partitioning = None
        mock_table.range_partitioning = None
        mock_table.clustering_fields = None
        
        mock_client.dataset.return_value.table.return_value = mock_table
        mock_client.get_table.return_value = mock_table
        
        # Mock the internal methods
        with patch.object(connector, '_get_columns', new_callable=AsyncMock) as mock_get_cols:
            with patch.object(connector, '_get_constraints', new_callable=AsyncMock) as mock_get_constraints:
                with patch.object(connector, '_get_partitioning_info', new_callable=AsyncMock) as mock_get_indexes:
                    mock_get_cols.return_value = []
                    mock_get_constraints.return_value = []
                    mock_get_indexes.return_value = []
                    
                    result = await connector.get_table_info("test_table")
        
        assert isinstance(result, TableInfo)
        assert result.name == "test_table"
        assert result.schema == "test_dataset"
        assert result.table_type == "table"
        assert result.row_count == 1000
        assert result.size_bytes == 50000
        assert result.comment == "Test table"

    def test_normalize_data_type(self, connector):
        """Test data type normalization."""
        # Test numeric types
        assert connector.normalize_data_type("INTEGER") == DataType.BIGINT
        assert connector.normalize_data_type("INT64") == DataType.BIGINT
        assert connector.normalize_data_type("FLOAT") == DataType.FLOAT
        assert connector.normalize_data_type("FLOAT64") == DataType.DOUBLE
        assert connector.normalize_data_type("NUMERIC") == DataType.NUMERIC
        assert connector.normalize_data_type("DECIMAL") == DataType.DECIMAL
        
        # Test string types
        assert connector.normalize_data_type("STRING") == DataType.TEXT
        assert connector.normalize_data_type("BYTES") == DataType.BINARY
        
        # Test date/time types
        assert connector.normalize_data_type("DATE") == DataType.DATE
        assert connector.normalize_data_type("TIME") == DataType.TIME
        assert connector.normalize_data_type("DATETIME") == DataType.TIMESTAMP
        assert connector.normalize_data_type("TIMESTAMP") == DataType.TIMESTAMPTZ
        
        # Test boolean
        assert connector.normalize_data_type("BOOLEAN") == DataType.BOOLEAN
        assert connector.normalize_data_type("BOOL") == DataType.BOOLEAN
        
        # Test complex types
        assert connector.normalize_data_type("JSON") == DataType.JSON
        assert connector.normalize_data_type("RECORD") == DataType.JSON
        assert connector.normalize_data_type("STRUCT") == DataType.JSON
        assert connector.normalize_data_type("ARRAY") == DataType.ARRAY
        
        # Test geographic
        assert connector.normalize_data_type("GEOGRAPHY") == DataType.TEXT
        
        # Test unknown
        assert connector.normalize_data_type("UNKNOWN_TYPE") == DataType.UNKNOWN

    def test_process_field_simple(self, connector):
        """Test processing simple BigQuery schema field."""
        from google.cloud import bigquery
        
        field = bigquery.SchemaField("test_column", "STRING", mode="NULLABLE", description="Test column")
        
        result = connector._process_field(field)
        
        assert isinstance(result, ColumnInfo)
        assert result.name == "test_column"
        assert result.data_type == DataType.TEXT
        assert result.raw_type == "STRING"
        assert result.nullable is True
        assert result.comment == "Test column"

    def test_process_field_required(self, connector):
        """Test processing required BigQuery schema field."""
        from google.cloud import bigquery
        
        field = bigquery.SchemaField("id", "INTEGER", mode="REQUIRED")
        
        result = connector._process_field(field)
        
        assert result.nullable is False

    def test_process_field_repeated(self, connector):
        """Test processing repeated (array) BigQuery schema field."""
        from google.cloud import bigquery
        
        field = bigquery.SchemaField("tags", "STRING", mode="REPEATED")
        
        result = connector._process_field(field)
        
        assert result.data_type == DataType.ARRAY
        assert result.raw_type == "ARRAY<STRING>"

    def test_process_field_record(self, connector):
        """Test processing RECORD (struct) BigQuery schema field."""
        from google.cloud import bigquery
        
        # Create nested fields for the struct
        nested_field1 = bigquery.SchemaField("name", "STRING")
        nested_field2 = bigquery.SchemaField("age", "INTEGER")
        
        field = bigquery.SchemaField("person", "RECORD", fields=[nested_field1, nested_field2])
        
        result = connector._process_field(field)
        
        assert result.data_type == DataType.JSON
        assert "STRUCT<" in result.raw_type
        assert "name: STRING" in result.raw_type
        assert "age: INTEGER" in result.raw_type

    @pytest.mark.asyncio
    async def test_get_sample_data(self, connector):
        """Test getting sample data from a table."""
        mock_client = MagicMock()
        connector.client = mock_client
        
        # Mock query job and results
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        
        # Mock row data
        mock_row1 = {"id": 1, "name": "John", "created_at": datetime(2023, 1, 1)}
        mock_row2 = {"id": 2, "name": "Jane", "created_at": datetime(2023, 1, 2)}
        
        mock_query_job.result.return_value = [
            MagicMock(items=lambda: mock_row1.items()),
            MagicMock(items=lambda: mock_row2.items())
        ]
        
        result = await connector.get_sample_data("test_table", limit=2)
        
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "John"
        assert result[1]["id"] == 2
        assert result[1]["name"] == "Jane"

    def test_serialize_value(self, connector):
        """Test value serialization for JSON compatibility."""
        # Test simple types
        assert connector._serialize_value(None) is None
        assert connector._serialize_value("string") == "string"
        assert connector._serialize_value(123) == 123
        assert connector._serialize_value(45.67) == 45.67
        assert connector._serialize_value(True) is True
        
        # Test datetime
        dt = datetime(2023, 1, 1, 12, 0, 0)
        assert connector._serialize_value(dt) == "2023-01-01T12:00:00"
        
        # Test list
        assert connector._serialize_value([1, "two", None]) == [1, "two", None]
        
        # Test dict
        assert connector._serialize_value({"key": "value"}) == {"key": "value"}
        
        # Test other types (converted to string)
        class CustomObject:
            def __str__(self):
                return "custom"
        
        assert connector._serialize_value(CustomObject()) == "custom"

    @pytest.mark.asyncio
    async def test_get_partitioning_info_time_partition(self, connector):
        """Test getting partitioning information for time-partitioned table."""
        from google.cloud import bigquery
        
        mock_table = MagicMock()
        mock_table.time_partitioning = MagicMock()
        mock_table.time_partitioning.field = "created_at"
        mock_table.time_partitioning.type_ = "DAY"
        mock_table.range_partitioning = None
        mock_table.clustering_fields = None
        
        result = await connector._get_partitioning_info(mock_table)
        
        assert len(result) == 1
        assert result[0].name == "partition_created_at"
        assert result[0].columns == ["created_at"]
        assert result[0].type == "time_partition"

    @pytest.mark.asyncio
    async def test_get_partitioning_info_clustering(self, connector):
        """Test getting clustering information."""
        mock_table = MagicMock()
        mock_table.time_partitioning = None
        mock_table.range_partitioning = None
        mock_table.clustering_fields = ["user_id", "created_date"]
        
        result = await connector._get_partitioning_info(mock_table)
        
        assert len(result) == 1
        assert result[0].name == "clustering"
        assert result[0].columns == ["user_id", "created_date"]
        assert result[0].type == "clustering"

    @pytest.mark.asyncio
    async def test_error_handling_connect(self, connector):
        """Test error handling during connection."""
        with patch('cartridge.scanner.bigquery.service_account.Credentials.from_service_account_info', 
                   side_effect=Exception("Invalid credentials")):
            with pytest.raises(Exception, match="Invalid credentials"):
                await connector.connect()

    @pytest.mark.asyncio
    async def test_error_handling_get_tables(self, connector):
        """Test error handling when getting tables."""
        mock_client = MagicMock()
        connector.client = mock_client
        mock_client.dataset.side_effect = Exception("Dataset not found")
        
        with pytest.raises(Exception, match="Dataset not found"):
            await connector.get_tables()

    @pytest.mark.asyncio
    async def test_error_handling_get_sample_data(self, connector):
        """Test error handling when getting sample data."""
        mock_client = MagicMock()
        connector.client = mock_client
        mock_client.query.side_effect = Exception("Query failed")
        
        with pytest.raises(Exception, match="Query failed"):
            await connector.get_sample_data("test_table")
