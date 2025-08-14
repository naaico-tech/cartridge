"""Integration tests for BigQuery connector."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio

from cartridge.scanner.factory import ConnectorFactory
from cartridge.scanner.bigquery import BigQueryConnector


class TestBigQueryIntegration:
    """Integration tests for BigQuery connector."""

    def test_factory_creates_bigquery_connector(self):
        """Test that factory creates BigQuery connector."""
        config = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset"
        }
        
        connector = ConnectorFactory.create_connector("bigquery", config)
        
        assert isinstance(connector, BigQueryConnector)
        assert connector.project_id == "test-project"
        assert connector.dataset_id == "test_dataset"

    def test_bigquery_in_supported_databases(self):
        """Test that BigQuery is listed in supported databases."""
        supported = ConnectorFactory.get_supported_databases()
        
        assert "bigquery" in supported

    @pytest.mark.asyncio
    async def test_end_to_end_schema_scan_mock(self):
        """Test end-to-end schema scanning with mocked BigQuery client."""
        config = {
            "type": "bigquery", 
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "location": "US"
        }
        
        connector = BigQueryConnector(config)
        
        # Mock the BigQuery client and its responses
        with patch('cartridge.scanner.bigquery.bigquery.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            # Mock dataset
            mock_dataset = MagicMock()
            mock_client.dataset.return_value = mock_dataset
            mock_client.get_dataset.return_value = mock_dataset
            
            # Mock table list
            mock_table_ref1 = MagicMock()
            mock_table_ref1.table_id = "customers"
            mock_table_ref1.table_type = "TABLE"
            
            mock_table_ref2 = MagicMock()
            mock_table_ref2.table_id = "orders"
            mock_table_ref2.table_type = "TABLE"
            
            mock_client.list_tables.return_value = [mock_table_ref1, mock_table_ref2]
            
            # Mock individual table details
            mock_table1 = MagicMock()
            mock_table1.table_type = "TABLE"
            mock_table1.num_rows = 1000
            mock_table1.num_bytes = 50000
            mock_table1.description = "Customer data"
            mock_table1.time_partitioning = None
            mock_table1.range_partitioning = None
            mock_table1.clustering_fields = None
            
            # Mock schema fields
            from google.cloud import bigquery
            mock_table1.schema = [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED", description="Customer ID"),
                bigquery.SchemaField("name", "STRING", mode="NULLABLE", description="Customer name"),
                bigquery.SchemaField("email", "STRING", mode="NULLABLE", description="Customer email"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="Creation timestamp")
            ]
            
            mock_table2 = MagicMock()
            mock_table2.table_type = "TABLE"
            mock_table2.num_rows = 5000
            mock_table2.num_bytes = 250000
            mock_table2.description = "Order data"
            mock_table2.time_partitioning = None
            mock_table2.range_partitioning = None
            mock_table2.clustering_fields = None
            mock_table2.schema = [
                bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED", description="Order ID"),
                bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED", description="Customer ID"),
                bigquery.SchemaField("amount", "NUMERIC", mode="REQUIRED", description="Order amount"),
                bigquery.SchemaField("order_date", "DATE", mode="REQUIRED", description="Order date")
            ]
            
            def get_table_side_effect(table_ref):
                if table_ref.table_id == "customers":
                    return mock_table1
                elif table_ref.table_id == "orders":
                    return mock_table2
                return MagicMock()
            
            mock_client.get_table.side_effect = get_table_side_effect
            
            # Mock sample data query
            mock_query_job = MagicMock()
            mock_client.query.return_value = mock_query_job
            mock_query_job.result.return_value = [
                MagicMock(items=lambda: {"id": 1, "name": "John Doe", "email": "john@example.com"}.items()),
                MagicMock(items=lambda: {"id": 2, "name": "Jane Smith", "email": "jane@example.com"}.items())
            ]
            
            # Perform the schema scan
            scan_result = await connector.scan_schema(include_sample_data=True, sample_size=10)
            
            # Verify the results
            assert scan_result.database_info.database_type == "bigquery"
            assert scan_result.database_info.database_name == "test_dataset"
            assert len(scan_result.tables) == 2
            
            # Check customers table
            customers_table = next(t for t in scan_result.tables if t.name == "customers")
            assert customers_table.row_count == 1000
            assert customers_table.size_bytes == 50000
            assert customers_table.comment == "Customer data"
            assert len(customers_table.columns) == 4
            
            # Check specific columns
            id_column = next(c for c in customers_table.columns if c.name == "id")
            assert id_column.nullable is False
            assert id_column.comment == "Customer ID"
            
            name_column = next(c for c in customers_table.columns if c.name == "name")
            assert name_column.nullable is True
            assert name_column.comment == "Customer name"
            
            # Check orders table
            orders_table = next(t for t in scan_result.tables if t.name == "orders")
            assert orders_table.row_count == 5000
            assert orders_table.size_bytes == 250000
            assert len(orders_table.columns) == 4

    @pytest.mark.asyncio
    async def test_partitioned_table_scan(self):
        """Test scanning a partitioned table."""
        config = {
            "type": "bigquery",
            "project_id": "test-project", 
            "dataset_id": "test_dataset"
        }
        
        connector = BigQueryConnector(config)
        
        with patch('cartridge.scanner.bigquery.bigquery.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            # Mock partitioned table
            mock_table = MagicMock()
            mock_table.table_type = "TABLE"
            mock_table.num_rows = 1000000
            mock_table.description = "Partitioned events table"
            mock_table.schema = []
            
            # Mock time partitioning
            mock_table.time_partitioning = MagicMock()
            mock_table.time_partitioning.field = "event_date"
            mock_table.time_partitioning.type_ = "DAY"
            mock_table.range_partitioning = None
            
            # Mock clustering
            mock_table.clustering_fields = ["user_id", "event_type"]
            
            mock_client.dataset.return_value.table.return_value = mock_table
            mock_client.get_table.return_value = mock_table
            
            # Test partitioning info extraction
            table_info = await connector.get_table_info("events")
            
            # Should have both partitioning and clustering indexes
            assert len(table_info.indexes) == 2
            
            # Check partitioning index
            partition_index = next(idx for idx in table_info.indexes if idx.type == "time_partition")
            assert partition_index.name == "partition_event_date"
            assert partition_index.columns == ["event_date"]
            
            # Check clustering index
            cluster_index = next(idx for idx in table_info.indexes if idx.type == "clustering")
            assert cluster_index.name == "clustering"
            assert cluster_index.columns == ["user_id", "event_type"]

    @pytest.mark.asyncio
    async def test_nested_struct_handling(self):
        """Test handling of nested STRUCT fields."""
        config = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset"
        }
        
        connector = BigQueryConnector(config)
        
        with patch('cartridge.scanner.bigquery.bigquery.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            
            # Mock table with nested STRUCT
            mock_table = MagicMock()
            mock_table.table_type = "TABLE"
            mock_table.description = "Table with nested data"
            mock_table.time_partitioning = None
            mock_table.range_partitioning = None
            mock_table.clustering_fields = None
            
            # Create nested STRUCT schema
            from google.cloud import bigquery
            
            # Nested fields for address struct
            address_fields = [
                bigquery.SchemaField("street", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("zipcode", "STRING")
            ]
            
            mock_table.schema = [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("address", "RECORD", fields=address_fields),
                bigquery.SchemaField("tags", "STRING", mode="REPEATED")  # Array field
            ]
            
            mock_client.dataset.return_value.table.return_value = mock_table
            mock_client.get_table.return_value = mock_table
            
            table_info = await connector.get_table_info("users")
            
            # Check struct field
            address_column = next(c for c in table_info.columns if c.name == "address")
            assert address_column.data_type.value == "json"  # STRUCT mapped to JSON
            assert "STRUCT<" in address_column.raw_type
            assert "street: STRING" in address_column.raw_type
            
            # Check array field
            tags_column = next(c for c in table_info.columns if c.name == "tags")
            assert tags_column.data_type.value == "array"
            assert tags_column.raw_type == "ARRAY<STRING>"

    @pytest.mark.asyncio
    async def test_connection_error_handling(self):
        """Test connection error handling scenarios."""
        config = {
            "type": "bigquery",
            "project_id": "nonexistent-project",
            "dataset_id": "test_dataset"
        }
        
        connector = BigQueryConnector(config)
        
        # Test with authentication error
        with patch('cartridge.scanner.bigquery.service_account.Credentials.from_service_account_info',
                   side_effect=Exception("Invalid credentials format")):
            result = await connector.test_connection()
            assert result["status"] == "failed"
            assert "Invalid credentials format" in result["error"]
        
        # Test with project not found
        from google.api_core import exceptions as google_exceptions
        
        with patch.object(connector, 'connect', new_callable=AsyncMock):
            with patch('cartridge.scanner.bigquery.bigquery.Client') as mock_client_class:
                mock_client = MagicMock()
                mock_client_class.return_value = mock_client
                mock_client.list_datasets.side_effect = google_exceptions.NotFound("Project not found")
                
                result = await connector.test_connection()
                assert result["status"] == "failed"
                assert "not found" in result["message"]

    def test_different_authentication_methods(self):
        """Test different authentication method configurations."""
        # Service account JSON string
        config1 = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "credentials_json": '{"type": "service_account"}'
        }
        connector1 = BigQueryConnector(config1)
        assert connector1.credentials_json == '{"type": "service_account"}'
        
        # Service account file path
        config2 = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset",
            "credentials_path": "/path/to/key.json"
        }
        connector2 = BigQueryConnector(config2)
        assert connector2.credentials_path == "/path/to/key.json"
        
        # Default credentials (ADC)
        config3 = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset"
        }
        connector3 = BigQueryConnector(config3)
        assert connector3.credentials_json is None
        assert connector3.credentials_path is None
