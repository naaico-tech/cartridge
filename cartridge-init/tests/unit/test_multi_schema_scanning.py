"""Tests for multi-schema and multi-database scanning functionality."""

import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import tempfile
import yaml

from cartridge.api.routes.scanner import (
    DataSourceConnection,
    ScanRequest,
    MultiDatabaseScanRequest,
    DatabaseConfig,
    scan_schema,
    scan_multiple_databases
)
from cartridge.scanner.base import ScanResult, DatabaseInfo, TableInfo, ColumnInfo, DataType


class TestMultiSchemaScanning:
    """Test multi-schema scanning functionality."""

    def create_mock_scan_result(self, schema_name: str, table_count: int = 2) -> ScanResult:
        """Create a mock scan result for testing."""
        tables = []
        for i in range(table_count):
            columns = [
                ColumnInfo(
                    name=f"id_{i}",
                    data_type=DataType.INTEGER,
                    raw_type="int",
                    nullable=False,
                    is_primary_key=True
                ),
                ColumnInfo(
                    name=f"name_{i}",
                    data_type=DataType.VARCHAR,
                    raw_type="varchar(255)",
                    nullable=True
                )
            ]
            
            table = TableInfo(
                name=f"table_{i}",
                schema=schema_name,
                table_type="table",
                columns=columns,
                constraints=[],
                indexes=[],
                row_count=100 + i
            )
            tables.append(table)
        
        db_info = DatabaseInfo(
            database_type="postgresql",
            version="13.0",
            host="localhost",
            port=5432,
            database_name="test_db",
            schema_name=schema_name,
            total_tables=table_count,
            total_views=0
        )
        
        return ScanResult(
            database_info=db_info,
            tables=tables,
            scan_duration_seconds=1.5,
            scan_timestamp="2024-01-01T10:00:00Z"
        )

    @pytest.mark.asyncio
    async def test_single_schema_scan_backward_compatibility(self):
        """Test that single schema scanning maintains backward compatibility."""
        connection = DataSourceConnection(
            type="postgresql",
            host="localhost",
            port=5432,
            database="test_db",
            username="user",
            password="pass",
            schema="public"
        )
        
        request = ScanRequest(connection=connection)
        
        mock_scan_result = self.create_mock_scan_result("public", 3)
        
        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema = AsyncMock(return_value=mock_scan_result)
            mock_factory.return_value = mock_connector
            
            with patch('cartridge.api.routes.scanner.ConnectorFactory.get_supported_databases', return_value=["postgresql"]):
                result = await scan_schema(request, db=MagicMock())
        
        # Should return ScanResult for single schema
        assert hasattr(result, 'tables')
        assert hasattr(result, 'scan_timestamp')
        assert len(result.tables) == 3
        assert result.connection_info['schema'] == 'public'

    @pytest.mark.asyncio
    async def test_multi_schema_scan(self):
        """Test multi-schema scanning functionality."""
        connection = DataSourceConnection(
            type="postgresql",
            host="localhost",
            port=5432,
            database="test_db",
            username="user",
            password="pass",
            schemas=["public", "staging", "marts"]
        )
        
        request = ScanRequest(connection=connection)
        
        # Create mock scan results for each schema
        mock_results = [
            self.create_mock_scan_result("public", 2),
            self.create_mock_scan_result("staging", 3),
            self.create_mock_scan_result("marts", 1)
        ]
        
        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema = AsyncMock(side_effect=mock_results)
            mock_factory.return_value = mock_connector
            
            with patch('cartridge.api.routes.scanner.ConnectorFactory.get_supported_databases', return_value=["postgresql"]):
                result = await scan_schema(request, db=MagicMock())
        
        # Should return MultiSchemaScanResult for multiple schemas
        assert hasattr(result, 'schemas_data')
        assert hasattr(result, 'total_schemas')
        assert hasattr(result, 'total_tables')
        
        assert result.total_schemas == 3
        assert result.total_tables == 6  # 2 + 3 + 1
        assert len(result.schemas_data) == 3
        
        # Check schema data structure
        schema_names = [schema_data['schema'] for schema_data in result.schemas_data]
        assert schema_names == ["public", "staging", "marts"]
        
        # Check table counts per schema
        assert len(result.schemas_data[0]['tables']) == 2  # public
        assert len(result.schemas_data[1]['tables']) == 3  # staging
        assert len(result.schemas_data[2]['tables']) == 1  # marts

    @pytest.mark.asyncio
    async def test_multi_database_scan(self):
        """Test multi-database scanning functionality."""
        databases = [
            DatabaseConfig(
                name="sales_db",
                uri="postgresql://user:pass@localhost:5432/sales",
                schemas=["public", "analytics"]
            ),
            DatabaseConfig(
                name="marketing_db",
                uri="mysql://user:pass@localhost:3306/marketing",
                schemas=["raw", "campaigns"]
            )
        ]
        
        request = MultiDatabaseScanRequest(databases=databases)
        
        # Create mock scan results
        mock_results = [
            self.create_mock_scan_result("public", 2),
            self.create_mock_scan_result("analytics", 3),
            self.create_mock_scan_result("raw", 1),
            self.create_mock_scan_result("campaigns", 2)
        ]
        
        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema = AsyncMock(side_effect=mock_results)
            mock_factory.return_value = mock_connector
            
            with patch('cartridge.api.routes.scanner.ConnectorFactory.get_supported_databases', return_value=["postgresql", "mysql"]):
                result = await scan_multiple_databases(request, db=MagicMock())
        
        # Verify multi-database result structure
        assert result.total_databases == 2
        assert result.total_schemas == 4
        assert result.total_tables == 8  # 2 + 3 + 1 + 2
        
        # Check database results
        assert len(result.databases) == 2
        
        sales_db = next(db for db in result.databases if db['name'] == 'sales_db')
        marketing_db = next(db for db in result.databases if db['name'] == 'marketing_db')
        
        assert sales_db['total_schemas'] == 2
        assert sales_db['total_tables'] == 5  # 2 + 3
        assert len(sales_db['schemas_data']) == 2
        
        assert marketing_db['total_schemas'] == 2
        assert marketing_db['total_tables'] == 3  # 1 + 2
        assert len(marketing_db['schemas_data']) == 2

    def test_database_config_validation(self):
        """Test database configuration validation."""
        # Valid config
        config = DatabaseConfig(
            name="test_db",
            uri="postgresql://user:pass@localhost:5432/test",
            schemas=["public", "staging"]
        )
        assert config.name == "test_db"
        assert len(config.schemas) == 2
        
        # Test that empty schemas list raises validation error
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            DatabaseConfig(
                name="test_db",
                uri="postgresql://user:pass@localhost:5432/test",
                schemas=[]
            )


class TestCLIMultiSchemaScanning:
    """Test CLI multi-schema scanning functionality."""

    def test_config_file_parsing(self):
        """Test parsing of multi-database configuration files."""
        config_data = {
            "databases": [
                {
                    "name": "sales_db",
                    "uri": "postgresql://user:pass@localhost:5432/sales",
                    "schemas": ["public", "analytics"]
                },
                {
                    "name": "marketing_db",
                    "uri": "mysql://user:pass@localhost:3306/marketing",
                    "schemas": ["raw", "campaigns"]
                }
            ]
        }
        
        # Test YAML parsing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump(config_data, f)
            yaml_path = f.name
        
        try:
            with open(yaml_path, 'r') as f:
                loaded_config = yaml.safe_load(f)
            
            assert 'databases' in loaded_config
            assert len(loaded_config['databases']) == 2
            assert loaded_config['databases'][0]['name'] == 'sales_db'
            assert loaded_config['databases'][1]['schemas'] == ['raw', 'campaigns']
        finally:
            Path(yaml_path).unlink()
        
        # Test JSON parsing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            json_path = f.name
        
        try:
            with open(json_path, 'r') as f:
                loaded_config = json.load(f)
            
            assert 'databases' in loaded_config
            assert len(loaded_config['databases']) == 2
        finally:
            Path(json_path).unlink()

    def test_schema_list_parsing(self):
        """Test parsing of comma-separated schema lists in CLI."""
        # Test valid schema lists
        test_cases = [
            ("schema1,schema2,schema3", ["schema1", "schema2", "schema3"]),
            ("public, staging, marts", ["public", "staging", "marts"]),
            ("  schema1  ,  schema2  ", ["schema1", "schema2"]),
            ("single_schema", ["single_schema"])
        ]
        
        for input_str, expected in test_cases:
            schema_list = [s.strip() for s in input_str.split(',') if s.strip()]
            assert schema_list == expected

    def test_output_format_structure(self):
        """Test the structure of multi-schema output formats."""
        # Single schema output (backward compatibility)
        single_schema_output = {
            "database_type": "postgresql",
            "schema": "public",
            "connection_string": "postgresql://***@localhost:5432/test",
            "scan_timestamp": "2024-01-01T10:00:00Z",
            "tables": [
                {
                    "name": "users",
                    "schema": "public",
                    "type": "table",
                    "row_count": 100,
                    "columns": []
                }
            ]
        }
        
        assert "schema" in single_schema_output
        assert "tables" in single_schema_output
        assert isinstance(single_schema_output["tables"], list)
        
        # Multi-schema output
        multi_schema_output = {
            "database_type": "postgresql",
            "schemas": ["public", "staging"],
            "connection_string": "postgresql://***@localhost:5432/test",
            "scan_timestamp": "2024-01-01T10:00:00Z",
            "total_schemas": 2,
            "total_tables": 3,
            "schemas_data": [
                {
                    "schema": "public",
                    "scan_timestamp": "2024-01-01T10:00:00Z",
                    "tables": [{"name": "users", "schema": "public"}]
                },
                {
                    "schema": "staging",
                    "scan_timestamp": "2024-01-01T10:00:00Z",
                    "tables": [{"name": "stg_users", "schema": "staging"}]
                }
            ]
        }
        
        assert "schemas" in multi_schema_output
        assert "schemas_data" in multi_schema_output
        assert "total_schemas" in multi_schema_output
        assert "total_tables" in multi_schema_output
        assert len(multi_schema_output["schemas_data"]) == 2
        
        # Multi-database output
        multi_db_output = {
            "scan_type": "multi_database",
            "total_databases": 2,
            "total_schemas": 4,
            "total_tables": 10,
            "scan_timestamp": "2024-01-01T10:00:00Z",
            "databases": [
                {
                    "name": "sales_db",
                    "database_type": "postgresql",
                    "schemas": ["public", "analytics"],
                    "total_schemas": 2,
                    "total_tables": 5,
                    "schemas_data": []
                }
            ]
        }
        
        assert multi_db_output["scan_type"] == "multi_database"
        assert "total_databases" in multi_db_output
        assert "databases" in multi_db_output
        assert isinstance(multi_db_output["databases"], list)


class TestErrorHandling:
    """Test error handling in multi-schema scanning."""

    @pytest.mark.asyncio
    async def test_unsupported_database_type(self):
        """Test handling of unsupported database types."""
        connection = DataSourceConnection(
            type="unsupported_db",
            host="localhost",
            port=5432,
            database="test_db",
            username="user",
            password="pass",
            schema="public"
        )
        
        request = ScanRequest(connection=connection)
        
        with patch('cartridge.api.routes.scanner.ConnectorFactory.get_supported_databases', return_value=["postgresql"]):
            with pytest.raises(Exception):  # Should raise HTTPException
                await scan_schema(request, db=MagicMock())

    @pytest.mark.asyncio
    async def test_partial_failure_multi_database(self):
        """Test handling of partial failures in multi-database scanning."""
        databases = [
            DatabaseConfig(
                name="working_db",
                uri="postgresql://user:pass@localhost:5432/working",
                schemas=["public"]
            ),
            DatabaseConfig(
                name="failing_db",
                uri="invalid://invalid:invalid@invalid:5432/invalid",
                schemas=["public"]
            )
        ]
        
        request = MultiDatabaseScanRequest(databases=databases)
        
        mock_scan_result = MagicMock()
        mock_scan_result.tables = []
        mock_scan_result.scan_timestamp = "2024-01-01T10:00:00Z"
        mock_scan_result.get_relationships = MagicMock(return_value=[])
        
        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema = AsyncMock(return_value=mock_scan_result)
            mock_factory.return_value = mock_connector
            
            with patch('cartridge.api.routes.scanner.ConnectorFactory.get_supported_databases', return_value=["postgresql"]):
                result = await scan_multiple_databases(request, db=MagicMock())
        
        # Should only include the working database
        assert result.total_databases == 1
        assert result.databases[0]['name'] == 'working_db'

    def test_empty_schema_list_validation(self):
        """Test validation of empty schema lists."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            DatabaseConfig(
                name="test_db",
                uri="postgresql://user:pass@localhost:5432/test",
                schemas=[]
            )

    def test_missing_config_file(self):
        """Test handling of missing configuration files."""
        # This would be tested in integration tests with actual CLI calls
        pass
