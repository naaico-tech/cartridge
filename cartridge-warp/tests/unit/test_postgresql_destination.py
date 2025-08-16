"""Unit tests for PostgreSQL destination connector."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cartridge_warp.connectors.base import (
    ColumnDefinition,
    ColumnType,
    OperationType,
    Record,
    SchemaChange,
    TableSchema,
)
from cartridge_warp.connectors.postgresql_destination import (
    PostgreSQLDestinationConnector,
    PostgreSQLTypeMapper,
)


class TestPostgreSQLTypeMapper:
    """Test type mapping functionality."""

    def test_get_postgresql_type_basic_types(self):
        """Test basic type mapping."""
        mapper = PostgreSQLTypeMapper()
        
        assert mapper.get_postgresql_type(ColumnType.STRING) == "TEXT"
        assert mapper.get_postgresql_type(ColumnType.INTEGER) == "INTEGER"
        assert mapper.get_postgresql_type(ColumnType.FLOAT) == "REAL"
        assert mapper.get_postgresql_type(ColumnType.BOOLEAN) == "BOOLEAN"
        assert mapper.get_postgresql_type(ColumnType.TIMESTAMP) == "TIMESTAMP WITH TIME ZONE"
        assert mapper.get_postgresql_type(ColumnType.JSON) == "JSONB"

    def test_get_postgresql_type_string_with_length(self):
        """Test string type with length mapping."""
        mapper = PostgreSQLTypeMapper()
        
        assert mapper.get_postgresql_type(ColumnType.STRING, 255) == "VARCHAR(255)"
        assert mapper.get_postgresql_type(ColumnType.STRING, 50) == "VARCHAR(50)"

    def test_convert_value_json(self):
        """Test JSON value conversion."""
        mapper = PostgreSQLTypeMapper()
        
        # Dict should be converted to JSON string
        data = {"key": "value", "number": 42}
        result = mapper.convert_value(data, ColumnType.JSON)
        assert result == json.dumps(data)
        
        # List should be converted to JSON string
        data = ["item1", "item2", 123]
        result = mapper.convert_value(data, ColumnType.JSON)
        assert result == json.dumps(data)

    def test_convert_value_timestamp(self):
        """Test timestamp conversion."""
        mapper = PostgreSQLTypeMapper()
        
        # ISO string should be parsed to datetime
        iso_string = "2024-01-15T10:30:00Z"
        result = mapper.convert_value(iso_string, ColumnType.TIMESTAMP)
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_convert_value_numeric(self):
        """Test numeric value conversion."""
        mapper = PostgreSQLTypeMapper()
        
        # String numbers should be converted
        assert mapper.convert_value("42", ColumnType.INTEGER) == 42
        assert mapper.convert_value("42.5", ColumnType.FLOAT) == 42.5

    def test_convert_value_boolean(self):
        """Test boolean value conversion."""
        mapper = PostgreSQLTypeMapper()
        
        # String booleans should be converted
        assert mapper.convert_value("true", ColumnType.BOOLEAN) is True
        assert mapper.convert_value("false", ColumnType.BOOLEAN) is False
        assert mapper.convert_value("1", ColumnType.BOOLEAN) is True
        assert mapper.convert_value("0", ColumnType.BOOLEAN) is False

    def test_convert_value_none(self):
        """Test None value handling."""
        mapper = PostgreSQLTypeMapper()
        
        # None should remain None regardless of type
        assert mapper.convert_value(None, ColumnType.STRING) is None
        assert mapper.convert_value(None, ColumnType.INTEGER) is None
        assert mapper.convert_value(None, ColumnType.JSON) is None


class TestPostgreSQLDestinationConnector:
    """Test PostgreSQL destination connector functionality."""

    @pytest.fixture
    def connector(self):
        """Create a test connector instance."""
        connection_string = "postgresql://test_user:test_pass@localhost:5432/test_db"
        return PostgreSQLDestinationConnector(
            connection_string=connection_string,
            batch_size=1000,
            min_connections=2,
            max_connections=5,
            enable_soft_deletes=True,
            deletion_strategy="hard",
            connection_timeout=30.0,
            command_timeout=60.0,
        )

    def test_initialization(self):
        """Test connector initialization with default and custom parameters."""
        # Test with default parameters
        connector = PostgreSQLDestinationConnector("postgresql://test")
        
        assert connector.connection_string == "postgresql://test"
        assert connector.metadata_schema == "cartridge_warp_metadata"
        assert connector.batch_size == 1000
        assert connector.max_connections == 10
        assert connector.min_connections == 2
        assert connector.enable_soft_deletes is True
        assert connector.deletion_strategy == "soft"
        assert connector.soft_delete_flag_column == "is_deleted"
        assert connector.soft_delete_timestamp_column == "deleted_at"
        assert not connector.connected
        assert connector.pool is None
        
        # Test with custom parameters
        connector_custom = PostgreSQLDestinationConnector(
            "postgresql://custom",
            batch_size=500,
            max_connections=20,
            enable_soft_deletes=False,
            deletion_strategy="hard",
            soft_delete_flag_column="archived",
            soft_delete_timestamp_column="archived_at"
        )
        
        assert connector_custom.batch_size == 500
        assert connector_custom.max_connections == 20
        assert connector_custom.enable_soft_deletes is False
        assert connector_custom.deletion_strategy == "hard"
        assert connector_custom.soft_delete_flag_column == "archived"
        assert connector_custom.soft_delete_timestamp_column == "archived_at"

    @pytest.mark.asyncio
    async def test_connect_success(self, connector):
        """Test successful connection."""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        
        # Properly setup async context manager for pool.acquire()
        mock_acquire = AsyncMock()
        mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire.__aexit__ = AsyncMock(return_value=False)
        
        # Make acquire return the async context manager directly, not a coroutine
        mock_pool.acquire = MagicMock(return_value=mock_acquire)
        
        with patch("cartridge_warp.connectors.postgresql_destination.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_pool
            
            await connector.connect()
            
            assert connector.connected is True
            assert connector.pool is mock_pool
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_failure(self, connector):
        """Test connection failure."""
        with patch("cartridge_warp.connectors.postgresql_destination.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.side_effect = Exception("Connection failed")
            
            # Connection failure should raise an exception
            with pytest.raises(Exception, match="Connection failed"):
                await connector.connect()
            
            # After failure, should still be disconnected
            assert connector.connected is False
            assert connector.pool is None

    @pytest.mark.asyncio
    async def test_disconnect(self, connector):
        """Test disconnection."""
        mock_pool = AsyncMock()
        connector.pool = mock_pool
        connector.connected = True
        
        await connector.disconnect()
        
        assert connector.connected is False
        assert connector.pool is None
        mock_pool.close.assert_called_once()

    def test_connection_string_property(self, connector):
        """Test connection string property access."""
        assert "postgresql://" in connector.connection_string
        assert "test_user" in connector.connection_string
        assert "localhost:5432" in connector.connection_string
        assert "test_db" in connector.connection_string
