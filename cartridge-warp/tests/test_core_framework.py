"""Test the core framework components for cartridge-warp."""

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Optional

import pytest

from cartridge_warp.connectors.base import (
    BaseDestinationConnector,
    BaseSourceConnector,
    ChangeEvent,
    ColumnDefinition,
    ColumnType,
    DatabaseSchema,
    OperationType,
    Record,
    SchemaChange,
    TableSchema,
)
from cartridge_warp.connectors.factory import (
    ConnectorFactory,
    register_destination_connector,
    register_source_connector,
)
from cartridge_warp.core.config import (
    DestinationConfig,
    SchemaConfig,
    SourceConfig,
    TableConfig,
    WarpConfig,
)


# Mock connectors for testing
@register_source_connector("test_source")
class MockSourceConnector(BaseSourceConnector):
    """Test source connector implementation."""

    def __init__(self, connection_string: str, **kwargs):
        super().__init__(connection_string, **kwargs)
        self._connected = False
        self._test_data = []

    async def connect(self) -> None:
        """Connect to test source."""
        self._connected = True

    async def disconnect(self) -> None:
        """Disconnect from test source."""
        self._connected = False

    async def get_schema(self, schema_name: str) -> DatabaseSchema:
        """Get test schema."""
        return DatabaseSchema(
            name=schema_name,
            tables=[
                TableSchema(
                    name="test_table",
                    columns=[
                        ColumnDefinition("id", ColumnType.INTEGER, False),
                        ColumnDefinition("name", ColumnType.STRING, True),
                        ColumnDefinition("created_at", ColumnType.TIMESTAMP, True),
                    ],
                    primary_keys=["id"],
                )
            ],
        )

    async def get_changes(
        self, schema_name: str, marker: Optional[Any] = None, batch_size: int = 1000
    ) -> AsyncIterator[ChangeEvent]:
        """Get test changes."""

        # Return a simple test change
        async def _generate_changes():
            yield ChangeEvent(
                record=Record(
                    table_name="test_table",
                    data={"id": 1, "name": "test_record", "created_at": datetime.now()},
                    operation=OperationType.INSERT,
                    timestamp=datetime.now(),
                    primary_key_values={"id": 1},
                ),
                position_marker="test_marker_1",
                schema_name=schema_name,
            )

        return _generate_changes()

    async def get_full_snapshot(
        self, schema_name: str, table_name: str, batch_size: int = 10000
    ) -> AsyncIterator[Record]:
        """Get test snapshot."""

        async def _generate_records():
            yield Record(
                table_name=table_name,
                data={"id": 1, "name": "snapshot_record", "created_at": datetime.now()},
                operation=OperationType.INSERT,
                timestamp=datetime.now(),
                primary_key_values={"id": 1},
            )

        return _generate_records()


@register_destination_connector("test_destination")
class MockDestinationConnector(BaseDestinationConnector):
    """Test destination connector implementation."""

    def __init__(self, connection_string: str, **kwargs):
        super().__init__(connection_string, **kwargs)
        self._connected = False
        self._written_records = []
        self._markers = {}
        self._schemas = set()
        self._tables = set()

    async def connect(self) -> None:
        """Connect to test destination."""
        self._connected = True

    async def disconnect(self) -> None:
        """Disconnect from test destination."""
        self._connected = False

    async def write_batch(self, schema_name: str, records: list[Record]) -> None:
        """Write test records."""
        self._written_records.extend(records)

    async def apply_schema_changes(
        self, schema_name: str, changes: list[SchemaChange]
    ) -> None:
        """Apply test schema changes."""
        # Mock implementation
        pass

    async def update_marker(
        self, schema_name: str, table_name: str, marker: Any
    ) -> None:
        """Update test marker."""
        self._markers[f"{schema_name}.{table_name}"] = marker

    async def get_marker(self, schema_name: str, table_name: str) -> Optional[Any]:
        """Get test marker."""
        return self._markers.get(f"{schema_name}.{table_name}")

    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        """Create test schema."""
        self._schemas.add(schema_name)

    async def create_table_if_not_exists(
        self, schema_name: str, table_schema: TableSchema
    ) -> None:
        """Create test table."""
        self._tables.add(f"{schema_name}.{table_schema.name}")


class TestCoreFramework:
    """Test the core framework components."""

    def test_connector_registration(self):
        """Test connector registration system."""
        factory = ConnectorFactory()

        # Check that our test connectors are registered
        available = factory.list_available_connectors()
        assert "test_source" in available["source"]
        assert "test_destination" in available["destination"]

    @pytest.mark.asyncio
    async def test_source_connector_creation(self):
        """Test source connector creation."""
        factory = ConnectorFactory()

        config = SourceConfig(type="test_source", connection_string="test://connection")

        connector = await factory.create_source_connector(config)
        assert isinstance(connector, MockSourceConnector)

        # Test connection methods
        await connector.connect()
        assert await connector.test_connection()
        await connector.disconnect()

    @pytest.mark.asyncio
    async def test_destination_connector_creation(self):
        """Test destination connector creation."""
        factory = ConnectorFactory()

        config = DestinationConfig(
            type="test_destination", connection_string="test://destination"
        )

        connector = await factory.create_destination_connector(config)
        assert isinstance(connector, MockDestinationConnector)

        # Test connection methods
        await connector.connect()
        assert await connector.test_connection()
        await connector.disconnect()

    @pytest.mark.asyncio
    async def test_source_connector_interface(self):
        """Test source connector interface implementation."""
        factory = ConnectorFactory()

        config = SourceConfig(type="test_source", connection_string="test://connection")

        connector = await factory.create_source_connector(config)

        # Test get_schema
        schema = await connector.get_schema("test_schema")
        assert schema.name == "test_schema"
        assert len(schema.tables) == 1
        assert schema.tables[0].name == "test_table"

        # Test get_changes
        changes_iter = await connector.get_changes("test_schema")
        changes = []
        async for change in changes_iter:
            changes.append(change)
            break  # Just get first change

        assert len(changes) == 1
        assert changes[0].schema_name == "test_schema"
        assert changes[0].record.table_name == "test_table"

        # Test get_full_snapshot
        snapshot_iter = await connector.get_full_snapshot("test_schema", "test_table")
        records = []
        async for record in snapshot_iter:
            records.append(record)
            break  # Just get first record

        assert len(records) == 1
        assert records[0].table_name == "test_table"

    @pytest.mark.asyncio
    async def test_destination_connector_interface(self):
        """Test destination connector interface implementation."""
        factory = ConnectorFactory()

        config = DestinationConfig(
            type="test_destination", connection_string="test://destination"
        )

        connector = await factory.create_destination_connector(config)

        # Test schema creation
        await connector.create_schema_if_not_exists("test_schema")
        assert "test_schema" in connector._schemas

        # Test table creation
        table_schema = TableSchema(
            name="test_table",
            columns=[ColumnDefinition("id", ColumnType.INTEGER, False)],
            primary_keys=["id"],
        )
        await connector.create_table_if_not_exists("test_schema", table_schema)
        assert "test_schema.test_table" in connector._tables

        # Test record writing
        test_record = Record(
            table_name="test_table",
            data={"id": 1, "name": "test"},
            operation=OperationType.INSERT,
            timestamp=datetime.now(),
            primary_key_values={"id": 1},
        )
        await connector.write_batch("test_schema", [test_record])
        assert len(connector._written_records) == 1

        # Test marker management
        await connector.update_marker("test_schema", "test_table", "marker_123")
        marker = await connector.get_marker("test_schema", "test_table")
        assert marker == "marker_123"

    def test_configuration_validation(self):
        """Test configuration validation."""
        # Test basic valid config
        config = WarpConfig(
            mode="single",
            single_schema_name="test_schema",
            source=SourceConfig(type="test_source", connection_string="test://source"),
            destination=DestinationConfig(
                type="test_destination", connection_string="test://destination"
            ),
            schemas=[
                SchemaConfig(
                    name="test_schema", tables=[TableConfig(name="test_table")]
                )
            ],
        )

        assert config.mode == "single"
        assert config.single_schema_name == "test_schema"
        assert len(config.schemas) == 1

        # Test schema lookup
        schema_config = config.get_schema_config("test_schema")
        assert schema_config is not None
        assert schema_config.name == "test_schema"

        # Test table lookup
        table_config = config.get_table_config("test_schema", "test_table")
        assert table_config is not None
        assert table_config.name == "test_table"

    def test_configuration_from_dict(self):
        """Test configuration from dictionary."""
        config_dict = {
            "mode": "multi",
            "source": {"type": "test_source", "connection_string": "test://source"},
            "destination": {
                "type": "test_destination",
                "connection_string": "test://destination",
            },
            "schemas": [
                {
                    "name": "schema1",
                    "mode": "stream",
                    "tables": [{"name": "table1", "stream_batch_size": 2000}],
                }
            ],
        }

        config = WarpConfig(**config_dict)
        assert config.mode == "multi"
        assert len(config.schemas) == 1
        assert config.schemas[0].name == "schema1"
        assert config.schemas[0].tables[0].stream_batch_size == 2000


if __name__ == "__main__":
    pytest.main([__file__])
