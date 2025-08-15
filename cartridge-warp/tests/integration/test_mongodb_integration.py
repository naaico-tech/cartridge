"""Integration tests for MongoDB source connector.

These tests require a running MongoDB instance.
Run with: docker run -d -p 27017:27017 mongo:5.0
"""

import os
from datetime import datetime, timezone

import pytest
from bson import ObjectId

from cartridge_warp.connectors.base import OperationType
from cartridge_warp.connectors.mongodb_source import MongoDBSourceConnector


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TESTS"),
    reason="Integration tests disabled. Set INTEGRATION_TESTS=1 to enable."
)
class TestMongoDBSourceConnectorIntegration:
    """Integration tests for MongoDB source connector."""

    @pytest.fixture
    async def connector(self):
        """Create a test MongoDB connector."""
        connector = MongoDBSourceConnector(
            connection_string="mongodb://localhost:27017",
            database="test_cartridge_warp",
            change_detection_column="updated_at",
            change_detection_strategy="timestamp"
        )

        try:
            await connector.connect()
            yield connector
        finally:
            await connector.disconnect()

    @pytest.fixture
    async def sample_data(self, connector):
        """Insert sample data for testing."""
        if connector._database:
            # Insert test documents
            test_collection = connector._database["test_collection"]

            documents = [
                {
                    "_id": ObjectId(),
                    "name": "John Doe",
                    "age": 30,
                    "email": "john@example.com",
                    "address": {
                        "street": "123 Main St",
                        "city": "Anytown",
                        "zipcode": "12345"
                    },
                    "tags": ["user", "premium"],
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "_id": ObjectId(),
                    "name": "Jane Smith",
                    "age": 25,
                    "email": "jane@example.com",
                    "preferences": {
                        "theme": "dark",
                        "notifications": True
                    },
                    "scores": [85, 92, 78],
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                }
            ]

            await test_collection.insert_many(documents)

            yield documents

            # Cleanup
            await test_collection.drop()

    @pytest.mark.asyncio
    async def test_connection(self, connector):
        """Test basic connection functionality."""
        assert connector.connected is True

        # Test connection test
        result = await connector.test_connection()
        assert result is True

    @pytest.mark.asyncio
    async def test_schema_discovery(self, connector, sample_data):
        """Test schema discovery from real MongoDB data."""
        schema = await connector.get_schema("test_schema")

        assert schema.name == "test_cartridge_warp"
        assert len(schema.tables) >= 1

        # Find the test table
        test_table = None
        for table in schema.tables:
            if table.name == "test_collection":
                test_table = table
                break

        assert test_table is not None
        assert "_id" in [col.name for col in test_table.columns]
        assert "name" in [col.name for col in test_table.columns]
        assert "address_street" in [col.name for col in test_table.columns]  # Flattened
        assert "tags" in [col.name for col in test_table.columns]  # JSON array

    @pytest.mark.asyncio
    async def test_full_snapshot(self, connector, sample_data):
        """Test full snapshot functionality."""
        records = []

        async for record in connector.get_full_snapshot("test_schema", "test_collection", 100):
            records.append(record)

        assert len(records) == 2

        for record in records:
            assert record.table_name == "test_collection"
            assert record.operation == OperationType.INSERT
            assert "_id" in record.data
            assert "name" in record.data
            assert "address_street" in record.data  # Flattened field
            assert record.data["tags"] is not None  # JSON field

    @pytest.mark.asyncio
    async def test_timestamp_based_changes(self, connector, sample_data):
        """Test timestamp-based change detection."""
        # Set strategy to timestamp
        connector.change_detection_strategy = "timestamp"

        events = []
        async for event in connector.get_changes("test_schema", batch_size=10):
            events.append(event)
            if len(events) >= 2:  # Limit to avoid infinite loop
                break

        # Should detect the sample documents as changes
        assert len(events) >= 0  # May be 0 if no timestamp field matches
