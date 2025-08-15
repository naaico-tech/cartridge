"""Unit tests for MongoDB source connector."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from bson import ObjectId, Timestamp

from cartridge_warp.connectors.base import ColumnType, OperationType
from cartridge_warp.connectors.mongodb_source import (
    MongoDBSourceConnector,
    MongoDBTypeMapper,
)


class TestMongoDBTypeMapper:
    """Test MongoDB type mapping functionality."""

    def test_map_bson_type_primitives(self):
        """Test mapping of primitive BSON types."""
        assert MongoDBTypeMapper.map_bson_type(None) == ColumnType.STRING
        assert MongoDBTypeMapper.map_bson_type(True) == ColumnType.BOOLEAN
        assert MongoDBTypeMapper.map_bson_type(False) == ColumnType.BOOLEAN
        assert MongoDBTypeMapper.map_bson_type(42) == ColumnType.INTEGER
        assert MongoDBTypeMapper.map_bson_type(2147483648) == ColumnType.BIGINT  # > int32
        assert MongoDBTypeMapper.map_bson_type(3.14) == ColumnType.DOUBLE
        assert MongoDBTypeMapper.map_bson_type("hello") == ColumnType.STRING
        assert MongoDBTypeMapper.map_bson_type(b"bytes") == ColumnType.BINARY

    def test_map_bson_type_mongodb_specific(self):
        """Test mapping of MongoDB-specific types."""
        oid = ObjectId()
        timestamp = Timestamp(1234567890, 1)
        dt = datetime.now()

        assert MongoDBTypeMapper.map_bson_type(oid) == ColumnType.STRING
        assert MongoDBTypeMapper.map_bson_type(timestamp) == ColumnType.TIMESTAMP
        assert MongoDBTypeMapper.map_bson_type(dt) == ColumnType.TIMESTAMP

    def test_map_bson_type_complex(self):
        """Test mapping of complex types."""
        assert MongoDBTypeMapper.map_bson_type({"key": "value"}) == ColumnType.JSON
        assert MongoDBTypeMapper.map_bson_type([1, 2, 3]) == ColumnType.JSON
        assert MongoDBTypeMapper.map_bson_type(object()) == ColumnType.JSON

    def test_flatten_document_simple(self):
        """Test flattening simple documents."""
        doc = {"name": "John", "age": 30}
        flattened = MongoDBTypeMapper.flatten_document(doc)

        assert flattened == {"name": "John", "age": 30}

    def test_flatten_document_nested(self):
        """Test flattening nested documents."""
        doc = {
            "name": "John",
            "address": {
                "street": "123 Main St",
                "city": "Anytown"
            }
        }
        flattened = MongoDBTypeMapper.flatten_document(doc)

        expected = {
            "name": "John",
            "address_street": "123 Main St",
            "address_city": "Anytown"
        }
        assert flattened == expected

    def test_flatten_document_max_depth(self):
        """Test flattening with maximum depth limit."""
        doc = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": "deep"
                    }
                }
            }
        }
        flattened = MongoDBTypeMapper.flatten_document(doc, max_depth=2)

        expected = {
            "level1_level2": json.dumps({"level3": {"level4": "deep"}})
        }
        assert flattened == expected

    def test_flatten_document_arrays(self):
        """Test flattening documents with arrays."""
        doc = {
            "tags": ["python", "mongodb"],
            "scores": [85, 90, 78]
        }
        flattened = MongoDBTypeMapper.flatten_document(doc)

        expected = {
            "tags": json.dumps(["python", "mongodb"]),
            "scores": json.dumps([85, 90, 78])
        }
        assert flattened == expected

    def test_flatten_document_empty_values(self):
        """Test flattening with empty values."""
        doc = {
            "empty_dict": {},
            "empty_list": [],
            "null_value": None
        }
        flattened = MongoDBTypeMapper.flatten_document(doc)

        expected = {
            "empty_dict": None,
            "empty_list": None,
            "null_value": None
        }
        assert flattened == expected


class TestMongoDBSourceConnector:
    """Test MongoDB source connector functionality."""

    @pytest.fixture
    def connector(self):
        """Create MongoDB connector instance for testing."""
        return MongoDBSourceConnector(
            connection_string="mongodb://localhost:27017",
            database="test_db",
            change_detection_column="updated_at",
            change_detection_strategy="timestamp"
        )

    @pytest.fixture
    def mock_client(self):
        """Mock MongoDB client."""
        client = AsyncMock()
        client.server_info = AsyncMock(return_value={"version": "5.0.0"})
        client.admin.command = AsyncMock(return_value={"ok": 1})
        return client

    @pytest.fixture
    def mock_database(self):
        """Mock MongoDB database."""
        database = AsyncMock()
        database.list_collection_names = AsyncMock(return_value=["users", "orders"])
        return database

    @pytest.fixture
    def mock_collection(self):
        """Mock MongoDB collection."""
        collection = AsyncMock()
        collection.name = "test_collection"
        return collection

    @pytest.mark.asyncio
    async def test_connect_success(self, connector):
        """Test successful connection to MongoDB."""
        with patch("cartridge_warp.connectors.mongodb_source.AsyncIOMotorClient") as mock_motor:
            mock_client = AsyncMock()
            mock_client.admin.command = AsyncMock(return_value={"ok": 1})
            mock_client.server_info = AsyncMock(return_value={"version": "5.0.0"})
            mock_motor.return_value = mock_client

            await connector.connect()

            assert connector.connected is True
            assert connector._client is mock_client
            mock_client.admin.command.assert_called_once_with("ping")

    @pytest.mark.asyncio
    async def test_connect_failure(self, connector):
        """Test connection failure handling."""
        from pymongo.errors import ConnectionFailure

        with patch("cartridge_warp.connectors.mongodb_source.AsyncIOMotorClient") as mock_motor:
            mock_client = AsyncMock()
            mock_client.admin.command.side_effect = ConnectionFailure("Connection failed")
            mock_motor.return_value = mock_client

            with pytest.raises(ConnectionFailure):
                await connector.connect()

            assert connector.connected is False

    @pytest.mark.asyncio
    async def test_disconnect(self, connector, mock_client):
        """Test disconnection from MongoDB."""
        connector._client = mock_client
        connector.connected = True

        await connector.disconnect()

        assert connector.connected is False
        assert connector._client is None
        mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_schema(self, connector):
        """Test schema discovery."""
        # Setup mocks
        connector.connected = True

        # Mock database
        mock_database = AsyncMock()
        mock_database.list_collection_names = AsyncMock(return_value=["users", "orders"])
        connector._database = mock_database

        # Mock collection and documents
        sample_docs = [
            {"_id": ObjectId(), "name": "John", "age": 30, "active": True},
            {"_id": ObjectId(), "name": "Jane", "age": 25, "email": "jane@example.com"},
        ]

        # Create a mock collection with proper method chaining
        mock_collection = AsyncMock()
        mock_collection.name = "test_collection"

        # Mock the find().limit() chain - return the actual sample docs
        async def async_find_iterator():
            for doc in sample_docs:
                yield doc

        mock_find_cursor = AsyncMock()
        mock_find_cursor.__aiter__ = lambda: async_find_iterator()

        mock_limit_cursor = AsyncMock()
        mock_limit_cursor.return_value = mock_find_cursor

        mock_find_result = AsyncMock()
        mock_find_result.limit = mock_limit_cursor

        mock_collection.find.return_value = mock_find_result

        # Mock list_indexes
        async def async_index_iterator():
            yield {"name": "_id_", "key": {"_id": 1}, "unique": True}

        mock_indexes_cursor = AsyncMock()
        mock_indexes_cursor.__aiter__ = lambda: async_index_iterator()
        mock_collection.list_indexes.return_value = mock_indexes_cursor

        # Mock database collection access
        mock_database.__getitem__.return_value = mock_collection

        # Test schema discovery
        schema = await connector.get_schema("test_schema")

        assert schema.name == "test_db"
        assert len(schema.tables) == 2  # users and orders collections

        # Verify database method calls
        mock_database.list_collection_names.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_full_snapshot(self, connector):
        """Test full snapshot functionality."""
        # Setup mocks
        connector.connected = True

        # Mock database
        mock_database = AsyncMock()
        connector._database = mock_database

        sample_docs = [
            {"_id": ObjectId(), "name": "John", "age": 30},
            {"_id": ObjectId(), "name": "Jane", "age": 25},
        ]

        # Mock collection with proper method chaining
        mock_collection = AsyncMock()

        async def async_snapshot_iterator():
            for doc in sample_docs:
                yield doc

        mock_batch_cursor = AsyncMock()
        mock_batch_cursor.__aiter__ = lambda: async_snapshot_iterator()

        mock_find_result = AsyncMock()
        mock_find_result.batch_size.return_value = mock_batch_cursor
        mock_collection.find.return_value = mock_find_result

        mock_database.__getitem__.return_value = mock_collection

        # Test full snapshot
        records = []
        async for record in connector.get_full_snapshot("test_schema", "test_table", 100):
            records.append(record)

        assert len(records) == 2
        assert all(record.table_name == "test_table" for record in records)
        assert all(record.operation == OperationType.INSERT for record in records)

    def test_document_to_record(self, connector):
        """Test document to record conversion."""
        doc = {
            "_id": ObjectId(),
            "name": "John Doe",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "Anytown"
            },
            "tags": ["user", "premium"]
        }

        record = connector._document_to_record(doc, "users", OperationType.INSERT)

        assert record.table_name == "users"
        assert record.operation == OperationType.INSERT
        assert record.data["name"] == "John Doe"
        assert record.data["age"] == 30
        assert record.data["address_street"] == "123 Main St"
        assert record.data["address_city"] == "Anytown"
        assert record.data["tags"] == json.dumps(["user", "premium"])
        assert "_id" in record.primary_key_values

    def test_change_to_event_insert(self, connector):
        """Test converting MongoDB insert change to event."""
        change = {
            "_id": {"_data": "resume_token"},
            "operationType": "insert",
            "ns": {"db": "test_db", "coll": "users"},
            "fullDocument": {
                "_id": ObjectId(),
                "name": "John",
                "age": 30
            }
        }

        event = connector._change_to_event(change)

        assert event is not None
        assert event.record.operation == OperationType.INSERT
        assert event.record.table_name == "users"
        assert event.record.data["name"] == "John"
        assert event.position_marker == change["_id"]

    def test_change_to_event_update(self, connector):
        """Test converting MongoDB update change to event."""
        change = {
            "_id": {"_data": "resume_token"},
            "operationType": "update",
            "ns": {"db": "test_db", "coll": "users"},
            "fullDocument": {
                "_id": ObjectId(),
                "name": "John Updated",
                "age": 31
            },
            "updateDescription": {
                "updatedFields": {"name": "John Updated", "age": 31},
                "removedFields": []
            }
        }

        event = connector._change_to_event(change)

        assert event is not None
        assert event.record.operation == OperationType.UPDATE
        assert event.record.table_name == "users"
        assert event.record.data["name"] == "John Updated"
        assert event.record.before_data is not None
        assert "updated_fields" in event.record.before_data

    def test_change_to_event_delete(self, connector):
        """Test converting MongoDB delete change to event."""
        doc_id = ObjectId()
        change = {
            "_id": {"_data": "resume_token"},
            "operationType": "delete",
            "ns": {"db": "test_db", "coll": "users"},
            "documentKey": {"_id": doc_id}
        }

        event = connector._change_to_event(change)

        assert event is not None
        assert event.record.operation == OperationType.DELETE
        assert event.record.table_name == "users"
        assert str(doc_id) in str(event.record.data["_id"])

    def test_change_to_event_unsupported_operation(self, connector):
        """Test handling unsupported operation types."""
        change = {
            "_id": {"_data": "resume_token"},
            "operationType": "unknown",
            "ns": {"db": "test_db", "coll": "users"}
        }

        event = connector._change_to_event(change)

        assert event is None

    def test_infer_columns_from_documents(self, connector):
        """Test column inference from sample documents."""
        documents = [
            {"name": "John", "age": 30, "active": True, "score": 85.5},
            {"name": "Jane", "age": None, "active": False, "tags": ["user"]},
            {"name": "Bob", "age": 25, "active": True, "metadata": {"role": "admin"}},
        ]

        columns = connector._infer_columns_from_documents(documents)

        # Convert to dict for easier testing
        column_dict = {col.name: col for col in columns}

        assert "name" in column_dict
        assert column_dict["name"].type == ColumnType.STRING
        assert column_dict["name"].nullable is False

        assert "age" in column_dict
        assert column_dict["age"].type == ColumnType.INTEGER
        assert column_dict["age"].nullable is True  # Due to None value

        assert "active" in column_dict
        assert column_dict["active"].type == ColumnType.BOOLEAN

        assert "score" in column_dict
        assert column_dict["score"].type == ColumnType.DOUBLE

        assert "tags" in column_dict
        assert column_dict["tags"].type == ColumnType.STRING  # JSON converted to string after flattening

        assert "metadata_role" in column_dict
        assert column_dict["metadata_role"].type == ColumnType.STRING

    @pytest.mark.asyncio
    async def test_get_changes_timestamp_strategy(self, connector):
        """Test getting changes using timestamp strategy."""
        # Setup mocks
        connector.connected = True

        # Mock database
        mock_database = AsyncMock()
        mock_database.list_collection_names = AsyncMock(return_value=["users"])
        connector._database = mock_database
        connector.change_detection_strategy = "timestamp"

        # Mock documents with timestamp
        timestamp = datetime.now(timezone.utc)
        sample_docs = [
            {"_id": ObjectId(), "name": "John", "updated_at": timestamp}
        ]

        # Mock collection with proper method chaining
        mock_collection = AsyncMock()

        async def async_changes_iterator():
            for doc in sample_docs:
                yield doc

        mock_final_cursor = AsyncMock()
        mock_final_cursor.__aiter__ = lambda: async_changes_iterator()

        mock_limit_result = AsyncMock()
        mock_limit_result.return_value = mock_final_cursor

        mock_sort_result = AsyncMock()
        mock_sort_result.limit = mock_limit_result

        mock_find_result = AsyncMock()
        mock_find_result.sort.return_value = mock_sort_result
        mock_collection.find.return_value = mock_find_result

        mock_database.__getitem__.return_value = mock_collection

        # Test getting changes
        events = []
        async for event in connector.get_changes("test_schema", batch_size=100):
            events.append(event)

        assert len(events) >= 1  # Should have at least one event

    @pytest.mark.asyncio
    async def test_test_connection_success(self, connector):
        """Test successful connection test."""
        with patch.object(connector, "connect", new_callable=AsyncMock) as mock_connect, \
             patch.object(connector, "disconnect", new_callable=AsyncMock) as mock_disconnect:

            result = await connector.test_connection()

            assert result is True
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_connection_failure(self, connector):
        """Test connection test failure."""
        with patch.object(connector, "connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")

            result = await connector.test_connection()

            assert result is False

    @pytest.mark.asyncio
    async def test_context_manager(self, connector):
        """Test using connector as async context manager."""
        with patch.object(connector, "connect", new_callable=AsyncMock) as mock_connect, \
             patch.object(connector, "disconnect", new_callable=AsyncMock) as mock_disconnect:

            async with connector:
                pass

            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_runtime_error_when_not_connected(self, connector):
        """Test that methods raise RuntimeError when not connected."""
        connector.connected = False

        with pytest.raises(RuntimeError, match="Not connected to MongoDB"):
            await connector.get_schema("test_schema")

        with pytest.raises(RuntimeError, match="Not connected to MongoDB"):
            async for _ in connector.get_changes("test_schema"):
                pass

        with pytest.raises(RuntimeError, match="Not connected to MongoDB"):
            async for _ in connector.get_full_snapshot("test_schema", "test_table"):
                pass
