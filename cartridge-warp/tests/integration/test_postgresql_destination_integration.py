"""Integration tests for PostgreSQL destination connector.

These tests require a real PostgreSQL database for full integration testing.
They can be run with testcontainers or against a local PostgreSQL instance.
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import List

import pytest
from testcontainers.postgres import PostgresContainer

from cartridge_warp.connectors.base import (
    ColumnDefinition,
    ColumnType,
    OperationType,
    Record,
    SchemaChange,
    TableSchema,
)
from cartridge_warp.connectors.postgresql_destination import PostgreSQLDestinationConnector


@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container for integration tests."""
    with PostgresContainer("postgres:15", username="test", password="test", dbname="testdb") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def postgres_connection_string(postgres_container):
    """Get PostgreSQL connection string."""
    return postgres_container.get_connection_url()


@pytest.fixture
async def connector(postgres_connection_string):
    """Create PostgreSQL connector connected to test database."""
    connector = PostgreSQLDestinationConnector(
        connection_string=postgres_connection_string,
        metadata_schema="integration_test_metadata",
        batch_size=100,
        max_connections=3,
    )
    await connector.connect()
    yield connector
    await connector.disconnect()


@pytest.fixture
def sample_table_schema():
    """Create a comprehensive table schema for testing."""
    return TableSchema(
        name="test_users",
        columns=[
            ColumnDefinition(name="id", type=ColumnType.INTEGER, nullable=False),
            ColumnDefinition(name="email", type=ColumnType.STRING, nullable=False, max_length=255),
            ColumnDefinition(name="name", type=ColumnType.STRING, nullable=True, max_length=100),
            ColumnDefinition(name="age", type=ColumnType.INTEGER, nullable=True),
            ColumnDefinition(name="salary", type=ColumnType.DOUBLE, nullable=True),
            ColumnDefinition(name="is_active", type=ColumnType.BOOLEAN, nullable=False, default=True),
            ColumnDefinition(name="created_at", type=ColumnType.TIMESTAMP, nullable=False),
            ColumnDefinition(name="profile_data", type=ColumnType.JSON, nullable=True),
        ],
        primary_keys=["id"],
        indexes=[
            {"name": "idx_test_users_email", "columns": ["email"], "unique": True},
            {"name": "idx_test_users_created_at", "columns": ["created_at"]},
            {"name": "idx_test_users_name", "columns": ["name"]},
        ],
    )


class TestPostgreSQLDestinationConnectorIntegration:
    """Integration tests for PostgreSQL destination connector."""

    @pytest.mark.asyncio
    async def test_connection_lifecycle(self, postgres_connection_string):
        """Test complete connection lifecycle."""
        connector = PostgreSQLDestinationConnector(
            connection_string=postgres_connection_string,
            metadata_schema="test_metadata",
        )
        
        # Initially not connected
        assert not connector.connected
        assert connector.pool is None
        
        # Test connection
        await connector.connect()
        assert connector.connected
        assert connector.pool is not None
        
        # Test connection health
        assert await connector.test_connection() is True
        
        # Disconnect
        await connector.disconnect()
        assert not connector.connected
        assert connector.pool is None

    @pytest.mark.asyncio
    async def test_schema_and_table_creation(self, connector, sample_table_schema):
        """Test schema and table creation with real database."""
        schema_name = "integration_test_schema"
        
        # Create schema
        await connector.create_schema_if_not_exists(schema_name)
        
        # Create table
        await connector.create_table_if_not_exists(schema_name, sample_table_schema)
        
        # Verify table exists by attempting to query it
        async with connector.pool.acquire() as conn:
            # Check schema exists
            schema_result = await conn.fetchval(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
                schema_name
            )
            assert schema_result == schema_name
            
            # Check table exists
            table_result = await conn.fetchval(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
                schema_name, sample_table_schema.name
            )
            assert table_result == sample_table_schema.name
            
            # Check columns exist
            columns_result = await conn.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
                schema_name, sample_table_schema.name
            )
            
            column_names = {row["column_name"] for row in columns_result}
            expected_columns = {col.name for col in sample_table_schema.columns}
            
            # Should have all original columns plus metadata columns
            assert expected_columns.issubset(column_names)
            assert "_cartridge_created_at" in column_names
            assert "_cartridge_updated_at" in column_names
            assert "_cartridge_version" in column_names

    @pytest.mark.asyncio
    async def test_insert_operations(self, connector, sample_table_schema):
        """Test INSERT operations with real data."""
        schema_name = "integration_test_schema"
        
        # Setup
        await connector.create_schema_if_not_exists(schema_name)
        await connector.create_table_if_not_exists(schema_name, sample_table_schema)
        
        # Create test records
        records = [
            Record(
                table_name=sample_table_schema.name,
                data={
                    "id": 1,
                    "email": "user1@example.com",
                    "name": "John Doe",
                    "age": 30,
                    "salary": 75000.50,
                    "is_active": True,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "profile_data": {"role": "developer", "skills": ["python", "sql"]},
                },
                operation=OperationType.INSERT,
                timestamp=datetime.now(timezone.utc),
                primary_key_values={"id": 1},
            ),
            Record(
                table_name=sample_table_schema.name,
                data={
                    "id": 2,
                    "email": "user2@example.com",
                    "name": "Jane Smith",
                    "age": 28,
                    "salary": 82000.75,
                    "is_active": True,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "profile_data": {"role": "manager", "team_size": 5},
                },
                operation=OperationType.INSERT,
                timestamp=datetime.now(timezone.utc),
                primary_key_values={"id": 2},
            ),
        ]
        
        # Write batch
        await connector.write_batch(schema_name, records)
        
        # Verify data was inserted
        async with connector.pool.acquire() as conn:
            result = await conn.fetch(
                f'SELECT * FROM "{schema_name}"."{sample_table_schema.name}" ORDER BY id'
            )
            
            assert len(result) == 2
            
            # Check first record
            row1 = result[0]
            assert row1["id"] == 1
            assert row1["email"] == "user1@example.com"
            assert row1["name"] == "John Doe"
            assert row1["age"] == 30
            assert row1["salary"] == 75000.50
            assert row1["is_active"] is True
            
            # Check JSON data
            profile_data = json.loads(row1["profile_data"])
            assert profile_data["role"] == "developer"
            assert "python" in profile_data["skills"]
            
            # Check metadata columns
            assert row1["_cartridge_created_at"] is not None
            assert row1["_cartridge_updated_at"] is not None
            assert row1["_cartridge_version"] == 1

    @pytest.mark.asyncio
    async def test_upsert_operations(self, connector, sample_table_schema):
        """Test UPSERT (INSERT with conflict resolution)."""
        schema_name = "integration_test_schema"
        
        # Setup
        await connector.create_schema_if_not_exists(schema_name)
        await connector.create_table_if_not_exists(schema_name, sample_table_schema)
        
        # Insert initial record
        initial_record = Record(
            table_name=sample_table_schema.name,
            data={
                "id": 1,
                "email": "original@example.com",
                "name": "Original Name",
                "age": 25,
                "salary": 50000.0,
                "is_active": True,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "profile_data": {"status": "new"},
            },
            operation=OperationType.INSERT,
            timestamp=datetime.now(timezone.utc),
            primary_key_values={"id": 1},
        )
        
        await connector.write_batch(schema_name, [initial_record])
        
        # Insert conflicting record (should update existing)
        conflict_record = Record(
            table_name=sample_table_schema.name,
            data={
                "id": 1,  # Same ID - should cause conflict
                "email": "updated@example.com",
                "name": "Updated Name",
                "age": 26,
                "salary": 55000.0,
                "is_active": True,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "profile_data": {"status": "updated"},
            },
            operation=OperationType.INSERT,
            timestamp=datetime.now(timezone.utc),
            primary_key_values={"id": 1},
        )
        
        await connector.write_batch(schema_name, [conflict_record])
        
        # Verify only one record exists with updated data
        async with connector.pool.acquire() as conn:
            result = await conn.fetch(
                f'SELECT * FROM "{schema_name}"."{sample_table_schema.name}" WHERE id = 1'
            )
            
            assert len(result) == 1
            row = result[0]
            assert row["email"] == "updated@example.com"
            assert row["name"] == "Updated Name"
            assert row["age"] == 26
            
            profile_data = json.loads(row["profile_data"])
            assert profile_data["status"] == "updated"

    @pytest.mark.asyncio
    async def test_update_operations(self, connector, sample_table_schema):
        """Test UPDATE operations."""
        schema_name = "integration_test_schema"
        
        # Setup with initial data
        await connector.create_schema_if_not_exists(schema_name)
        await connector.create_table_if_not_exists(schema_name, sample_table_schema)
        
        # Insert initial record
        initial_record = Record(
            table_name=sample_table_schema.name,
            data={
                "id": 1,
                "email": "test@example.com",
                "name": "Test User",
                "age": 30,
                "salary": 70000.0,
                "is_active": True,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "profile_data": {"level": "junior"},
            },
            operation=OperationType.INSERT,
            timestamp=datetime.now(timezone.utc),
            primary_key_values={"id": 1},
        )
        
        await connector.write_batch(schema_name, [initial_record])
        
        # Update record
        update_record = Record(
            table_name=sample_table_schema.name,
            data={
                "name": "Test User Updated",
                "age": 31,
                "salary": 75000.0,
                "profile_data": {"level": "senior"},
            },
            operation=OperationType.UPDATE,
            timestamp=datetime.now(timezone.utc),
            primary_key_values={"id": 1},
        )
        
        await connector.write_batch(schema_name, [update_record])
        
        # Verify update
        async with connector.pool.acquire() as conn:
            result = await conn.fetchrow(
                f'SELECT * FROM "{schema_name}"."{sample_table_schema.name}" WHERE id = 1'
            )
            
            assert result["name"] == "Test User Updated"
            assert result["age"] == 31
            assert result["salary"] == 75000.0
            assert result["email"] == "test@example.com"  # Unchanged
            
            profile_data = json.loads(result["profile_data"])
            assert profile_data["level"] == "senior"
            
            # Version should be incremented
            assert result["_cartridge_version"] == 2

    @pytest.mark.asyncio
    async def test_soft_delete_operations(self, connector, sample_table_schema):
        """Test soft delete operations."""
        schema_name = "integration_test_schema"
        
        # Setup
        await connector.create_schema_if_not_exists(schema_name)
        await connector.create_table_if_not_exists(schema_name, sample_table_schema)
        
        # Insert test record
        insert_record = Record(
            table_name=sample_table_schema.name,
            data={
                "id": 1,
                "email": "delete_test@example.com",
                "name": "Delete Test",
                "age": 25,
                "salary": 50000.0,
                "is_active": True,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            operation=OperationType.INSERT,
            timestamp=datetime.now(timezone.utc),
            primary_key_values={"id": 1},
        )
        
        await connector.write_batch(schema_name, [insert_record])
        
        # Soft delete record
        delete_record = Record(
            table_name=sample_table_schema.name,
            data={"id": 1},
            operation=OperationType.DELETE,
            timestamp=datetime.now(timezone.utc),
            primary_key_values={"id": 1},
        )
        
        await connector.write_batch(schema_name, [delete_record])
        
        # Verify soft delete
        async with connector.pool.acquire() as conn:
            result = await conn.fetchrow(
                f'SELECT * FROM "{schema_name}"."{sample_table_schema.name}" WHERE id = 1'
            )
            
            # Record should still exist but marked as deleted
            assert result is not None
            assert result["is_deleted"] is True
            assert result["deleted_at"] is not None
            assert result["_cartridge_version"] == 2  # Version incremented

    @pytest.mark.asyncio
    async def test_schema_evolution_add_column(self, connector, sample_table_schema):
        """Test schema evolution by adding columns."""
        schema_name = "integration_test_schema"
        
        # Setup
        await connector.create_schema_if_not_exists(schema_name)
        await connector.create_table_if_not_exists(schema_name, sample_table_schema)
        
        # Add a new column
        schema_change = SchemaChange(
            schema_name=schema_name,
            table_name=sample_table_schema.name,
            change_type="add_column",
            details={
                "column_name": "phone_number",
                "column_type": "string",
                "nullable": True,
                "default": None,
            },
            timestamp=datetime.now(timezone.utc),
        )
        
        await connector.apply_schema_changes(schema_name, [schema_change])
        
        # Verify column was added
        async with connector.pool.acquire() as conn:
            columns_result = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
                schema_name, sample_table_schema.name, "phone_number"
            )
            
            assert len(columns_result) == 1
            assert columns_result[0]["column_name"] == "phone_number"
        
        # Test inserting data with new column
        record_with_new_column = Record(
            table_name=sample_table_schema.name,
            data={
                "id": 2,
                "email": "newcol@example.com",
                "name": "New Column Test",
                "age": 28,
                "salary": 60000.0,
                "is_active": True,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "phone_number": "+1-555-0123",
            },
            operation=OperationType.INSERT,
            timestamp=datetime.now(timezone.utc),
            primary_key_values={"id": 2},
        )
        
        await connector.write_batch(schema_name, [record_with_new_column])
        
        # Verify data insertion with new column
        async with connector.pool.acquire() as conn:
            result = await conn.fetchrow(
                f'SELECT * FROM "{schema_name}"."{sample_table_schema.name}" WHERE id = 2'
            )
            
            assert result["phone_number"] == "+1-555-0123"

    @pytest.mark.asyncio
    async def test_marker_management(self, connector):
        """Test processing marker update and retrieval."""
        schema_name = "integration_test_schema"
        table_name = "test_table"
        
        # Test marker doesn't exist initially
        marker = await connector.get_marker(schema_name, table_name)
        assert marker is None
        
        # Update marker
        test_marker = {
            "position": 12345,
            "timestamp": "2024-01-15T10:30:00Z",
            "operation_id": "op_123",
        }
        
        await connector.update_marker(schema_name, table_name, test_marker)
        
        # Retrieve marker
        retrieved_marker = await connector.get_marker(schema_name, table_name)
        assert retrieved_marker == test_marker
        
        # Update marker again
        updated_marker = {
            "position": 67890,
            "timestamp": "2024-01-15T11:00:00Z",
            "operation_id": "op_456",
        }
        
        await connector.update_marker(schema_name, table_name, updated_marker)
        
        # Verify update
        final_marker = await connector.get_marker(schema_name, table_name)
        assert final_marker == updated_marker

    @pytest.mark.asyncio
    async def test_large_batch_processing(self, connector, sample_table_schema):
        """Test processing large batches of records efficiently."""
        schema_name = "integration_test_schema"
        
        # Setup
        await connector.create_schema_if_not_exists(schema_name)
        await connector.create_table_if_not_exists(schema_name, sample_table_schema)
        
        # Create large batch of records
        large_batch = []
        batch_size = 1000
        
        for i in range(batch_size):
            record = Record(
                table_name=sample_table_schema.name,
                data={
                    "id": i,
                    "email": f"user{i}@example.com",
                    "name": f"User {i}",
                    "age": 20 + (i % 50),
                    "salary": 50000.0 + (i * 100),
                    "is_active": i % 2 == 0,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "profile_data": {"batch_id": i // 100, "index": i},
                },
                operation=OperationType.INSERT,
                timestamp=datetime.now(timezone.utc),
                primary_key_values={"id": i},
            )
            large_batch.append(record)
        
        # Process large batch
        start_time = datetime.now()
        await connector.write_batch(schema_name, large_batch)
        end_time = datetime.now()
        
        processing_time = (end_time - start_time).total_seconds()
        print(f"Processed {batch_size} records in {processing_time:.2f} seconds")
        
        # Verify all records were inserted
        async with connector.pool.acquire() as conn:
            count = await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema_name}"."{sample_table_schema.name}"'
            )
            assert count == batch_size
        
        # Performance check: should process at least 100 records per second
        records_per_second = batch_size / processing_time
        assert records_per_second >= 100, f"Performance too slow: {records_per_second:.2f} records/sec"

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, connector, sample_table_schema):
        """Test concurrent operations don't cause conflicts."""
        schema_name = "integration_test_schema"
        
        # Setup
        await connector.create_schema_if_not_exists(schema_name)
        await connector.create_table_if_not_exists(schema_name, sample_table_schema)
        
        async def create_batch(start_id: int, count: int) -> List[Record]:
            """Create a batch of records with unique IDs."""
            batch = []
            for i in range(count):
                record = Record(
                    table_name=sample_table_schema.name,
                    data={
                        "id": start_id + i,
                        "email": f"concurrent{start_id + i}@example.com",
                        "name": f"Concurrent User {start_id + i}",
                        "age": 25,
                        "salary": 55000.0,
                        "is_active": True,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    },
                    operation=OperationType.INSERT,
                    timestamp=datetime.now(timezone.utc),
                    primary_key_values={"id": start_id + i},
                )
                batch.append(record)
            return batch
        
        # Create multiple batches concurrently
        batch1 = create_batch(1000, 100)
        batch2 = create_batch(2000, 100)
        batch3 = create_batch(3000, 100)
        
        # Process batches concurrently
        await asyncio.gather(
            connector.write_batch(schema_name, await batch1),
            connector.write_batch(schema_name, await batch2),
            connector.write_batch(schema_name, await batch3),
        )
        
        # Verify all records were inserted
        async with connector.pool.acquire() as conn:
            count = await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema_name}"."{sample_table_schema.name}" WHERE id >= 1000'
            )
            assert count == 300  # 3 batches × 100 records each
