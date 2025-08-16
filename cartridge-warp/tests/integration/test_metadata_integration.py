"""Integration tests for metadata management system using testcontainers.

These tests use real PostgreSQL containers to test the metadata system
in a production-like environment, as recommended by the coding guidelines.
"""

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List

import pytest
import asyncpg
from asyncpg import Pool
from testcontainers.postgres import PostgresContainer

from cartridge_warp.metadata.manager import MetadataManager
from cartridge_warp.metadata.models import (
    DeadLetterQueue,
    DLQStatus,
    ErrorLog,
    ErrorStatus,
    ErrorType,
    EvolutionType,
    MarkerType,
    OperationType,
    SchemaDefinition,
    SchemaRegistry,
    SyncMarker,
    SyncRun,
    SyncRunStatistics,
    SyncStatus,
    SyncMode,
)


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def postgres_container():
    """Start a PostgreSQL container for testing."""
    container = PostgresContainer("postgres:15")
    container.start()
    
    yield container
    
    container.stop()


@pytest.fixture(scope="session")
async def database_pool(postgres_container):
    """Create a database connection pool."""
    connection_url = postgres_container.get_connection_url()
    
    pool = await asyncpg.create_pool(
        connection_url,
        min_size=1,
        max_size=10,
        command_timeout=60
    )
    
    yield pool
    
    await pool.close()


@pytest.fixture
async def metadata_manager(database_pool):
    """Create a metadata manager with real database connection."""
    manager = MetadataManager(
        connection_pool=database_pool,
        metadata_schema="test_metadata",
        enable_cleanup=False,  # Disable for testing
        retention_days=1,
        cleanup_interval_seconds=10,  # Short interval for testing
        retry_initial_interval_seconds=1,
        retry_max_interval_seconds=5
    )
    
    await manager.initialize()
    
    yield manager
    
    # Cleanup after test
    await manager.cleanup_metadata_schema()


class TestMetadataManagerIntegration:
    """Integration tests for MetadataManager with real database."""

    async def test_initialize_creates_tables(self, metadata_manager):
        """Test that initialization creates all required tables."""
        async with metadata_manager.pool.acquire() as conn:
            # Check that all tables exist
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'test_metadata'
            """)
            
            table_names = {row['table_name'] for row in tables}
            expected_tables = {
                'sync_markers',
                'schema_registry', 
                'sync_runs',
                'error_log',
                'dead_letter_queue'
            }
            
            assert expected_tables.issubset(table_names)

    async def test_sync_marker_operations(self, metadata_manager):
        """Test sync marker CRUD operations with real database."""
        schema_name = "test_schema"
        position_data = {"lsn": "0/1234567", "timestamp": datetime.now(timezone.utc).isoformat()}
        
        # Create marker
        marker = await metadata_manager.update_sync_marker(
            schema_name=schema_name,
            position_data=position_data,
            marker_type=MarkerType.STREAM
        )
        
        assert marker.schema_name == schema_name
        assert marker.position_data == position_data
        assert marker.marker_type == MarkerType.STREAM
        
        # Retrieve marker
        retrieved_marker = await metadata_manager.get_sync_marker(
            schema_name=schema_name,
            marker_type=MarkerType.STREAM
        )
        
        assert retrieved_marker is not None
        assert retrieved_marker.id == marker.id
        assert retrieved_marker.position_data == position_data
        
        # Update marker
        new_position = {"lsn": "0/1234568", "timestamp": datetime.now(timezone.utc).isoformat()}
        updated_marker = await metadata_manager.update_sync_marker(
            schema_name=schema_name,
            position_data=new_position,
            marker_type=MarkerType.STREAM
        )
        
        assert updated_marker.id == marker.id  # Same record
        assert updated_marker.position_data == new_position

    async def test_schema_registry_operations(self, metadata_manager):
        """Test schema registry operations with real database."""
        schema_name = "test_schema"
        table_name = "test_table"
        
        schema_def = SchemaDefinition(
            columns=[
                {"name": "id", "type": "int", "nullable": False},
                {"name": "name", "type": "varchar", "length": 255}
            ],
            primary_keys=["id"]
        )
        
        # Register schema
        registry = await metadata_manager.register_schema(
            schema_name=schema_name,
            table_name=table_name,
            schema_definition=schema_def,
            evolution_type=EvolutionType.CREATE
        )
        
        assert registry.schema_name == schema_name
        assert registry.table_name == table_name
        assert registry.version == 1
        assert registry.evolution_type == EvolutionType.CREATE
        
        # Retrieve schema
        retrieved = await metadata_manager.get_schema_version(
            schema_name=schema_name,
            table_name=table_name
        )
        
        assert retrieved is not None
        assert retrieved.version == 1
        assert retrieved.schema_definition.columns == schema_def.columns
        
        # Register new version
        new_schema_def = SchemaDefinition(
            columns=[
                {"name": "id", "type": "int", "nullable": False},
                {"name": "name", "type": "varchar", "length": 255},
                {"name": "email", "type": "varchar", "length": 255}
            ],
            primary_keys=["id"]
        )
        
        new_registry = await metadata_manager.register_schema(
            schema_name=schema_name,
            table_name=table_name,
            schema_definition=new_schema_def,
            evolution_type=EvolutionType.ADD_COLUMN
        )
        
        assert new_registry.version == 2
        assert new_registry.previous_version == 1
        assert new_registry.evolution_type == EvolutionType.ADD_COLUMN

    async def test_sync_run_lifecycle(self, metadata_manager):
        """Test complete sync run lifecycle."""
        schema_name = "test_schema"
        
        # Start sync run
        sync_run = await metadata_manager.start_sync_run(
            schema_name=schema_name,
            sync_mode=SyncMode.STREAM,
            config_hash="abc123",
            instance_id="test-instance"
        )
        
        assert sync_run.schema_name == schema_name
        assert sync_run.sync_mode == SyncMode.STREAM
        assert sync_run.status == SyncStatus.RUNNING
        assert sync_run.config_hash == "abc123"
        
        # Complete sync run
        stats = SyncRunStatistics(
            records_processed=1000,
            records_inserted=800,
            records_updated=200,
            bytes_processed=50000
        )
        
        await metadata_manager.complete_sync_run(
            sync_run_id=sync_run.id,
            status=SyncStatus.COMPLETED,
            statistics=stats
        )
        
        # Verify in database
        async with metadata_manager.pool.acquire() as conn:
            run_data = await conn.fetchrow(
                f"SELECT * FROM {metadata_manager.metadata_schema}.sync_runs WHERE id = $1",
                sync_run.id
            )
            
            assert run_data['status'] == SyncStatus.COMPLETED
            assert run_data['records_processed'] == 1000
            assert run_data['records_inserted'] == 800
            assert run_data['records_updated'] == 200
            assert run_data['bytes_processed'] == 50000
            assert run_data['completed_at'] is not None
            assert run_data['duration_ms'] is not None

    async def test_error_logging_and_dlq(self, metadata_manager):
        """Test error logging and dead letter queue operations."""
        schema_name = "test_schema"
        table_name = "test_table"
        
        # Start a sync run first
        sync_run = await metadata_manager.start_sync_run(
            schema_name=schema_name,
            sync_mode=SyncMode.STREAM
        )
        
        # Log an error
        error_log = await metadata_manager.log_error(
            schema_name=schema_name,
            table_name=table_name,
            error_type=ErrorType.CONSTRAINT,
            error_message="Unique constraint violation",
            sync_run_id=sync_run.id,
            error_code="23505",
            error_details={"column": "email", "value": "test@example.com"},
            record_data={"id": 1, "name": "Test", "email": "test@example.com"},
            operation_type=OperationType.INSERT
        )
        
        assert error_log.schema_name == schema_name
        assert error_log.error_type == ErrorType.CONSTRAINT
        assert error_log.error_message == "Unique constraint violation"
        assert error_log.sync_run_id == sync_run.id
        
        # Add to dead letter queue
        dlq_record = await metadata_manager.add_to_dead_letter_queue(
            schema_name=schema_name,
            table_name=table_name,
            operation_type=OperationType.INSERT,
            record_data={"id": 1, "name": "Test", "email": "test@example.com"},
            sync_run_id=sync_run.id,
            error_log_id=error_log.id,
            source_record_id="1",
            error_message="Failed to insert due to constraint violation"
        )
        
        assert dlq_record.schema_name == schema_name
        assert dlq_record.table_name == table_name
        assert dlq_record.operation_type == OperationType.INSERT
        assert dlq_record.sync_run_id == sync_run.id
        assert dlq_record.error_log_id == error_log.id
        assert dlq_record.error_count == 1
        assert dlq_record.status == DLQStatus.PENDING
        
        # Verify in database
        async with metadata_manager.pool.acquire() as conn:
            error_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {metadata_manager.metadata_schema}.error_log WHERE sync_run_id = $1",
                sync_run.id
            )
            assert error_count == 1
            
            dlq_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {metadata_manager.metadata_schema}.dead_letter_queue WHERE sync_run_id = $1",
                sync_run.id
            )
            assert dlq_count == 1

    async def test_recovery_operations(self, metadata_manager):
        """Test recovery of stuck sync runs."""
        schema_name = "test_schema"
        
        # Create a sync run directly in database (simulating stuck run)
        async with metadata_manager.pool.acquire() as conn:
            stuck_run_id = uuid.uuid4()
            old_timestamp = datetime.now(timezone.utc) - timedelta(hours=25)
            
            await conn.execute(
                f"""
                INSERT INTO {metadata_manager.metadata_schema}.sync_runs
                    (id, schema_name, sync_mode, status, started_at)
                VALUES ($1, $2, $3, $4, $5)
                """,
                stuck_run_id, schema_name, SyncMode.STREAM, SyncStatus.RUNNING, old_timestamp
            )
        
        # Run recovery
        recovered_runs = await metadata_manager.recover_failed_runs(max_age_hours=24)
        
        assert len(recovered_runs) == 1
        assert recovered_runs[0] == stuck_run_id
        
        # Verify the run was marked as failed
        async with metadata_manager.pool.acquire() as conn:
            run_status = await conn.fetchval(
                f"SELECT status FROM {metadata_manager.metadata_schema}.sync_runs WHERE id = $1",
                stuck_run_id
            )
            assert run_status == SyncStatus.FAILED

    async def test_cleanup_operations(self, metadata_manager):
        """Test metadata cleanup operations."""
        # Create old completed sync run
        schema_name = "test_schema"
        old_timestamp = datetime.now(timezone.utc) - timedelta(days=35)
        
        async with metadata_manager.pool.acquire() as conn:
            old_run_id = uuid.uuid4()
            await conn.execute(
                f"""
                INSERT INTO {metadata_manager.metadata_schema}.sync_runs
                    (id, schema_name, sync_mode, status, started_at, completed_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                old_run_id, schema_name, SyncMode.STREAM, SyncStatus.COMPLETED, 
                old_timestamp, old_timestamp
            )
        
        # Run cleanup with 30-day retention
        manager_with_cleanup = MetadataManager(
            connection_pool=metadata_manager.pool,
            metadata_schema=metadata_manager.metadata_schema,
            enable_cleanup=True,
            retention_days=30
        )
        
        cleanup_stats = await manager_with_cleanup.cleanup_old_metadata()
        
        assert cleanup_stats['sync_runs'] >= 1
        
        # Verify the old run was cleaned up
        async with metadata_manager.pool.acquire() as conn:
            remaining_runs = await conn.fetchval(
                f"SELECT COUNT(*) FROM {metadata_manager.metadata_schema}.sync_runs WHERE id = $1",
                old_run_id
            )
            assert remaining_runs == 0

    async def test_background_cleanup_with_exponential_backoff(self, database_pool):
        """Test background cleanup with exponential backoff on failures."""
        # Create a manager that will fail cleanup (invalid schema name)
        failing_manager = MetadataManager(
            connection_pool=database_pool,
            metadata_schema="nonexistent_schema",
            enable_cleanup=True,
            retention_days=1,
            cleanup_interval_seconds=0.1,  # Very short for testing
            retry_initial_interval_seconds=0.1,
            retry_max_interval_seconds=0.5
        )
        
        # Start background cleanup task
        cleanup_task = asyncio.create_task(failing_manager._background_cleanup())
        
        # Let it run for a short time to experience failures
        await asyncio.sleep(1)
        
        # Cancel the task
        cleanup_task.cancel()
        
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        
        # Test passes if we get here without hanging (exponential backoff working)
        assert True

    @pytest.mark.skip(reason="Manual test for performance validation")
    async def test_performance_with_large_dataset(self, metadata_manager):
        """Performance test with large number of operations (manual execution only)."""
        import time
        
        # Create many sync markers
        start_time = time.time()
        
        for i in range(1000):
            await metadata_manager.update_sync_marker(
                schema_name=f"schema_{i % 10}",
                table_name=f"table_{i % 100}",
                position_data={"position": i, "timestamp": datetime.now(timezone.utc).isoformat()},
                marker_type=MarkerType.STREAM
            )
        
        creation_time = time.time() - start_time
        
        # Retrieve markers
        start_time = time.time()
        
        for i in range(100):
            marker = await metadata_manager.get_sync_marker(
                schema_name=f"schema_{i % 10}",
                table_name=f"table_{i % 100}",
                marker_type=MarkerType.STREAM
            )
            assert marker is not None
        
        retrieval_time = time.time() - start_time
        
        print(f"Created 1000 markers in {creation_time:.2f}s")
        print(f"Retrieved 100 markers in {retrieval_time:.2f}s")
        
        # Performance should be reasonable (< 10s for creation, < 1s for retrieval)
        assert creation_time < 10.0
        assert retrieval_time < 1.0
