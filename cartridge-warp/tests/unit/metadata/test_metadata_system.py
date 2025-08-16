"""Comprehensive tests for metadata management system."""

import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest
import asyncpg
from asyncpg import Pool

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
from cartridge_warp.metadata.schema import get_schema_creation_sql, get_schema_cleanup_sql


@pytest.fixture
async def metadata_manager():
    """Create a metadata manager with a mocked connection pool."""
    pool_mock = AsyncMock(spec=Pool)
    manager = MetadataManager(
        connection_pool=pool_mock,
        metadata_schema="cartridge_warp",
        enable_cleanup=False,  # Disable for tests
        retention_days=7
    )
    
    # Mock the acquire context manager
    conn_mock = AsyncMock(spec=asyncpg.Connection)
    pool_mock.acquire.return_value.__aenter__.return_value = conn_mock
    pool_mock.acquire.return_value.__aexit__.return_value = None
    
    return manager, pool_mock, conn_mock


class TestMetadataModels:
    """Test metadata model validation and behavior."""
    
    def test_sync_marker_validation(self):
        """Test SyncMarker model validation."""
        marker = SyncMarker(
            schema_name="test_schema",
            table_name="test_table",
            marker_type=MarkerType.STREAM,
            position_data={"lsn": "123456", "timestamp": "2023-01-01T00:00:00Z"}
        )
        
        assert marker.schema_name == "test_schema"
        assert marker.table_name == "test_table"
        assert marker.marker_type == MarkerType.STREAM
        assert marker.position_data["lsn"] == "123456"
        assert isinstance(marker.id, uuid.UUID)
        assert isinstance(marker.created_at, datetime)
        assert isinstance(marker.last_updated, datetime)
        
        # Test validation failure
        with pytest.raises(ValueError, match="position_data cannot be empty"):
            SyncMarker(
                schema_name="test",
                marker_type=MarkerType.STREAM,
                position_data={}
            )
    
    def test_schema_definition_hash(self):
        """Test schema definition hashing."""
        schema_def = SchemaDefinition(
            columns=[
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "TEXT", "nullable": True}
            ],
            primary_keys=["id"],
            indexes=[{"name": "idx_name", "columns": ["name"]}]
        )
        
        # Same definition should have same hash
        schema_def2 = SchemaDefinition(
            columns=[
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "TEXT", "nullable": True}
            ],
            primary_keys=["id"],
            indexes=[{"name": "idx_name", "columns": ["name"]}]
        )
        
        assert schema_def.schema_hash == schema_def2.schema_hash
        assert len(schema_def.schema_hash) == 64  # SHA-256 hex length
        
        # Different definition should have different hash
        schema_def3 = SchemaDefinition(
            columns=[
                {"name": "id", "type": "BIGINT", "nullable": False},  # Different type
                {"name": "name", "type": "TEXT", "nullable": True}
            ],
            primary_keys=["id"]
        )
        
        assert schema_def.schema_hash != schema_def3.schema_hash
    
    def test_sync_run_computed_fields(self):
        """Test SyncRun computed properties."""
        # Running sync
        running_sync = SyncRun(
            schema_name="test",
            sync_mode=SyncMode.STREAM,
            status=SyncStatus.RUNNING
        )
        
        assert running_sync.is_running is True
        assert running_sync.is_completed is False
        assert running_sync.is_failed is False
        
        # Completed sync
        completed_sync = SyncRun(
            schema_name="test",
            sync_mode=SyncMode.BATCH,
            status=SyncStatus.COMPLETED
        )
        
        assert completed_sync.is_running is False
        assert completed_sync.is_completed is True
        assert completed_sync.is_failed is False
        
        # Failed sync
        failed_sync = SyncRun(
            schema_name="test",
            sync_mode=SyncMode.INITIAL,
            status=SyncStatus.FAILED
        )
        
        assert failed_sync.is_running is False
        assert failed_sync.is_completed is False
        assert failed_sync.is_failed is True
    
    def test_error_log_retry_logic(self):
        """Test ErrorLog retry logic."""
        error = ErrorLog(
            schema_name="test",
            error_type=ErrorType.CONNECTION,
            error_message="Connection failed",
            retry_count=1,
            max_retries=3
        )
        
        assert error.can_retry is True
        assert error.is_resolved is False
        
        # Exhausted retries
        error_exhausted = ErrorLog(
            schema_name="test",
            error_type=ErrorType.CONNECTION,
            error_message="Connection failed",
            retry_count=3,
            max_retries=3
        )
        
        assert error_exhausted.can_retry is False
        
        # Resolved error
        error_resolved = ErrorLog(
            schema_name="test",
            error_type=ErrorType.CONNECTION,
            error_message="Connection failed",
            status=ErrorStatus.RESOLVED
        )
        
        assert error_resolved.is_resolved is True
        assert error_resolved.can_retry is False
    
    def test_dead_letter_queue_status(self):
        """Test DeadLetterQueue status checks."""
        dlq_record = DeadLetterQueue(
            schema_name="test",
            table_name="test_table",
            operation_type=OperationType.INSERT,
            record_data={"id": 1, "name": "test"}
        )
        
        assert dlq_record.is_pending is True
        assert dlq_record.is_resolved is False
        
        # Resolved record
        dlq_resolved = DeadLetterQueue(
            schema_name="test",
            table_name="test_table", 
            operation_type=OperationType.UPDATE,
            record_data={"id": 1, "name": "updated"},
            status=DLQStatus.RESOLVED
        )
        
        assert dlq_resolved.is_pending is False
        assert dlq_resolved.is_resolved is True


@pytest.mark.asyncio
class TestMetadataManager:
    """Test metadata manager functionality."""
    
    async def test_initialization(self, metadata_manager):
        """Test metadata manager initialization."""
        manager, pool_mock, conn_mock = metadata_manager
        
        # Mock successful table creation
        conn_mock.execute.return_value = None
        
        await manager.initialize()
        
        # Verify SQL execution calls
        assert conn_mock.execute.call_count > 0
        
        # Should include schema creation and table creation
        calls = [call[0][0] for call in conn_mock.execute.call_args_list]
        assert any("CREATE SCHEMA" in call for call in calls)
        assert any("sync_markers" in call for call in calls)
        assert any("schema_registry" in call for call in calls)
        assert any("sync_runs" in call for call in calls)
        assert any("error_log" in call for call in calls)
        assert any("dead_letter_queue" in call for call in calls)
    
    async def test_sync_marker_operations(self, metadata_manager):
        """Test sync marker CRUD operations."""
        manager, pool_mock, conn_mock = metadata_manager
        
        # Mock get_sync_marker - not found
        conn_mock.fetchrow.return_value = None
        
        result = await manager.get_sync_marker("test_schema", "test_table", MarkerType.STREAM)
        assert result is None
        
        # Mock update_sync_marker
        mock_row = {
            'id': uuid.uuid4(),
            'created_at': datetime.now(timezone.utc)
        }
        conn_mock.fetchrow.return_value = mock_row
        
        position_data = {"lsn": "123456", "timestamp": "2023-01-01T00:00:00Z"}
        result = await manager.update_sync_marker(
            schema_name="test_schema",
            position_data=position_data,
            table_name="test_table",
            marker_type=MarkerType.STREAM
        )
        
        assert isinstance(result, SyncMarker)
        assert result.schema_name == "test_schema"
        assert result.table_name == "test_table"
        assert result.marker_type == MarkerType.STREAM
        assert result.position_data == position_data
    
    async def test_stream_position_helpers(self, metadata_manager):
        """Test stream position helper methods."""
        manager, pool_mock, conn_mock = metadata_manager
        
        # Mock get_stream_position
        mock_marker = {
            'id': uuid.uuid4(),
            'schema_name': 'test_schema',
            'table_name': None,
            'marker_type': 'stream',
            'position_data': {"lsn": "123456", "resume_token": "abc123"},
            'last_updated': datetime.now(timezone.utc),
            'sync_run_id': None,
            'created_at': datetime.now(timezone.utc)
        }
        conn_mock.fetchrow.return_value = mock_marker
        
        position = await manager.get_stream_position("test_schema")
        assert position == {"lsn": "123456", "resume_token": "abc123"}
        
        # Mock update_stream_position
        mock_row = {
            'id': uuid.uuid4(),
            'created_at': datetime.now(timezone.utc)
        }
        conn_mock.fetchrow.return_value = mock_row
        
        new_position = {"lsn": "789012", "resume_token": "def456"}
        await manager.update_stream_position("test_schema", new_position)
        
        # Verify the correct SQL was called
        assert conn_mock.execute.called
    
    async def test_batch_timestamp_helpers(self, metadata_manager):
        """Test batch timestamp helper methods."""
        manager, pool_mock, conn_mock = metadata_manager
        
        # Mock get_batch_timestamp - with timestamp
        timestamp_str = "2023-01-01T12:00:00Z"
        mock_marker = {
            'id': uuid.uuid4(),
            'schema_name': 'test_schema', 
            'table_name': 'test_table',
            'marker_type': 'batch',
            'position_data': {"timestamp": timestamp_str},
            'last_updated': datetime.now(timezone.utc),
            'sync_run_id': None,
            'created_at': datetime.now(timezone.utc)
        }
        conn_mock.fetchrow.return_value = mock_marker
        
        timestamp = await manager.get_batch_timestamp("test_schema", "test_table")
        assert timestamp is not None
        assert timestamp.year == 2023
        assert timestamp.month == 1
        assert timestamp.day == 1
        
        # Mock update_batch_timestamp
        mock_row = {
            'id': uuid.uuid4(),
            'created_at': datetime.now(timezone.utc)
        }
        conn_mock.fetchrow.return_value = mock_row
        
        new_timestamp = datetime(2023, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
        await manager.update_batch_timestamp("test_schema", new_timestamp, "test_table")
        
        # Verify the correct SQL was called
        assert conn_mock.execute.called
    
    async def test_schema_registry_operations(self, metadata_manager):
        """Test schema registry operations.""" 
        manager, pool_mock, conn_mock = metadata_manager
        
        schema_def = SchemaDefinition(
            columns=[
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "TEXT", "nullable": True}
            ],
            primary_keys=["id"]
        )
        
        # Mock register_schema - new schema
        conn_mock.fetchval.return_value = 0  # No existing versions
        conn_mock.fetchrow.return_value = None  # No existing schema with this hash
        
        # Mock transaction
        transaction_mock = AsyncMock()
        conn_mock.transaction.return_value = transaction_mock
        transaction_mock.__aenter__.return_value = transaction_mock
        transaction_mock.__aexit__.return_value = None
        
        result = await manager.register_schema(
            schema_name="test_schema",
            table_name="test_table", 
            schema_definition=schema_def,
            evolution_type=EvolutionType.CREATE
        )
        
        assert isinstance(result, SchemaRegistry)
        assert result.schema_name == "test_schema"
        assert result.table_name == "test_table"
        assert result.version == 1
        assert result.evolution_type == EvolutionType.CREATE
        assert result.schema_definition == schema_def
    
    async def test_sync_run_lifecycle(self, metadata_manager):
        """Test complete sync run lifecycle."""
        manager, pool_mock, conn_mock = metadata_manager
        
        # Start sync run
        sync_run = await manager.start_sync_run(
            schema_name="test_schema",
            sync_mode=SyncMode.STREAM,
            config_hash="abc123",
            instance_id="pod-123"
        )
        
        assert isinstance(sync_run, SyncRun)
        assert sync_run.schema_name == "test_schema"
        assert sync_run.sync_mode == SyncMode.STREAM
        assert sync_run.status == SyncStatus.RUNNING
        assert sync_run.config_hash == "abc123"
        assert sync_run.instance_id == "pod-123"
        
        # Mock completion
        conn_mock.fetchval.return_value = sync_run.started_at  # Mock start time lookup
        
        statistics = SyncRunStatistics(
            records_processed=1000,
            records_inserted=800,
            records_updated=150,
            records_deleted=50,
            bytes_processed=1024000
        )
        
        await manager.complete_sync_run(
            sync_run_id=sync_run.id,
            status=SyncStatus.COMPLETED,
            statistics=statistics
        )
        
        # Verify completion SQL was called
        assert conn_mock.execute.called
    
    async def test_error_logging(self, metadata_manager):
        """Test error logging functionality."""
        manager, pool_mock, conn_mock = metadata_manager
        
        error_log = await manager.log_error(
            schema_name="test_schema",
            error_type=ErrorType.CONNECTION,
            error_message="Database connection failed",
            table_name="test_table",
            error_code="CONN_001",
            error_details={"host": "localhost", "port": 5432},
            stack_trace="Traceback...",
            record_data={"id": 1, "name": "test"},
            operation_type=OperationType.INSERT,
            max_retries=5
        )
        
        assert isinstance(error_log, ErrorLog)
        assert error_log.schema_name == "test_schema"
        assert error_log.error_type == ErrorType.CONNECTION
        assert error_log.error_message == "Database connection failed"
        assert error_log.table_name == "test_table"
        assert error_log.error_code == "CONN_001"
        assert error_log.max_retries == 5
        assert error_log.status == ErrorStatus.OPEN
    
    async def test_dead_letter_queue_operations(self, metadata_manager):
        """Test dead letter queue operations."""
        manager, pool_mock, conn_mock = metadata_manager
        
        # Mock no existing DLQ record
        conn_mock.fetchrow.return_value = None
        
        record_data = {"id": 1, "name": "test", "invalid_field": "problematic_value"}
        
        dlq_record = await manager.add_to_dead_letter_queue(
            schema_name="test_schema",
            table_name="test_table",
            operation_type=OperationType.INSERT,
            record_data=record_data,
            source_record_id="source_123",
            error_message="Constraint violation"
        )
        
        assert isinstance(dlq_record, DeadLetterQueue)
        assert dlq_record.schema_name == "test_schema"
        assert dlq_record.table_name == "test_table"
        assert dlq_record.operation_type == OperationType.INSERT
        assert dlq_record.record_data == record_data
        assert dlq_record.source_record_id == "source_123"
        assert dlq_record.error_count == 1
        assert dlq_record.status == DLQStatus.PENDING
        
        # Test existing record update
        existing_record = {
            'id': dlq_record.id,
            'error_count': 1
        }
        conn_mock.fetchrow.return_value = existing_record
        
        dlq_record2 = await manager.add_to_dead_letter_queue(
            schema_name="test_schema",
            table_name="test_table",
            operation_type=OperationType.INSERT,
            record_data=record_data,
            source_record_id="source_123",
            error_message="Another constraint violation"
        )
        
        assert dlq_record2.error_count == 2
    
    async def test_recovery_operations(self, metadata_manager):
        """Test recovery and cleanup operations."""
        manager, pool_mock, conn_mock = metadata_manager
        
        # Mock stuck runs
        old_time = datetime.now(timezone.utc) - timedelta(hours=25)
        stuck_runs = [
            {'id': uuid.uuid4(), 'schema_name': 'schema1', 'started_at': old_time},
            {'id': uuid.uuid4(), 'schema_name': 'schema2', 'started_at': old_time}
        ]
        conn_mock.fetch.return_value = stuck_runs
        
        recovered_ids = await manager.recover_failed_runs(max_age_hours=24)
        
        assert len(recovered_ids) == 2
        assert recovered_ids == [run['id'] for run in stuck_runs]
        
        # Verify recovery SQL was called for each run
        assert conn_mock.execute.call_count >= 2
    
    async def test_statistics_query(self, metadata_manager):
        """Test sync statistics query."""
        manager, pool_mock, conn_mock = metadata_manager
        
        # Mock statistics results
        runs_stats = {
            'total_runs': 100,
            'completed_runs': 95,
            'failed_runs': 3,
            'running_runs': 2,
            'avg_duration_ms': 5000,
            'total_records_processed': 1000000,
            'total_bytes_processed': 1024000000
        }
        
        error_stats = {
            'total_errors': 25,
            'open_errors': 5,
            'retried_errors': 15
        }
        
        dlq_stats = {
            'total_dlq_records': 10,
            'pending_dlq_records': 3,
            'avg_error_count': 2.5
        }
        
        # Mock multiple fetchrow calls
        conn_mock.fetchrow.side_effect = [runs_stats, error_stats, dlq_stats]
        
        statistics = await manager.get_sync_statistics(schema_name="test_schema", hours=24)
        
        assert statistics['time_range_hours'] == 24
        assert statistics['schema_name'] == "test_schema"
        assert statistics['sync_runs'] == runs_stats
        assert statistics['errors'] == error_stats
        assert statistics['dead_letter_queue'] == dlq_stats
        assert 'generated_at' in statistics


class TestSchemaDefinitions:
    """Test SQL schema definitions."""
    
    def test_schema_creation_sql(self):
        """Test schema creation SQL generation."""
        sql_statements = get_schema_creation_sql()
        
        assert len(sql_statements) > 0
        assert any("CREATE SCHEMA" in stmt for stmt in sql_statements)
        assert any("sync_markers" in stmt for stmt in sql_statements)
        assert any("schema_registry" in stmt for stmt in sql_statements)
        assert any("sync_runs" in stmt for stmt in sql_statements)
        assert any("error_log" in stmt for stmt in sql_statements)
        assert any("dead_letter_queue" in stmt for stmt in sql_statements)
        
        # Check for proper indexing
        assert any("CREATE INDEX" in stmt for stmt in sql_statements)
    
    def test_schema_cleanup_sql(self):
        """Test schema cleanup SQL generation."""
        sql_statements = get_schema_cleanup_sql()
        
        assert len(sql_statements) > 0
        assert any("DROP TABLE" in stmt for stmt in sql_statements)
        assert any("DROP SCHEMA" in stmt for stmt in sql_statements)
        
        # Should drop in reverse dependency order
        drop_order = []
        for stmt in sql_statements:
            if "DROP TABLE" in stmt and "dead_letter_queue" in stmt:
                drop_order.append("dlq")
            elif "DROP TABLE" in stmt and "error_log" in stmt:
                drop_order.append("error_log")
            elif "DROP TABLE" in stmt and "sync_runs" in stmt:
                drop_order.append("sync_runs")
        
        # DLQ should be dropped before error_log, which should be before sync_runs
        assert drop_order.index("dlq") < drop_order.index("error_log")
        assert drop_order.index("error_log") < drop_order.index("sync_runs")
