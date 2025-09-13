"""Comprehensive metadata management for cartridge-warp.

This module implements a full-featured metadata management system that handles:
- CDC position tracking with atomic updates
- Schema evolution and version management  
- Sync run monitoring and statistics
- Error logging and dead letter queue management
- Recovery mechanisms and cleanup operations
"""

import asyncio
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

import asyncpg
import structlog
from asyncpg import Connection, Pool
from asyncpg.exceptions import PostgresError, UniqueViolationError

from .models import (
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
from .schema import get_schema_creation_sql, get_schema_cleanup_sql

logger = structlog.get_logger(__name__)


class MetadataManager:
    """Comprehensive metadata manager for CDC operations.
    
    Provides atomic, transactional metadata operations with full recovery support.
    """

    def __init__(
        self,
        connection_pool: Pool,
        metadata_schema: str = "cartridge_warp",
        enable_cleanup: bool = True,
        retention_days: int = 30,
        cleanup_interval_seconds: int = 3600,
        retry_initial_interval_seconds: int = 60,
        retry_max_interval_seconds: int = 3600
    ):
        """Initialize metadata manager.
        
        Args:
            connection_pool: AsyncPG connection pool for database operations
            metadata_schema: Schema name for metadata tables
            enable_cleanup: Whether to enable automatic cleanup operations
            retention_days: Number of days to retain historical metadata
            cleanup_interval_seconds: Interval between cleanup runs (default: 1 hour)
            retry_initial_interval_seconds: Initial retry interval on cleanup failure (default: 1 minute)
            retry_max_interval_seconds: Maximum retry interval with exponential backoff (default: 1 hour)
        """
        self.pool = connection_pool
        self.metadata_schema = metadata_schema
        self.enable_cleanup = enable_cleanup
        self.retention_days = retention_days
        self.cleanup_interval_seconds = cleanup_interval_seconds
        self.retry_initial_interval_seconds = retry_initial_interval_seconds
        self.retry_max_interval_seconds = retry_max_interval_seconds
        self._initialized = False
        
        # Cache for frequently accessed data
        self._marker_cache: Dict[str, SyncMarker] = {}
        self._schema_cache: Dict[Tuple[str, str], SchemaRegistry] = {}

    async def initialize(self) -> None:
        """Initialize metadata tables and indexes."""
        if self._initialized:
            return
            
        logger.info(
            "Initializing comprehensive metadata system",
            schema=self.metadata_schema,
            retention_days=self.retention_days
        )
        
        try:
            async with self.pool.acquire() as conn:
                # Create schema and tables
                for sql in get_schema_creation_sql(self.metadata_schema):
                    await conn.execute(sql)
                
                logger.info("Metadata tables created successfully")
                self._initialized = True
                
                # Start background cleanup if enabled
                if self.enable_cleanup:
                    asyncio.create_task(self._background_cleanup())
                    
        except Exception as e:
            logger.error("Failed to initialize metadata system", error=str(e))
            raise

    async def cleanup_metadata_schema(self) -> None:
        """Clean up entire metadata schema (for testing/reset)."""
        logger.warning("Cleaning up metadata schema", schema=self.metadata_schema)
        
        async with self.pool.acquire() as conn:
            for sql in get_schema_cleanup_sql(self.metadata_schema):
                try:
                    await conn.execute(sql)
                except Exception as e:
                    logger.warning("Cleanup SQL failed", sql=sql[:100], error=str(e))

    # =====================
    # Sync Position Tracking  
    # =====================
    
    async def get_sync_marker(
        self, 
        schema_name: str,
        table_name: Optional[str] = None,
        marker_type: MarkerType = MarkerType.STREAM
    ) -> Optional[SyncMarker]:
        """Get sync position marker for schema/table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table (None for schema-level markers)
            marker_type: Type of marker to retrieve
            
        Returns:
            SyncMarker if found, None otherwise
        """
        cache_key = f"{schema_name}:{table_name or ''}:{marker_type}"
        if cache_key in self._marker_cache:
            return self._marker_cache[cache_key]
            
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT id, schema_name, table_name, marker_type, position_data,
                       last_updated, sync_run_id, created_at
                FROM {self.metadata_schema}.sync_markers
                WHERE schema_name = $1 
                  AND COALESCE(table_name, '') = COALESCE($2, '')
                  AND marker_type = $3
                """,
                schema_name, table_name, marker_type.value
            )
            
            if row:
                marker = SyncMarker(
                    id=row['id'],
                    schema_name=row['schema_name'],
                    table_name=row['table_name'],
                    marker_type=MarkerType(row['marker_type']),
                    position_data=row['position_data'],
                    last_updated=row['last_updated'],
                    sync_run_id=row['sync_run_id'],
                    created_at=row['created_at']
                )
                self._marker_cache[cache_key] = marker
                return marker
                
        return None

    async def update_sync_marker(
        self,
        schema_name: str,
        position_data: Dict[str, Any],
        table_name: Optional[str] = None,
        marker_type: MarkerType = MarkerType.STREAM,
        sync_run_id: Optional[UUID] = None,
        conn: Optional[Connection] = None
    ) -> SyncMarker:
        """Update sync position marker atomically.
        
        Args:
            schema_name: Name of the schema
            position_data: Position information (LSN, timestamp, etc.)
            table_name: Name of the table (None for schema-level)
            marker_type: Type of marker
            sync_run_id: Associated sync run ID
            conn: Database connection (for transactions)
            
        Returns:
            Updated SyncMarker
        """
        async def _update(connection: Connection) -> SyncMarker:
            now = datetime.now(timezone.utc)
            
            # Upsert marker
            row = await connection.fetchrow(
                f"""
                INSERT INTO {self.metadata_schema}.sync_markers 
                    (schema_name, table_name, marker_type, position_data, last_updated, sync_run_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (schema_name, COALESCE(table_name, ''), marker_type)
                DO UPDATE SET 
                    position_data = EXCLUDED.position_data,
                    last_updated = EXCLUDED.last_updated,
                    sync_run_id = EXCLUDED.sync_run_id
                RETURNING id, created_at
                """,
                schema_name, table_name, marker_type, 
                json.dumps(position_data), now, sync_run_id
            )
            
            marker = SyncMarker(
                id=row['id'],
                schema_name=schema_name,
                table_name=table_name,
                marker_type=marker_type,
                position_data=position_data,
                last_updated=now,
                sync_run_id=sync_run_id,
                created_at=row['created_at']
            )
            
            # Update cache
            cache_key = f"{schema_name}:{table_name or ''}:{marker_type}"
            self._marker_cache[cache_key] = marker
            
            return marker
        
        if conn:
            return await _update(conn)
        else:
            async with self.pool.acquire() as connection:
                return await _update(connection)

    async def get_stream_position(self, schema_name: str, table_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get the last processed stream position for a schema/table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table (None for schema-level)
            
        Returns:
            Position data dictionary or None
        """
        marker = await self.get_sync_marker(schema_name, table_name, MarkerType.STREAM)
        return marker.position_data if marker else None

    async def update_stream_position(
        self,
        schema_name: str,
        position: Dict[str, Any],
        table_name: Optional[str] = None,
        sync_run_id: Optional[UUID] = None
    ) -> None:
        """Update the stream position for a schema/table.
        
        Args:
            schema_name: Name of the schema
            position: Position data (LSN, resume token, etc.)
            table_name: Name of the table (None for schema-level)
            sync_run_id: Associated sync run ID
        """
        await self.update_sync_marker(
            schema_name=schema_name,
            position_data=position,
            table_name=table_name,
            marker_type=MarkerType.STREAM,
            sync_run_id=sync_run_id
        )

    async def get_batch_timestamp(self, schema_name: str, table_name: Optional[str] = None) -> Optional[datetime]:
        """Get the last processed timestamp for batch mode.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table (None for schema-level)
            
        Returns:
            Last processed timestamp or None
        """
        marker = await self.get_sync_marker(schema_name, table_name, MarkerType.BATCH)
        if marker and 'timestamp' in marker.position_data:
            timestamp_str = marker.position_data['timestamp']
            if isinstance(timestamp_str, str):
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            elif isinstance(timestamp_str, datetime):
                return timestamp_str
        return None

    async def update_batch_timestamp(
        self,
        schema_name: str,
        timestamp: datetime,
        table_name: Optional[str] = None,
        sync_run_id: Optional[UUID] = None
    ) -> None:
        """Update the batch timestamp for a schema/table.
        
        Args:
            schema_name: Name of the schema
            timestamp: Last processed timestamp
            table_name: Name of the table (None for schema-level)  
            sync_run_id: Associated sync run ID
        """
        position_data = {
            'timestamp': timestamp.isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        await self.update_sync_marker(
            schema_name=schema_name,
            position_data=position_data,
            table_name=table_name,
            marker_type=MarkerType.BATCH,
            sync_run_id=sync_run_id
        )

    # =====================
    # Schema Registry Management
    # =====================
    
    async def register_schema(
        self,
        schema_name: str,
        table_name: str,
        schema_definition: SchemaDefinition,
        evolution_type: Optional[EvolutionType] = None,
        registered_by: str = "cartridge-warp"
    ) -> SchemaRegistry:
        """Register a new schema version.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            schema_definition: Complete schema definition
            evolution_type: Type of schema evolution
            registered_by: Who registered this schema
            
        Returns:
            SchemaRegistry entry
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Get current version
                current_version = await conn.fetchval(
                    f"""
                    SELECT COALESCE(MAX(version), 0)
                    FROM {self.metadata_schema}.schema_registry
                    WHERE schema_name = $1 AND table_name = $2
                    """,
                    schema_name, table_name
                )
                
                new_version = current_version + 1
                schema_hash = schema_definition.schema_hash
                
                # Check if this exact schema already exists
                existing = await conn.fetchrow(
                    f"""
                    SELECT id, version FROM {self.metadata_schema}.schema_registry
                    WHERE schema_name = $1 AND table_name = $2 AND schema_hash = $3
                    """,
                    schema_name, table_name, schema_hash
                )
                
                if existing:
                    logger.info(
                        "Schema already exists",
                        schema_name=schema_name,
                        table_name=table_name,
                        version=existing['version']
                    )
                    existing_schema = await self.get_schema_version(schema_name, table_name, existing['version'])
                    if existing_schema:
                        return existing_schema
                    # If for some reason we can't fetch it, continue with new registration
                
                # Insert new schema version
                registry_id = uuid.uuid4()
                await conn.execute(
                    f"""
                    INSERT INTO {self.metadata_schema}.schema_registry
                        (id, schema_name, table_name, version, schema_definition, schema_hash,
                         evolution_type, previous_version, registered_by)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    registry_id, schema_name, table_name, new_version,
                    json.dumps(schema_definition.model_dump()), schema_hash,
                    evolution_type if evolution_type else None,
                    current_version if current_version > 0 else None,
                    registered_by
                )
                
                registry = SchemaRegistry(
                    id=registry_id,
                    schema_name=schema_name,
                    table_name=table_name,
                    version=new_version,
                    schema_definition=schema_definition,
                    evolution_type=evolution_type,
                    previous_version=current_version if current_version > 0 else None,
                    registered_by=registered_by
                )
                
                # Update cache
                cache_key = (schema_name, table_name)
                self._schema_cache[cache_key] = registry
                
                logger.info(
                    "Schema registered",
                    schema_name=schema_name,
                    table_name=table_name,
                    version=new_version,
                    evolution_type=evolution_type
                )
                
                return registry

    async def get_schema_version(
        self,
        schema_name: str,
        table_name: str,
        version: Optional[int] = None
    ) -> Optional[SchemaRegistry]:
        """Get specific or latest schema version.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table  
            version: Specific version (None for latest)
            
        Returns:
            SchemaRegistry entry or None
        """
        cache_key = (schema_name, table_name)
        if version is None and cache_key in self._schema_cache:
            return self._schema_cache[cache_key]
            
        async with self.pool.acquire() as conn:
            if version is not None:
                where_clause = "WHERE schema_name = $1 AND table_name = $2 AND version = $3"
                params = [schema_name, table_name, version]
            else:
                where_clause = """
                WHERE schema_name = $1 AND table_name = $2 
                ORDER BY version DESC LIMIT 1
                """
                params = [schema_name, table_name]
            
            row = await conn.fetchrow(
                f"""
                SELECT id, schema_name, table_name, version, schema_definition, schema_hash,
                       evolution_type, previous_version, compatibility_status, 
                       registered_at, registered_by
                FROM {self.metadata_schema}.schema_registry
                {where_clause}
                """,
                *params
            )
            
            if row:
                schema_def = SchemaDefinition(**row['schema_definition'])
                registry = SchemaRegistry(
                    id=row['id'],
                    schema_name=row['schema_name'],
                    table_name=row['table_name'],
                    version=row['version'],
                    schema_definition=schema_def,
                    evolution_type=EvolutionType(row['evolution_type']) if row['evolution_type'] else None,
                    previous_version=row['previous_version'],
                    registered_at=row['registered_at'],
                    registered_by=row['registered_by']
                )
                
                if version is None:
                    self._schema_cache[cache_key] = registry
                
                return registry
                
        return None

    # =====================
    # Sync Run Management
    # =====================
    
    async def start_sync_run(
        self,
        schema_name: str,
        sync_mode: SyncMode,
        config_hash: Optional[str] = None,
        source_info: Optional[Dict[str, Any]] = None,
        destination_info: Optional[Dict[str, Any]] = None,
        instance_id: Optional[str] = None,
        node_id: Optional[str] = None
    ) -> SyncRun:
        """Start a new sync run.
        
        Args:
            schema_name: Name of the schema being synced
            sync_mode: Type of sync operation
            config_hash: Hash of configuration used
            source_info: Source connection info (without credentials)
            destination_info: Destination connection info (without credentials)
            instance_id: Pod/container identifier
            node_id: Node identifier
            
        Returns:
            SyncRun entry
        """
        sync_run = SyncRun(
            schema_name=schema_name,
            sync_mode=sync_mode,
            status=SyncStatus.RUNNING,
            config_hash=config_hash,
            source_info=source_info,
            destination_info=destination_info,
            instance_id=instance_id,
            node_id=node_id
        )
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self.metadata_schema}.sync_runs
                    (id, schema_name, sync_mode, status, started_at, config_hash,
                     source_info, destination_info, instance_id, node_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                sync_run.id, sync_run.schema_name, sync_run.sync_mode,
                sync_run.status, sync_run.started_at, sync_run.config_hash,
                json.dumps(sync_run.source_info) if sync_run.source_info else None,
                json.dumps(sync_run.destination_info) if sync_run.destination_info else None,
                sync_run.instance_id, sync_run.node_id
            )
        
        logger.info(
            "Sync run started",
            sync_run_id=str(sync_run.id),
            schema_name=schema_name,
            sync_mode=sync_mode.value
        )
        
        return sync_run

    async def complete_sync_run(
        self,
        sync_run_id: UUID,
        status: SyncStatus,
        statistics: Optional[SyncRunStatistics] = None,
        error_message: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Complete a sync run with final status.
        
        Args:
            sync_run_id: ID of the sync run
            status: Final status
            statistics: Run statistics
            error_message: Error message if failed
            error_details: Additional error details
        """
        completed_at = datetime.now(timezone.utc)
        stats = statistics or SyncRunStatistics()
        
        async with self.pool.acquire() as conn:
            # Get start time to calculate duration
            start_time = await conn.fetchval(
                f"SELECT started_at FROM {self.metadata_schema}.sync_runs WHERE id = $1",
                sync_run_id
            )
            
            duration_ms = None
            if start_time:
                duration = completed_at - start_time
                duration_ms = int(duration.total_seconds() * 1000)
            
            await conn.execute(
                f"""
                UPDATE {self.metadata_schema}.sync_runs
                SET status = $2, completed_at = $3, duration_ms = $4,
                    records_processed = $5, records_inserted = $6, records_updated = $7,
                    records_deleted = $8, records_failed = $9, bytes_processed = $10,
                    error_message = $11, error_details = $12
                WHERE id = $1
                """,
                sync_run_id, status, completed_at, duration_ms,
                stats.records_processed, stats.records_inserted, stats.records_updated,
                stats.records_deleted, stats.records_failed, stats.bytes_processed,
                error_message, json.dumps(error_details) if error_details else None
            )
        
        logger.info(
            "Sync run completed",
            sync_run_id=str(sync_run_id),
            status=status,
            duration_ms=duration_ms,
            records_processed=stats.records_processed
        )

    # =====================  
    # Error Logging & Dead Letter Queue
    # =====================
    
    async def log_error(
        self,
        schema_name: str,
        error_type: ErrorType,
        error_message: str,
        table_name: Optional[str] = None,
        sync_run_id: Optional[UUID] = None,
        error_code: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
        stack_trace: Optional[str] = None,
        record_data: Optional[Dict[str, Any]] = None,
        operation_type: Optional[OperationType] = None,
        max_retries: int = 3
    ) -> ErrorLog:
        """Log an error with full context.
        
        Args:
            schema_name: Name of the schema
            error_type: Type of error
            error_message: Error message
            table_name: Name of the table (if applicable)
            sync_run_id: Associated sync run ID
            error_code: Specific error code
            error_details: Additional error details
            stack_trace: Full stack trace
            record_data: Record that caused the error
            operation_type: Type of operation that failed
            max_retries: Maximum retry attempts
            
        Returns:
            ErrorLog entry
        """
        error_log = ErrorLog(
            sync_run_id=sync_run_id,
            schema_name=schema_name,
            table_name=table_name,
            error_type=error_type,
            error_code=error_code,
            error_message=error_message,
            error_details=error_details,
            stack_trace=stack_trace,
            record_data=record_data,
            operation_type=operation_type,
            max_retries=max_retries
        )
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self.metadata_schema}.error_log
                    (id, sync_run_id, schema_name, table_name, error_type, error_code,
                     error_message, error_details, stack_trace, record_data, operation_type,
                     retry_count, max_retries, occurred_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                """,
                error_log.id, error_log.sync_run_id, error_log.schema_name, error_log.table_name,
                error_log.error_type, error_log.error_code, error_log.error_message,
                json.dumps(error_log.error_details) if error_log.error_details else None,
                error_log.stack_trace,
                json.dumps(error_log.record_data) if error_log.record_data else None,
                error_log.operation_type if error_log.operation_type else None,
                error_log.retry_count, error_log.max_retries, error_log.occurred_at
            )
        
        logger.error(
            "Error logged",
            error_id=str(error_log.id),
            schema_name=schema_name,
            table_name=table_name,
            error_type=error_type,
            error_message=error_message
        )
        
        return error_log

    async def add_to_dead_letter_queue(
        self,
        schema_name: str,
        table_name: str,
        operation_type: OperationType,
        record_data: Dict[str, Any],
        sync_run_id: Optional[UUID] = None,
        error_log_id: Optional[UUID] = None,
        source_record_id: Optional[str] = None,
        original_timestamp: Optional[datetime] = None,
        error_message: Optional[str] = None
    ) -> DeadLetterQueue:
        """Add a failed record to the dead letter queue.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            operation_type: Type of operation that failed
            record_data: The record data
            sync_run_id: Associated sync run ID
            error_log_id: Associated error log ID
            source_record_id: ID of the source record
            original_timestamp: Original record timestamp
            error_message: Latest error message
            
        Returns:
            DeadLetterQueue entry
        """
        dlq_record = DeadLetterQueue(
            sync_run_id=sync_run_id,
            error_log_id=error_log_id,
            schema_name=schema_name,
            table_name=table_name,
            source_record_id=source_record_id,
            operation_type=operation_type,
            record_data=record_data,
            original_timestamp=original_timestamp,
            last_error_message=error_message
        )
        
        async with self.pool.acquire() as conn:
            # Check if record already exists and increment error count
            existing = await conn.fetchrow(
                f"""
                SELECT id, error_count FROM {self.metadata_schema}.dead_letter_queue
                WHERE schema_name = $1 AND table_name = $2 
                  AND COALESCE(source_record_id, '') = COALESCE($3, '')
                  AND status IN ('pending', 'processing')
                """,
                schema_name, table_name, source_record_id
            )
            
            if existing:
                # Update existing record
                await conn.execute(
                    f"""
                    UPDATE {self.metadata_schema}.dead_letter_queue
                    SET error_count = error_count + 1,
                        last_error_at = NOW(),
                        last_error_message = $2,
                        error_log_id = COALESCE($3, error_log_id),
                        sync_run_id = COALESCE($4, sync_run_id)
                    WHERE id = $1
                    """,
                    existing['id'], error_message, error_log_id, sync_run_id
                )
                dlq_record.id = existing['id']
                dlq_record.error_count = existing['error_count'] + 1
            else:
                # Insert new record
                await conn.execute(
                    f"""
                    INSERT INTO {self.metadata_schema}.dead_letter_queue
                        (id, sync_run_id, error_log_id, schema_name, table_name,
                         source_record_id, operation_type, record_data, original_timestamp,
                         error_count, last_error_message, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    """,
                    dlq_record.id, dlq_record.sync_run_id, dlq_record.error_log_id,
                    dlq_record.schema_name, dlq_record.table_name, dlq_record.source_record_id,
                    dlq_record.operation_type, json.dumps(dlq_record.record_data),
                    dlq_record.original_timestamp, dlq_record.error_count,
                    dlq_record.last_error_message, dlq_record.status
                )
        
        logger.warning(
            "Record added to dead letter queue",
            dlq_id=str(dlq_record.id),
            schema_name=schema_name,
            table_name=table_name,
            operation_type=operation_type,
            error_count=dlq_record.error_count
        )
        
        return dlq_record

    # =====================
    # Recovery and Cleanup Operations
    # =====================
    
    async def recover_failed_runs(self, max_age_hours: int = 24) -> List[UUID]:
        """Recover sync runs that failed to complete properly.
        
        Args:
            max_age_hours: Maximum age of runs to consider for recovery
            
        Returns:
            List of recovered sync run IDs
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        recovered_runs = []
        
        async with self.pool.acquire() as conn:
            # Find runs that are stuck in RUNNING status
            stuck_runs = await conn.fetch(
                f"""
                SELECT id, schema_name, started_at
                FROM {self.metadata_schema}.sync_runs
                WHERE status = 'running' AND started_at < $1
                """,
                cutoff_time
            )
            
            for run in stuck_runs:
                await conn.execute(
                    f"""
                    UPDATE {self.metadata_schema}.sync_runs
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = 'Run recovered after timeout',
                        duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000
                    WHERE id = $1
                    """,
                    run['id']
                )
                recovered_runs.append(run['id'])
                
                logger.warning(
                    "Recovered stuck sync run",
                    sync_run_id=str(run['id']),
                    schema_name=run['schema_name'],
                    started_at=run['started_at']
                )
        
        return recovered_runs

    async def cleanup_old_metadata(self) -> Dict[str, int]:
        """Clean up old metadata based on retention policy.
        
        Returns:
            Dict with counts of cleaned up records
        """
        if not self.enable_cleanup:
            return {}
            
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        cleanup_stats = {}
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Clean up old sync runs and related data
                cleanup_stats['sync_runs'] = await conn.fetchval(
                    f"""
                    DELETE FROM {self.metadata_schema}.sync_runs
                    WHERE completed_at < $1 AND status IN ('completed', 'failed', 'cancelled')
                    """,
                    cutoff_date
                ) or 0
                
                # Clean up resolved errors
                cleanup_stats['error_log'] = await conn.fetchval(
                    f"""
                    DELETE FROM {self.metadata_schema}.error_log
                    WHERE resolved_at < $1 AND status = 'resolved'
                    """,
                    cutoff_date
                ) or 0
                
                # Clean up resolved DLQ records
                cleanup_stats['dead_letter_queue'] = await conn.fetchval(
                    f"""
                    DELETE FROM {self.metadata_schema}.dead_letter_queue
                    WHERE processed_at < $1 AND status IN ('resolved', 'discarded')
                    """,
                    cutoff_date
                ) or 0
                
                # Keep only latest schema versions (retain last 10 versions)
                cleanup_stats['schema_registry'] = await conn.fetchval(
                    f"""
                    DELETE FROM {self.metadata_schema}.schema_registry sr1
                    WHERE EXISTS (
                        SELECT 1 FROM {self.metadata_schema}.schema_registry sr2
                        WHERE sr2.schema_name = sr1.schema_name 
                          AND sr2.table_name = sr1.table_name
                          AND sr2.version > sr1.version + 10
                    ) AND sr1.registered_at < $1
                    """,
                    cutoff_date
                ) or 0
        
        if any(cleanup_stats.values()):
            logger.info("Metadata cleanup completed", **cleanup_stats)
        
        return cleanup_stats

    async def _background_cleanup(self) -> None:
        """Background task for periodic metadata cleanup."""
        retry_interval = self.retry_initial_interval_seconds
        consecutive_failures = 0
        
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval_seconds)
                await self.cleanup_old_metadata()
                await self.recover_failed_runs()
                # Reset backoff after success
                retry_interval = self.retry_initial_interval_seconds
                consecutive_failures = 0
            except Exception as e:
                logger.error("Background cleanup failed", error=str(e))
                consecutive_failures += 1
                # Exponential backoff, capped at max
                retry_interval = min(
                    self.retry_initial_interval_seconds * (2 ** (consecutive_failures - 1)), 
                    self.retry_max_interval_seconds
                )
                await asyncio.sleep(retry_interval)  # Wait before retrying

    # =====================
    # Query and Reporting Methods
    # =====================
    
    async def get_sync_statistics(
        self,
        schema_name: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get sync statistics for monitoring and reporting.
        
        Args:
            schema_name: Specific schema name (None for all)
            hours: Number of hours to look back
            
        Returns:
            Dictionary with comprehensive statistics
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        async with self.pool.acquire() as conn:
            where_clause = "WHERE started_at >= $1"
            params: List[Any] = [since]
            
            if schema_name:
                where_clause += " AND schema_name = $2"
                params.append(schema_name)
            
            # Basic run statistics
            runs_stats = await conn.fetchrow(
                f"""
                SELECT 
                    COUNT(*) as total_runs,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_runs,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_runs,
                    COUNT(CASE WHEN status = 'running' THEN 1 END) as running_runs,
                    AVG(duration_ms) as avg_duration_ms,
                    SUM(records_processed) as total_records_processed,
                    SUM(bytes_processed) as total_bytes_processed
                FROM {self.metadata_schema}.sync_runs
                {where_clause}
                """,
                *params
            )
            
            # Error statistics
            error_stats = await conn.fetchrow(
                f"""
                SELECT 
                    COUNT(*) as total_errors,
                    COUNT(CASE WHEN status = 'open' THEN 1 END) as open_errors,
                    COUNT(CASE WHEN retry_count > 0 THEN 1 END) as retried_errors
                FROM {self.metadata_schema}.error_log
                WHERE occurred_at >= $1
                {" AND schema_name = $2" if schema_name else ""}
                """,
                *params
            )
            
            # DLQ statistics
            dlq_stats = await conn.fetchrow(
                f"""
                SELECT 
                    COUNT(*) as total_dlq_records,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_dlq_records,
                    AVG(error_count) as avg_error_count
                FROM {self.metadata_schema}.dead_letter_queue
                WHERE first_error_at >= $1
                {" AND schema_name = $2" if schema_name else ""}
                """,
                *params
            )
            
            return {
                'time_range_hours': hours,
                'schema_name': schema_name,
                'sync_runs': dict(runs_stats) if runs_stats else {},
                'errors': dict(error_stats) if error_stats else {},
                'dead_letter_queue': dict(dlq_stats) if dlq_stats else {},
                'generated_at': datetime.now(timezone.utc).isoformat()
            }

    async def get_active_markers(self) -> List[SyncMarker]:
        """Get all active sync markers.
        
        Returns:
            List of all sync markers
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, schema_name, table_name, marker_type, position_data,
                       last_updated, sync_run_id, created_at
                FROM {self.metadata_schema}.sync_markers
                ORDER BY schema_name, table_name, marker_type
                """
            )
            
            return [
                SyncMarker(
                    id=row['id'],
                    schema_name=row['schema_name'],
                    table_name=row['table_name'],
                    marker_type=MarkerType(row['marker_type']),
                    position_data=row['position_data'],
                    last_updated=row['last_updated'],
                    sync_run_id=row['sync_run_id'],
                    created_at=row['created_at']
                )
                for row in rows
            ]
