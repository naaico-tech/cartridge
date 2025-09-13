"""Database schema definitions for metadata management.

This module defines the SQL schema for comprehensive metadata management including:
- Sync position tracking 
- Schema registry and versioning
- Execution monitoring
- Error logging and recovery
"""

from typing import Dict, List

# Metadata schema SQL definitions
METADATA_TABLES_SQL = {
    "sync_markers": """
    CREATE TABLE IF NOT EXISTS cartridge_warp.sync_markers (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        schema_name VARCHAR(255) NOT NULL,
        table_name VARCHAR(255),
        marker_type VARCHAR(50) NOT NULL, -- 'stream', 'batch', 'initial'
        position_data JSONB NOT NULL, -- LSN, resume token, timestamp, etc.
        last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        sync_run_id UUID,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
    
    -- Create unique index instead of constraint with COALESCE
    CREATE UNIQUE INDEX IF NOT EXISTS uk_sync_markers_schema_table_type 
        ON cartridge_warp.sync_markers (schema_name, COALESCE(table_name, ''), marker_type);
    
    CREATE INDEX IF NOT EXISTS idx_sync_markers_schema_name ON cartridge_warp.sync_markers (schema_name);
    CREATE INDEX IF NOT EXISTS idx_sync_markers_table_name ON cartridge_warp.sync_markers (table_name);
    CREATE INDEX IF NOT EXISTS idx_sync_markers_last_updated ON cartridge_warp.sync_markers (last_updated);
    CREATE INDEX IF NOT EXISTS idx_sync_markers_sync_run_id ON cartridge_warp.sync_markers (sync_run_id);
    """,
    
    "schema_registry": """
    CREATE TABLE IF NOT EXISTS cartridge_warp.schema_registry (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        schema_name VARCHAR(255) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        schema_definition JSONB NOT NULL, -- Full table schema with columns, types, constraints
        schema_hash VARCHAR(64) NOT NULL, -- SHA-256 of normalized schema
        evolution_type VARCHAR(50), -- 'create', 'add_column', 'modify_column', 'drop_column'
        previous_version INTEGER,
        compatibility_status VARCHAR(50) NOT NULL DEFAULT 'compatible', -- 'compatible', 'breaking', 'unknown'
        registered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        registered_by VARCHAR(255) DEFAULT 'cartridge-warp',
        
        CONSTRAINT uk_schema_registry_schema_table_version 
            UNIQUE (schema_name, table_name, version)
    );
    
    CREATE INDEX IF NOT EXISTS idx_schema_registry_schema_name ON cartridge_warp.schema_registry (schema_name);
    CREATE INDEX IF NOT EXISTS idx_schema_registry_table_name ON cartridge_warp.schema_registry (table_name);
    CREATE INDEX IF NOT EXISTS idx_schema_registry_version ON cartridge_warp.schema_registry (version);
    CREATE INDEX IF NOT EXISTS idx_schema_registry_hash ON cartridge_warp.schema_registry (schema_hash);
    CREATE INDEX IF NOT EXISTS idx_schema_registry_registered_at ON cartridge_warp.schema_registry (registered_at);
    """,
    
    "sync_runs": """
    CREATE TABLE IF NOT EXISTS cartridge_warp.sync_runs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        schema_name VARCHAR(255) NOT NULL,
        sync_mode VARCHAR(50) NOT NULL, -- 'stream', 'batch', 'initial'
        status VARCHAR(50) NOT NULL, -- 'running', 'completed', 'failed', 'cancelled'
        started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMP WITH TIME ZONE,
        duration_ms BIGINT,
        
        -- Statistics
        records_processed BIGINT DEFAULT 0,
        records_inserted BIGINT DEFAULT 0,
        records_updated BIGINT DEFAULT 0,
        records_deleted BIGINT DEFAULT 0,
        records_failed BIGINT DEFAULT 0,
        bytes_processed BIGINT DEFAULT 0,
        
        -- Configuration
        config_hash VARCHAR(64), -- Hash of config used for this run
        source_info JSONB, -- Source connection info (without credentials)
        destination_info JSONB, -- Destination connection info (without credentials)
        
        -- Error information
        error_message TEXT,
        error_details JSONB,
        
        -- Metadata
        instance_id VARCHAR(255), -- Pod/container identifier
        node_id VARCHAR(255), -- Node identifier
        created_by VARCHAR(255) DEFAULT 'cartridge-warp'
    );
    
    CREATE INDEX IF NOT EXISTS idx_sync_runs_schema_name ON cartridge_warp.sync_runs (schema_name);
    CREATE INDEX IF NOT EXISTS idx_sync_runs_status ON cartridge_warp.sync_runs (status);
    CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON cartridge_warp.sync_runs (started_at);
    CREATE INDEX IF NOT EXISTS idx_sync_runs_completed_at ON cartridge_warp.sync_runs (completed_at);
    CREATE INDEX IF NOT EXISTS idx_sync_runs_instance_id ON cartridge_warp.sync_runs (instance_id);
    """,
    
    "error_log": """
    CREATE TABLE IF NOT EXISTS cartridge_warp.error_log (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        sync_run_id UUID,
        schema_name VARCHAR(255) NOT NULL,
        table_name VARCHAR(255),
        error_type VARCHAR(100) NOT NULL, -- 'connection', 'transformation', 'constraint', 'schema'
        error_code VARCHAR(50),
        error_message TEXT NOT NULL,
        error_details JSONB,
        stack_trace TEXT,
        
        -- Context information
        record_data JSONB, -- The record that caused the error (if applicable)
        operation_type VARCHAR(50), -- 'insert', 'update', 'delete', 'schema_change'
        retry_count INTEGER DEFAULT 0,
        max_retries INTEGER DEFAULT 3,
        retry_after TIMESTAMP WITH TIME ZONE,
        
        -- Status
        status VARCHAR(50) NOT NULL DEFAULT 'open', -- 'open', 'resolved', 'ignored'
        resolved_at TIMESTAMP WITH TIME ZONE,
        resolved_by VARCHAR(255),
        resolution_notes TEXT,
        
        occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        
        FOREIGN KEY (sync_run_id) REFERENCES cartridge_warp.sync_runs(id) ON DELETE SET NULL
    );
    
    CREATE INDEX IF NOT EXISTS idx_error_log_sync_run_id ON cartridge_warp.error_log (sync_run_id);
    CREATE INDEX IF NOT EXISTS idx_error_log_schema_name ON cartridge_warp.error_log (schema_name);
    CREATE INDEX IF NOT EXISTS idx_error_log_table_name ON cartridge_warp.error_log (table_name);
    CREATE INDEX IF NOT EXISTS idx_error_log_error_type ON cartridge_warp.error_log (error_type);
    CREATE INDEX IF NOT EXISTS idx_error_log_status ON cartridge_warp.error_log (status);
    CREATE INDEX IF NOT EXISTS idx_error_log_occurred_at ON cartridge_warp.error_log (occurred_at);
    CREATE INDEX IF NOT EXISTS idx_error_log_retry_after ON cartridge_warp.error_log (retry_after);
    """,
    
    "dead_letter_queue": """
    CREATE TABLE IF NOT EXISTS cartridge_warp.dead_letter_queue (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        sync_run_id UUID,
        error_log_id UUID,
        schema_name VARCHAR(255) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        
        -- Record information
        source_record_id VARCHAR(255),
        operation_type VARCHAR(50) NOT NULL, -- 'insert', 'update', 'delete'
        record_data JSONB NOT NULL,
        original_timestamp TIMESTAMP WITH TIME ZONE,
        
        -- Error context
        error_count INTEGER NOT NULL DEFAULT 1,
        first_error_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        last_error_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        last_error_message TEXT,
        
        -- Processing status
        status VARCHAR(50) NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'resolved', 'discarded'
        processed_at TIMESTAMP WITH TIME ZONE,
        processed_by VARCHAR(255),
        resolution_method VARCHAR(100), -- 'reprocessed', 'manual_fix', 'discarded', 'schema_updated'
        
        FOREIGN KEY (sync_run_id) REFERENCES cartridge_warp.sync_runs(id) ON DELETE SET NULL,
        FOREIGN KEY (error_log_id) REFERENCES cartridge_warp.error_log(id) ON DELETE SET NULL
    );
    
    CREATE INDEX IF NOT EXISTS idx_dlq_sync_run_id ON cartridge_warp.dead_letter_queue (sync_run_id);
    CREATE INDEX IF NOT EXISTS idx_dlq_error_log_id ON cartridge_warp.dead_letter_queue (error_log_id);
    CREATE INDEX IF NOT EXISTS idx_dlq_schema_table ON cartridge_warp.dead_letter_queue (schema_name, table_name);
    CREATE INDEX IF NOT EXISTS idx_dlq_status ON cartridge_warp.dead_letter_queue (status);
    CREATE INDEX IF NOT EXISTS idx_dlq_first_error_at ON cartridge_warp.dead_letter_queue (first_error_at);
    CREATE INDEX IF NOT EXISTS idx_dlq_last_error_at ON cartridge_warp.dead_letter_queue (last_error_at);
    """
}

# Schema creation order (respects foreign key dependencies)
TABLE_CREATION_ORDER = [
    "sync_runs",
    "sync_markers", 
    "schema_registry",
    "error_log",
    "dead_letter_queue"
]

def get_schema_creation_sql(schema_name: str = "cartridge_warp") -> List[str]:
    """Get SQL statements for creating metadata schema in correct order.
    
    Args:
        schema_name: Name of the schema to create (default: cartridge_warp)
    
    Returns:
        List of SQL statements to execute in order
    """
    statements = [
        f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";',
        f'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'
    ]
    
    for table_name in TABLE_CREATION_ORDER:
        if table_name in METADATA_TABLES_SQL:
            # Replace hardcoded schema name with the provided schema_name
            sql = METADATA_TABLES_SQL[table_name].replace("cartridge_warp", schema_name)
            statements.append(sql)
    
    return statements

def get_schema_cleanup_sql(schema_name: str = "cartridge_warp") -> List[str]:
    """Get SQL statements for cleaning up metadata schema.
    
    Args:
        schema_name: Name of the schema to clean up (default: cartridge_warp)
    
    Returns:
        List of SQL statements to execute for cleanup
    """
    return [
        f'DROP TABLE IF EXISTS "{schema_name}".dead_letter_queue CASCADE;',
        f'DROP TABLE IF EXISTS "{schema_name}".error_log CASCADE;',
        f'DROP TABLE IF EXISTS "{schema_name}".schema_registry CASCADE;',
        f'DROP TABLE IF EXISTS "{schema_name}".sync_markers CASCADE;',
        f'DROP TABLE IF EXISTS "{schema_name}".sync_runs CASCADE;',
        f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;'
    ]
