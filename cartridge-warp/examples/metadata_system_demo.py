#!/usr/bin/env python3
"""Demonstration of the comprehensive metadata management system.

This script shows how to use the new metadata management features including:
- Position tracking for CDC operations
- Schema evolution management
- Sync run monitoring
- Error handling and dead letter queue
- Recovery operations

Usage:
    python demo_metadata_system.py [--database-url DATABASE_URL]
"""

import asyncio
import asyncpg
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from cartridge_warp.metadata import (
    MetadataManager,
    MarkerType,
    SyncMode,
    SyncStatus,
    ErrorType,
    OperationType,
    EvolutionType,
    SchemaDefinition,
    SyncRunStatistics,
)


async def demo_metadata_system(database_url: str = "postgresql://localhost/cartridge_demo"):
    """Demonstrate the metadata management system."""
    print("🚀 Starting Cartridge-Warp Metadata Management System Demo")
    print("=" * 60)
    
    # Create connection pool
    pool = await asyncpg.create_pool(database_url, min_size=2, max_size=10)
    
    try:
        # Initialize metadata manager
        metadata_manager = MetadataManager(
            connection_pool=pool,
            metadata_schema="cartridge_warp",
            enable_cleanup=True,
            retention_days=30
        )
        
        print("\n📊 Initializing metadata system...")
        await metadata_manager.initialize()
        print("✅ Metadata system initialized successfully!")
        
        # Demo 1: Position Tracking
        print("\n" + "=" * 60)
        print("DEMO 1: CDC Position Tracking")
        print("=" * 60)
        
        await demo_position_tracking(metadata_manager)
        
        # Demo 2: Schema Evolution
        print("\n" + "=" * 60) 
        print("DEMO 2: Schema Evolution Management")
        print("=" * 60)
        
        await demo_schema_evolution(metadata_manager)
        
        # Demo 3: Sync Run Lifecycle
        print("\n" + "=" * 60)
        print("DEMO 3: Sync Run Monitoring")
        print("=" * 60)
        
        await demo_sync_run_lifecycle(metadata_manager)
        
        # Demo 4: Error Handling & Dead Letter Queue
        print("\n" + "=" * 60)
        print("DEMO 4: Error Handling & Dead Letter Queue")
        print("=" * 60)
        
        await demo_error_handling(metadata_manager)
        
        # Demo 5: Recovery Operations
        print("\n" + "=" * 60)
        print("DEMO 5: Recovery & Monitoring")
        print("=" * 60)
        
        await demo_recovery_operations(metadata_manager)
        
        print("\n🎉 Demo completed successfully!")
        print("The metadata system is ready for production use.")
        
    finally:
        await pool.close()


async def demo_position_tracking(manager: MetadataManager):
    """Demonstrate position tracking capabilities."""
    schema_name = "ecommerce"
    
    print(f"📍 Position Tracking Demo for schema: {schema_name}")
    
    # 1. Stream position tracking (MongoDB Change Streams)
    print("\n1. Stream Position Tracking (MongoDB Change Streams)")
    
    # Simulate initial position
    initial_position = {
        "resume_token": {
            "_data": "8265A4F2EC000000012B0229296E04"
        },
        "cluster_time": "7123456789012345678",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    await manager.update_stream_position(schema_name, initial_position)
    print(f"   ✅ Initial stream position stored: {initial_position['resume_token']}")
    
    # Simulate progress
    progress_position = {
        "resume_token": {
            "_data": "8265A4F2EC000000022B0229296E04"
        },
        "cluster_time": "7123456789012345679",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    await manager.update_stream_position(schema_name, progress_position)
    print(f"   ✅ Updated stream position: {progress_position['resume_token']}")
    
    # Retrieve current position
    current_position = await manager.get_stream_position(schema_name)
    if current_position:
        print(f"   📖 Current stream position: {current_position['resume_token']}")
    
    # 2. Batch timestamp tracking
    print("\n2. Batch Timestamp Tracking")
    
    # Initial batch timestamp
    initial_timestamp = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    await manager.update_batch_timestamp(schema_name, initial_timestamp, "orders")
    print(f"   ✅ Initial batch timestamp: {initial_timestamp}")
    
    # Progress batch timestamp
    progress_timestamp = datetime.now(timezone.utc) - timedelta(hours=1)
    await manager.update_batch_timestamp(schema_name, progress_timestamp, "orders")
    print(f"   ✅ Updated batch timestamp: {progress_timestamp}")
    
    # Retrieve current timestamp
    current_timestamp = await manager.get_batch_timestamp(schema_name, "orders")
    if current_timestamp:
        print(f"   📖 Current batch timestamp: {current_timestamp}")


async def demo_schema_evolution(manager: MetadataManager):
    """Demonstrate schema evolution management."""
    schema_name = "ecommerce"
    table_name = "orders"
    
    print(f"🏗️  Schema Evolution Demo for table: {schema_name}.{table_name}")
    
    # 1. Initial schema registration
    print("\n1. Initial Schema Registration")
    
    initial_schema = SchemaDefinition(
        columns=[
            {"name": "id", "type": "INTEGER", "nullable": False, "primary_key": True},
            {"name": "customer_id", "type": "INTEGER", "nullable": False},
            {"name": "order_date", "type": "TIMESTAMP", "nullable": False},
            {"name": "status", "type": "VARCHAR", "nullable": False, "max_length": 20},
            {"name": "total_amount", "type": "DECIMAL", "nullable": False, "precision": 10, "scale": 2}
        ],
        primary_keys=["id"],
        indexes=[
            {"name": "idx_customer_id", "columns": ["customer_id"]},
            {"name": "idx_order_date", "columns": ["order_date"]}
        ]
    )
    
    registry_v1 = await manager.register_schema(
        schema_name=schema_name,
        table_name=table_name,
        schema_definition=initial_schema,
        evolution_type=EvolutionType.CREATE,
        registered_by="demo_script"
    )
    
    print(f"   ✅ Schema v{registry_v1.version} registered")
    print(f"   📋 Schema hash: {registry_v1.schema_hash[:16]}...")
    
    # 2. Schema evolution - add column
    print("\n2. Schema Evolution - Add Column")
    
    evolved_schema = SchemaDefinition(
        columns=[
            {"name": "id", "type": "INTEGER", "nullable": False, "primary_key": True},
            {"name": "customer_id", "type": "INTEGER", "nullable": False},
            {"name": "order_date", "type": "TIMESTAMP", "nullable": False},
            {"name": "status", "type": "VARCHAR", "nullable": False, "max_length": 20},
            {"name": "total_amount", "type": "DECIMAL", "nullable": False, "precision": 10, "scale": 2},
            {"name": "discount_amount", "type": "DECIMAL", "nullable": True, "precision": 10, "scale": 2, "default": "0.00"},  # New column
            {"name": "shipping_address", "type": "JSONB", "nullable": True}  # New column
        ],
        primary_keys=["id"],
        indexes=[
            {"name": "idx_customer_id", "columns": ["customer_id"]},
            {"name": "idx_order_date", "columns": ["order_date"]},
            {"name": "idx_total_amount", "columns": ["total_amount"]}  # New index
        ]
    )
    
    registry_v2 = await manager.register_schema(
        schema_name=schema_name,
        table_name=table_name,
        schema_definition=evolved_schema,
        evolution_type=EvolutionType.ADD_COLUMN,
        registered_by="demo_script"
    )
    
    print(f"   ✅ Schema v{registry_v2.version} registered (added columns)")
    print(f"   📋 Schema hash: {registry_v2.schema_hash[:16]}...")
    print(f"   🔗 Previous version: {registry_v2.previous_version}")
    
    # 3. Retrieve schema versions
    print("\n3. Schema Version Retrieval")
    
    latest_schema = await manager.get_schema_version(schema_name, table_name)
    if latest_schema:
        print(f"   📖 Latest schema version: {latest_schema.version}")
        print(f"   📊 Column count: {len(latest_schema.schema_definition.columns)}")
        print(f"   🏷️  Evolution type: {latest_schema.evolution_type}")
    
    v1_schema = await manager.get_schema_version(schema_name, table_name, version=1)
    if v1_schema:
        print(f"   📖 Version 1 column count: {len(v1_schema.schema_definition.columns)}")


async def demo_sync_run_lifecycle(manager: MetadataManager):
    """Demonstrate sync run monitoring."""
    schema_name = "ecommerce"
    
    print(f"📊 Sync Run Monitoring Demo for schema: {schema_name}")
    
    # 1. Start a streaming sync run
    print("\n1. Starting Stream Sync Run")
    
    sync_run = await manager.start_sync_run(
        schema_name=schema_name,
        sync_mode=SyncMode.STREAM,
        config_hash="abc123def456",
        source_info={
            "type": "mongodb",
            "host": "localhost",
            "database": "ecommerce_prod"
            # Note: credentials are excluded from metadata
        },
        destination_info={
            "type": "postgresql", 
            "host": "localhost",
            "database": "analytics_warehouse"
        },
        instance_id="cartridge-warp-pod-123",
        node_id="worker-node-01"
    )
    
    print(f"   ✅ Sync run started: {sync_run.id}")
    print(f"   📝 Mode: {sync_run.sync_mode.value}")
    print(f"   📊 Status: {sync_run.status.value}")
    print(f"   🏷️  Instance: {sync_run.instance_id}")
    
    # Simulate some processing time
    await asyncio.sleep(0.1)
    
    # 2. Complete the sync run with statistics
    print("\n2. Completing Sync Run")
    
    statistics = SyncRunStatistics(
        records_processed=10000,
        records_inserted=8500,
        records_updated=1200,
        records_deleted=300,
        bytes_processed=5 * 1024 * 1024  # 5MB
    )
    
    await manager.complete_sync_run(
        sync_run_id=sync_run.id,
        status=SyncStatus.COMPLETED,
        statistics=statistics
    )
    
    print(f"   ✅ Sync run completed successfully")
    print(f"   📊 Records processed: {statistics.records_processed:,}")
    print(f"   📝 Records inserted: {statistics.records_inserted:,}")
    print(f"   🔄 Records updated: {statistics.records_updated:,}")
    print(f"   🗑️  Records deleted: {statistics.records_deleted:,}")
    print(f"   💾 Bytes processed: {statistics.bytes_processed:,}")
    
    # 3. Start a batch sync that will fail
    print("\n3. Simulating Failed Sync Run")
    
    failed_run = await manager.start_sync_run(
        schema_name=schema_name,
        sync_mode=SyncMode.BATCH,
        config_hash="xyz789abc123",
        instance_id="cartridge-warp-pod-124"
    )
    
    await asyncio.sleep(0.1)
    
    await manager.complete_sync_run(
        sync_run_id=failed_run.id,
        status=SyncStatus.FAILED,
        error_message="Connection timeout during batch processing",
        error_details={
            "error_code": "TIMEOUT_001",
            "timeout_seconds": 30,
            "last_successful_batch": "2023-12-01T10:30:00Z"
        }
    )
    
    print(f"   ❌ Sync run failed: {failed_run.id}")
    print(f"   🚨 Error: Connection timeout during batch processing")


async def demo_error_handling(manager: MetadataManager):
    """Demonstrate error logging and dead letter queue."""
    schema_name = "ecommerce"
    table_name = "orders"
    
    print(f"🚨 Error Handling & Dead Letter Queue Demo")
    
    # Start a sync run for context
    sync_run = await manager.start_sync_run(
        schema_name=schema_name,
        sync_mode=SyncMode.STREAM,
        instance_id="cartridge-warp-pod-error-demo"
    )
    
    # 1. Log various types of errors
    print("\n1. Logging Different Error Types")
    
    # Connection error
    connection_error = await manager.log_error(
        schema_name=schema_name,
        error_type=ErrorType.CONNECTION,
        error_message="Failed to connect to source database",
        sync_run_id=sync_run.id,
        error_code="CONN_001",
        error_details={
            "host": "mongodb.prod.local",
            "port": 27017,
            "timeout": 30
        },
        stack_trace="ConnectionError: [Errno 111] Connection refused",
        max_retries=5
    )
    
    print(f"   🔌 Connection error logged: {connection_error.id}")
    
    # Schema error with record data
    problematic_record = {
        "id": 12345,
        "customer_id": "invalid_id",  # Should be integer
        "order_date": "2023-13-45",   # Invalid date
        "total_amount": "not_a_number"  # Should be decimal
    }
    
    schema_error = await manager.log_error(
        schema_name=schema_name,
        table_name=table_name,
        error_type=ErrorType.VALIDATION,
        error_message="Data validation failed: invalid types",
        sync_run_id=sync_run.id,
        error_code="VAL_001",
        record_data=problematic_record,
        operation_type=OperationType.INSERT,
        max_retries=3
    )
    
    print(f"   📋 Validation error logged: {schema_error.id}")
    
    # 2. Add records to dead letter queue
    print("\n2. Dead Letter Queue Management")
    
    # Add problematic record to DLQ
    dlq_record = await manager.add_to_dead_letter_queue(
        schema_name=schema_name,
        table_name=table_name,
        operation_type=OperationType.INSERT,
        record_data=problematic_record,
        sync_run_id=sync_run.id,
        error_log_id=schema_error.id,
        source_record_id="mongo_obj_id_12345",
        error_message="Validation failed: multiple type errors"
    )
    
    print(f"   📫 Record added to DLQ: {dlq_record.id}")
    print(f"   🔢 Error count: {dlq_record.error_count}")
    print(f"   📊 Status: {dlq_record.status.value}")
    
    # Simulate the same record failing again
    dlq_record2 = await manager.add_to_dead_letter_queue(
        schema_name=schema_name,
        table_name=table_name,
        operation_type=OperationType.INSERT,
        record_data=problematic_record,
        source_record_id="mongo_obj_id_12345",
        error_message="Still failing validation after schema fix attempt"
    )
    
    print(f"   🔄 Record error count incremented: {dlq_record2.error_count}")
    
    # Complete the sync run as failed
    await manager.complete_sync_run(
        sync_run_id=sync_run.id,
        status=SyncStatus.FAILED,
        error_message="Multiple validation errors encountered"
    )


async def demo_recovery_operations(manager: MetadataManager):
    """Demonstrate recovery and monitoring operations."""
    print(f"🔧 Recovery & Monitoring Demo")
    
    # 1. Get comprehensive statistics
    print("\n1. System Statistics")
    
    stats = await manager.get_sync_statistics(hours=24)
    
    print(f"   📊 Statistics for last 24 hours:")
    print(f"   📈 Total sync runs: {stats['sync_runs'].get('total_runs', 0)}")
    print(f"   ✅ Completed runs: {stats['sync_runs'].get('completed_runs', 0)}")
    print(f"   ❌ Failed runs: {stats['sync_runs'].get('failed_runs', 0)}")
    print(f"   🏃 Running runs: {stats['sync_runs'].get('running_runs', 0)}")
    print(f"   🚨 Total errors: {stats['errors'].get('total_errors', 0)}")
    print(f"   📫 DLQ records: {stats['dead_letter_queue'].get('total_dlq_records', 0)}")
    
    if stats['sync_runs'].get('total_records_processed'):
        print(f"   📝 Records processed: {stats['sync_runs']['total_records_processed']:,}")
    
    if stats['sync_runs'].get('avg_duration_ms'):
        avg_seconds = stats['sync_runs']['avg_duration_ms'] / 1000
        print(f"   ⏱️  Average run duration: {avg_seconds:.2f}s")
    
    # 2. Recovery operations
    print("\n2. Recovery Operations")
    
    # Simulate recovery (would find actually stuck runs in real scenario)
    recovered_runs = await manager.recover_failed_runs(max_age_hours=1)
    
    if recovered_runs:
        print(f"   🔧 Recovered {len(recovered_runs)} stuck sync runs")
        for run_id in recovered_runs:
            print(f"      - {run_id}")
    else:
        print(f"   ✅ No stuck sync runs found")
    
    # 3. Show active markers
    print("\n3. Active Position Markers")
    
    markers = await manager.get_active_markers()
    
    if markers:
        print(f"   📍 Found {len(markers)} active position markers:")
        for marker in markers:
            table_info = f".{marker.table_name}" if marker.table_name else ""
            print(f"      - {marker.schema_name}{table_info} ({marker.marker_type.value})")
            print(f"        Last updated: {marker.last_updated.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    else:
        print(f"   📍 No active position markers found")
    
    # 4. Cleanup operations
    print("\n4. Cleanup Operations")
    
    # Force cleanup (normally runs automatically in background)
    cleanup_stats = await manager.cleanup_old_metadata()
    
    if any(cleanup_stats.values()):
        print(f"   🧹 Cleanup completed:")
        for table, count in cleanup_stats.items():
            if count > 0:
                print(f"      - {table}: {count} records cleaned")
    else:
        print(f"   ✅ No old metadata to clean up")


async def main():
    """Main demo function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Demo the comprehensive metadata management system"
    )
    parser.add_argument(
        "--database-url",
        default="postgresql://localhost/cartridge_demo",
        help="PostgreSQL database URL for the demo"
    )
    
    args = parser.parse_args()
    
    try:
        await demo_metadata_system(args.database_url)
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        print(f"Make sure PostgreSQL is running and accessible at: {args.database_url}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
