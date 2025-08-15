#!/usr/bin/env python3
"""
Example script demonstrating MongoDB source connector usage.

This script shows how to:
1. Connect to MongoDB
2. Discover schema from collections
3. Take a full snapshot of data
4. Monitor for changes (timestamp-based)

Prerequisites:
- MongoDB running on localhost:27017
- Sample data in the database

Usage:
    python examples/mongodb_example.py
"""

import asyncio
import json
from datetime import datetime, timezone

from cartridge_warp.connectors.mongodb_source import MongoDBSourceConnector


async def main():
    """Demonstrate MongoDB connector functionality."""
    print("MongoDB Source Connector Example")
    print("=" * 40)
    
    # Initialize connector
    connector = MongoDBSourceConnector(
        connection_string="mongodb://localhost:27017",
        database="sample_database",
        change_detection_column="updated_at",
        change_detection_strategy="timestamp"
    )
    
    try:
        # Connect to MongoDB
        print("Connecting to MongoDB...")
        await connector.connect()
        print("✅ Connected successfully!")
        
        # Test connection
        print("\nTesting connection...")
        is_connected = await connector.test_connection()
        print(f"✅ Connection test: {'Passed' if is_connected else 'Failed'}")
        
        # Discover schema
        print("\nDiscovering schema...")
        schema = await connector.get_schema("sample_schema")
        print(f"📊 Found {len(schema.tables)} collections:")
        
        for table in schema.tables:
            print(f"  - {table.name} ({len(table.columns)} fields)")
            
            # Show sample columns
            sample_columns = table.columns[:5]  # First 5 columns
            for col in sample_columns:
                print(f"    • {col.name}: {col.type.value}")
            
            if len(table.columns) > 5:
                print(f"    ... and {len(table.columns) - 5} more")
        
        # Demonstrate full snapshot (limit to first collection)
        if schema.tables:
            collection_name = schema.tables[0].name
            print(f"\n📸 Taking snapshot of '{collection_name}'...")
            
            record_count = 0
            async for record in connector.get_full_snapshot(
                "sample_schema", 
                collection_name, 
                batch_size=10
            ):
                record_count += 1
                if record_count <= 3:  # Show first 3 records
                    print(f"  Record {record_count}:")
                    print(f"    ID: {record.primary_key_values}")
                    print(f"    Operation: {record.operation.value}")
                    print(f"    Timestamp: {record.timestamp}")
                    
                    # Show sample data fields
                    data_sample = dict(list(record.data.items())[:3])
                    print(f"    Sample data: {json.dumps(data_sample, default=str)}")
                
                if record_count >= 10:  # Limit output
                    break
            
            print(f"📊 Processed {record_count} records from snapshot")
        
        # Demonstrate change detection
        print(f"\n🔄 Monitoring for changes (timestamp-based)...")
        print("   Note: This will only find documents with 'updated_at' field")
        
        change_count = 0
        async for event in connector.get_changes("sample_schema", batch_size=5):
            change_count += 1
            print(f"  Change {change_count}:")
            print(f"    Collection: {event.record.table_name}")
            print(f"    Operation: {event.record.operation.value}")
            print(f"    Position: {event.position_marker}")
            
            if change_count >= 5:  # Limit output
                break
        
        if change_count == 0:
            print("  No recent changes found (documents may not have 'updated_at' field)")
        else:
            print(f"📊 Found {change_count} recent changes")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Cleanup
        print("\nDisconnecting...")
        await connector.disconnect()
        print("👋 Disconnected from MongoDB")


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
