#!/usr/bin/env python3
"""Simple test script to verify BigQuery connector implementation."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_bigquery_connector_import():
    """Test that BigQuery connector can be imported."""
    try:
        from cartridge.scanner.bigquery import BigQueryConnector
        print("✅ BigQuery connector imported successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to import BigQuery connector: {e}")
        return False

def test_bigquery_connector_instantiation():
    """Test that BigQuery connector can be instantiated."""
    try:
        from cartridge.scanner.bigquery import BigQueryConnector
        
        config = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset"
        }
        
        connector = BigQueryConnector(config)
        assert connector.project_id == "test-project"
        assert connector.dataset_id == "test_dataset"
        assert connector.location == "US"
        
        print("✅ BigQuery connector instantiated successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to instantiate BigQuery connector: {e}")
        return False

def test_data_type_normalization():
    """Test BigQuery data type normalization."""
    try:
        from cartridge.scanner.bigquery import BigQueryConnector
        from cartridge.scanner.base import DataType
        
        config = {"type": "bigquery", "project_id": "test", "dataset_id": "test"}
        connector = BigQueryConnector(config)
        
        # Test key mappings
        assert connector.normalize_data_type("INTEGER") == DataType.BIGINT
        assert connector.normalize_data_type("STRING") == DataType.TEXT
        assert connector.normalize_data_type("TIMESTAMP") == DataType.TIMESTAMPTZ
        assert connector.normalize_data_type("BOOLEAN") == DataType.BOOLEAN
        assert connector.normalize_data_type("STRUCT") == DataType.JSON
        assert connector.normalize_data_type("ARRAY") == DataType.ARRAY
        assert connector.normalize_data_type("UNKNOWN_TYPE") == DataType.UNKNOWN
        
        print("✅ Data type normalization working correctly")
        return True
    except Exception as e:
        print(f"❌ Data type normalization failed: {e}")
        return False

def test_factory_registration():
    """Test that BigQuery connector is registered in factory."""
    try:
        from cartridge.scanner.factory import ConnectorFactory
        
        supported_dbs = ConnectorFactory.get_supported_databases()
        assert "bigquery" in supported_dbs
        
        print("✅ BigQuery connector registered in factory")
        print(f"   Supported databases: {supported_dbs}")
        return True
    except Exception as e:
        print(f"❌ Factory registration test failed: {e}")
        return False

def test_factory_connector_creation():
    """Test that factory can create BigQuery connector."""
    try:
        from cartridge.scanner.factory import ConnectorFactory
        from cartridge.scanner.bigquery import BigQueryConnector
        
        config = {
            "type": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test_dataset"
        }
        
        connector = ConnectorFactory.create_connector("bigquery", config)
        assert isinstance(connector, BigQueryConnector)
        assert connector.project_id == "test-project"
        
        print("✅ Factory can create BigQuery connector")
        return True
    except Exception as e:
        print(f"❌ Factory connector creation failed: {e}")
        return False

def main():
    """Run all tests."""
    print("Running BigQuery connector tests...\n")
    
    tests = [
        test_bigquery_connector_import,
        test_bigquery_connector_instantiation,
        test_data_type_normalization,
        test_factory_registration,
        test_factory_connector_creation,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! BigQuery connector implementation is working.")
        return 0
    else:
        print("❌ Some tests failed. Check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
