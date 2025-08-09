"""Background tasks for schema scanning."""

from typing import Dict, Any
import uuid
import asyncio

from cartridge.tasks.celery_app import celery_app
from cartridge.scanner.factory import ConnectorFactory
from cartridge.scanner.base import SchemaAnalyzer
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True)
def scan_database_schema(self, scan_result_id: str, connection_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Background task to scan database schema.
    
    Args:
        scan_result_id: UUID of the scan result record
        connection_config: Database connection configuration
        
    Returns:
        Dict with scan results
    """
    logger.info("Starting database schema scan", scan_result_id=scan_result_id)
    
    try:
        # Update task status
        self.update_state(
            state="PROGRESS",
            meta={"current": 0, "total": 100, "status": "Connecting to database..."}
        )
        
        # Create database connector
        connector = ConnectorFactory.create_connector(
            connection_config["type"], 
            connection_config
        )
        
        # Run schema scan in async context
        async def run_scan():
            self.update_state(
                state="PROGRESS",
                meta={"current": 20, "total": 100, "status": "Scanning schema..."}
            )
            
            # Perform the actual schema scan
            scan_result = await connector.scan_schema(
                tables=connection_config.get("tables"),
                include_sample_data=connection_config.get("include_samples", True),
                sample_size=connection_config.get("sample_size", 100)
            )
            
            self.update_state(
                state="PROGRESS",
                meta={"current": 80, "total": 100, "status": "Analyzing schema..."}
            )
            
            # Analyze the schema for insights
            analyzer = SchemaAnalyzer(scan_result)
            
            # Get analysis results
            fact_tables = analyzer.detect_fact_tables()
            dimension_tables = analyzer.detect_dimension_tables()
            bridge_tables = analyzer.detect_bridge_tables()
            staging_models = analyzer.suggest_staging_models()
            mart_models = analyzer.suggest_mart_models()
            
            self.update_state(
                state="PROGRESS",
                meta={"current": 100, "total": 100, "status": "Completing scan..."}
            )
            
            # Convert scan result to serializable format
            tables_data = []
            for table in scan_result.tables:
                table_data = {
                    "name": table.name,
                    "schema": table.schema,
                    "table_type": table.table_type,
                    "row_count": table.row_count,
                    "size_bytes": table.size_bytes,
                    "comment": table.comment,
                    "columns": [
                        {
                            "name": col.name,
                            "data_type": col.data_type.value,
                            "raw_type": col.raw_type,
                            "nullable": col.nullable,
                            "default_value": col.default_value,
                            "max_length": col.max_length,
                            "precision": col.precision,
                            "scale": col.scale,
                            "is_primary_key": col.is_primary_key,
                            "is_foreign_key": col.is_foreign_key,
                            "foreign_key_table": col.foreign_key_table,
                            "foreign_key_column": col.foreign_key_column,
                            "is_unique": col.is_unique,
                            "is_indexed": col.is_indexed,
                            "comment": col.comment,
                            "null_count": col.null_count,
                            "unique_count": col.unique_count,
                            "min_value": col.min_value,
                            "max_value": col.max_value,
                            "avg_value": col.avg_value,
                            "sample_values": col.sample_values
                        }
                        for col in table.columns
                    ],
                    "constraints": [
                        {
                            "name": const.name,
                            "type": const.type,
                            "columns": const.columns,
                            "referenced_table": const.referenced_table,
                            "referenced_columns": const.referenced_columns,
                            "definition": const.definition
                        }
                        for const in table.constraints
                    ],
                    "indexes": [
                        {
                            "name": idx.name,
                            "columns": idx.columns,
                            "is_unique": idx.is_unique,
                            "is_primary": idx.is_primary,
                            "type": idx.type,
                            "definition": idx.definition
                        }
                        for idx in table.indexes
                    ],
                    "sample_data": table.sample_data,
                    "primary_key_columns": table.get_primary_key_columns(),
                    "foreign_key_relationships": table.get_foreign_key_relationships()
                }
                tables_data.append(table_data)
            
            return {
                "scan_result_id": scan_result_id,
                "status": "completed",
                "database_info": {
                    "database_type": scan_result.database_info.database_type,
                    "version": scan_result.database_info.version,
                    "host": scan_result.database_info.host,
                    "port": scan_result.database_info.port,
                    "database_name": scan_result.database_info.database_name,
                    "schema_name": scan_result.database_info.schema_name,
                    "total_tables": scan_result.database_info.total_tables,
                    "total_views": scan_result.database_info.total_views,
                    "total_size_bytes": scan_result.database_info.total_size_bytes
                },
                "tables": tables_data,
                "relationships": scan_result.get_relationships(),
                "analysis": {
                    "fact_tables": fact_tables,
                    "dimension_tables": dimension_tables,
                    "bridge_tables": bridge_tables,
                    "suggested_staging_models": staging_models,
                    "suggested_mart_models": mart_models
                },
                "scan_duration_seconds": scan_result.scan_duration_seconds,
                "scan_timestamp": scan_result.scan_timestamp,
                "errors": scan_result.errors
            }
        
        # Run the async scan
        result = asyncio.run(run_scan())
        
        logger.info("Database schema scan completed", 
                   scan_result_id=scan_result_id,
                   tables_found=len(result["tables"]))
        
        return result
        
    except Exception as e:
        logger.error("Database schema scan failed", 
                    scan_result_id=scan_result_id, 
                    error=str(e))
        
        self.update_state(
            state="FAILURE",
            meta={"error": str(e), "scan_result_id": scan_result_id}
        )
        raise


@celery_app.task(bind=True)
def test_database_connection(self, connection_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Test database connection.
    
    Args:
        connection_config: Database connection configuration
        
    Returns:
        Dict with connection test results
    """
    logger.info("Testing database connection", database_type=connection_config.get("type"))
    
    try:
        # Create database connector
        connector = ConnectorFactory.create_connector(
            connection_config["type"], 
            connection_config
        )
        
        # Run connection test in async context
        async def run_test():
            import time
            start_time = time.time()
            
            result = await connector.test_connection()
            
            end_time = time.time()
            response_time = int((end_time - start_time) * 1000)  # Convert to milliseconds
            
            if result["status"] == "success":
                result["response_time_ms"] = response_time
            
            return result
        
        # Run the async test
        result = asyncio.run(run_test())
        
        if result["status"] == "success":
            logger.info("Database connection test successful")
        else:
            logger.warning("Database connection test failed", error=result.get("error"))
        
        return result
        
    except Exception as e:
        logger.error("Database connection test failed", error=str(e))
        
        result = {
            "status": "failed", 
            "message": f"Connection failed: {str(e)}",
            "error": str(e)
        }
        
        return result