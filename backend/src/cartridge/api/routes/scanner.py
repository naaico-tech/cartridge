"""Schema scanner API endpoints."""

from typing import Dict, Any, List, Optional, Union
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cartridge.core.database import get_db
from cartridge.core.logging import get_logger
from cartridge.scanner.factory import ConnectorFactory
from cartridge.scanner.base import SchemaAnalyzer, TableInfo as ScannerTableInfo, ColumnInfo, ConstraintInfo, IndexInfo
from cartridge.tasks.scan_tasks import scan_database_schema, test_database_connection
from cartridge.tasks.celery_app import celery_app

logger = get_logger(__name__)
router = APIRouter()


def _convert_column_to_dict(column: ColumnInfo) -> Dict[str, Any]:
    """Convert ColumnInfo to dictionary."""
    return {
        "name": column.name,
        "type": column.data_type.value,
        "nullable": column.nullable,
        "primary_key": column.is_primary_key,
        "foreign_key": column.is_foreign_key,
        "default_value": column.default_value,
        "comment": column.comment,
        "max_length": column.max_length,
        "precision": column.precision,
        "scale": column.scale,
    }


def _convert_constraint_to_dict(constraint: ConstraintInfo) -> Dict[str, Any]:
    """Convert ConstraintInfo to dictionary."""
    return {
        "name": constraint.name,
        "type": constraint.type,
        "columns": constraint.columns,
        "referenced_table": constraint.referenced_table,
        "referenced_columns": constraint.referenced_columns,
        "definition": constraint.definition,
    }


def _convert_index_to_dict(index: IndexInfo) -> Dict[str, Any]:
    """Convert IndexInfo to dictionary."""
    return {
        "name": index.name,
        "columns": index.columns,
        "is_unique": index.is_unique,
        "is_primary": index.is_primary,
        "type": index.type,
        "definition": index.definition,
    }


def _convert_table_to_dict(table: ScannerTableInfo) -> Dict[str, Any]:
    """Convert scanner TableInfo to API TableInfo dictionary."""
    return {
        "name": table.name,
        "schema": table.schema,
        "row_count": table.row_count or 0,
        "columns": [_convert_column_to_dict(col) for col in table.columns],
        "constraints": [_convert_constraint_to_dict(const) for const in table.constraints],
        "indexes": [_convert_index_to_dict(idx) for idx in table.indexes],
        "sample_data": table.sample_data or [],
    }


class DataSourceConnection(BaseModel):
    """Data source connection configuration."""
    
    type: str = Field(..., description="Database type (postgresql, mysql, snowflake, etc.)")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")
    schema: str = Field(default="public", description="Schema name")
    
    class Config:
        schema_extra = {
            "example": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "analytics",
                "username": "user",
                "password": "password",
                "schema": "public"
            }
        }


class ScanRequest(BaseModel):
    """Schema scan request."""
    
    connection: DataSourceConnection
    tables: List[str] = Field(default=[], description="Specific tables to scan (empty for all)")
    include_samples: bool = Field(default=True, description="Include sample data")
    sample_size: int = Field(default=100, description="Number of sample rows")
    async_mode: bool = Field(default=False, description="Run scan as background task")


class TableInfo(BaseModel):
    """Table information model."""
    
    name: str
    schema: str
    row_count: int
    columns: List[Dict[str, Any]]
    constraints: List[Dict[str, Any]]
    indexes: List[Dict[str, Any]]
    sample_data: List[Dict[str, Any]] = []


class ScanResult(BaseModel):
    """Schema scan result."""
    
    connection_info: Dict[str, Any]
    tables: List[TableInfo]
    relationships: List[Dict[str, Any]]
    scan_timestamp: str
    scan_duration_seconds: float


class TaskResult(BaseModel):
    """Background task result."""
    
    task_id: str
    status: str  # PENDING, PROGRESS, SUCCESS, FAILURE
    message: str
    result: Optional[Dict[str, Any]] = None
    progress: Optional[Dict[str, Any]] = None


@router.post("/scan", response_model=Union[ScanResult, TaskResult])
async def scan_schema(
    request: ScanRequest,
    db: Session = Depends(get_db)
) -> Union[ScanResult, TaskResult]:
    """Scan a data source schema and return detailed information."""
    logger.info("Starting schema scan", 
                database_type=request.connection.type,
                database=request.connection.database,
                async_mode=request.async_mode)
    
    try:
        # Validate connection type
        supported_types = ConnectorFactory.get_supported_databases()
        if request.connection.type not in supported_types:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported database type: {request.connection.type}. "
                      f"Supported types: {supported_types}"
            )
        
        # Create connection config
        connection_config = {
            "type": request.connection.type,
            "host": request.connection.host,
            "port": request.connection.port,
            "database": request.connection.database,
            "username": request.connection.username,
            "password": request.connection.password,
            "schema": request.connection.schema,
            "tables": request.tables,
            "include_samples": request.include_samples,
            "sample_size": request.sample_size,
        }
        
        # Check if async mode is requested
        if request.async_mode:
            # Generate scan result ID
            import uuid
            scan_result_id = str(uuid.uuid4())
            
            # Queue the background task
            task = scan_database_schema.delay(scan_result_id, connection_config)
            
            return TaskResult(
                task_id=task.id,
                status="PENDING",
                message="Schema scan queued for background processing",
                result={"scan_result_id": scan_result_id}
            )
        
        # Synchronous execution (existing logic)
        # Create connector and perform scan
        connector = ConnectorFactory.create_connector(request.connection.type, connection_config)
        
        # Perform the actual schema scan
        scan_result = await connector.scan_schema(
            tables=request.tables if request.tables else None,
            include_sample_data=request.include_samples,
            sample_size=request.sample_size
        )
        
        # Convert scanner result to API response format
        api_tables = [_convert_table_to_dict(table) for table in scan_result.tables]
        relationships = scan_result.get_relationships()
        
        result = ScanResult(
            connection_info={
                "type": request.connection.type,
                "host": request.connection.host,
                "database": request.connection.database,
                "schema": request.connection.schema,
                "database_version": scan_result.database_info.version,
                "total_tables": scan_result.database_info.total_tables,
                "total_views": scan_result.database_info.total_views,
            },
            tables=api_tables,
            relationships=relationships,
            scan_timestamp=scan_result.scan_timestamp,
            scan_duration_seconds=scan_result.scan_duration_seconds
        )
        
        logger.info("Schema scan completed successfully",
                   table_count=len(result.tables),
                   duration=result.scan_duration_seconds)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Schema scan failed", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Schema scan failed: {str(e)}"
        )


@router.post("/test-connection")
async def test_connection(connection: DataSourceConnection) -> Dict[str, Any]:
    """Test a data source connection."""
    logger.info("Testing database connection",
                database_type=connection.type,
                host=connection.host,
                database=connection.database)
    
    try:
        # Validate connection type
        supported_types = ConnectorFactory.get_supported_databases()
        if connection.type not in supported_types:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported database type: {connection.type}. "
                      f"Supported types: {supported_types}"
            )
        
        # Create connection config
        connection_config = {
            "host": connection.host,
            "port": connection.port,
            "database": connection.database,
            "username": connection.username,
            "password": connection.password,
            "schema": connection.schema,
        }
        
        # Create connector and test connection
        connector = ConnectorFactory.create_connector(connection.type, connection_config)
        result = await connector.test_connection()
        
        if result["status"] == "success":
            return {
                "status": "success",
                "message": "Connection test successful",
                "connection_info": {
                    "type": connection.type,
                    "host": connection.host,
                    "database": connection.database,
                    "schema": connection.schema,
                },
                "database_info": {
                    "version": result.get("database_version", "unknown"),
                    "type": result.get("database_type", connection.type)
                }
            }
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Connection test failed: {result['message']}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Connection test failed", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Connection test failed: {str(e)}"
        )


@router.get("/tasks/{task_id}", response_model=TaskResult)
async def get_task_status(task_id: str) -> TaskResult:
    """Get the status of a background task."""
    try:
        # Get task result from Celery
        task_result = celery_app.AsyncResult(task_id)
        
        if task_result.state == "PENDING":
            return TaskResult(
                task_id=task_id,
                status="PENDING",
                message="Task is waiting to be processed"
            )
        elif task_result.state == "PROGRESS":
            return TaskResult(
                task_id=task_id,
                status="PROGRESS",
                message="Task is being processed",
                progress=task_result.info
            )
        elif task_result.state == "SUCCESS":
            return TaskResult(
                task_id=task_id,
                status="SUCCESS",
                message="Task completed successfully",
                result=task_result.result
            )
        elif task_result.state == "FAILURE":
            return TaskResult(
                task_id=task_id,
                status="FAILURE",
                message=f"Task failed: {str(task_result.info)}",
                result={"error": str(task_result.info)}
            )
        else:
            return TaskResult(
                task_id=task_id,
                status=task_result.state,
                message=f"Task status: {task_result.state}",
                result=task_result.info if task_result.info else None
            )
            
    except Exception as e:
        logger.error("Failed to get task status", task_id=task_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get task status: {str(e)}"
        )