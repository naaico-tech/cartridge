"""Project management API endpoints."""

from typing import Dict, Any, List, Optional, Union
import uuid
import tempfile
import os
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cartridge.core.database import get_db
from cartridge.core.logging import get_logger
from cartridge.ai.factory import AIProviderFactory
from cartridge.ai.base import ModelGenerationRequest as AIModelRequest, ModelType, TableMapping, ColumnMapping
from cartridge.dbt.project_generator import DBTProjectGenerator
from cartridge.scanner.base import SchemaAnalyzer
from cartridge.tasks.generation_tasks import generate_dbt_models
from cartridge.tasks.celery_app import celery_app

logger = get_logger(__name__)
router = APIRouter()


class ModelGenerationRequest(BaseModel):
    """Request to generate dbt models from schema."""
    
    schema_data: Dict[str, Any] = Field(..., description="Schema scan results")
    model_types: List[str] = Field(
        default=["staging", "intermediate", "marts"],
        description="Types of models to generate"
    )
    ai_model: str = Field(default="gpt-4", description="AI model to use for generation")
    include_tests: bool = Field(default=True, description="Generate dbt tests")
    include_docs: bool = Field(default=True, description="Generate documentation")
    async_mode: bool = Field(default=False, description="Run generation as background task")
    project_name: Optional[str] = Field(default=None, description="Custom project name")
    business_context: Optional[str] = Field(default=None, description="Business context for AI generation")


class GeneratedModel(BaseModel):
    """Generated dbt model information."""
    
    name: str
    type: str  # staging, intermediate, marts
    sql: str
    description: str
    tests: List[Dict[str, Any]] = []
    dependencies: List[str] = []


class ProjectGenerationResult(BaseModel):
    """Result of project generation."""
    
    project_id: str
    models: List[GeneratedModel]
    project_structure: Dict[str, Any]
    generation_timestamp: str
    ai_model_used: str


class TestRunRequest(BaseModel):
    """Request to test run generated models."""
    
    project_id: str
    models_to_test: List[str] = Field(default=[], description="Specific models to test (empty for all)")
    dry_run: bool = Field(default=True, description="Perform dry run only")


class TestRunResult(BaseModel):
    """Result of test run."""
    
    project_id: str
    status: str  # success, failed, partial
    results: List[Dict[str, Any]]
    execution_time_seconds: float
    errors: List[Dict[str, Any]] = []


class TaskResult(BaseModel):
    """Background task result."""
    
    task_id: str
    status: str  # PENDING, PROGRESS, SUCCESS, FAILURE
    message: str
    result: Optional[Dict[str, Any]] = None
    progress: Optional[Dict[str, Any]] = None


def _convert_schema_data_to_tables(schema_data: Dict[str, Any]) -> List[TableMapping]:
    """Convert API schema data to TableMapping objects for AI providers."""
    tables = []
    
    for table_data in schema_data.get("tables", []):
        columns = []
        for col_data in table_data.get("columns", []):
            column = ColumnMapping(
                name=col_data["name"],
                data_type=col_data["type"],
                nullable=col_data.get("nullable", True),
                is_primary_key=col_data.get("primary_key", False),
                is_foreign_key=col_data.get("foreign_key", False),
                default_value=col_data.get("default_value"),
                comment=col_data.get("comment"),
                max_length=col_data.get("max_length"),
                precision=col_data.get("precision"),
                scale=col_data.get("scale"),
            )
            columns.append(column)
        
        table = TableMapping(
            name=table_data["name"],
            schema=table_data["schema"],
            table_type=table_data.get("table_type", "table"),
            columns=columns,
            row_count=table_data.get("row_count", 0),
            comment=table_data.get("comment"),
        )
        tables.append(table)
    
    return tables


def _convert_ai_model_to_api(ai_model) -> GeneratedModel:
    """Convert AI provider GeneratedModel to API GeneratedModel."""
    return GeneratedModel(
        name=ai_model.name,
        type=ai_model.model_type.value,
        sql=ai_model.sql,
        description=ai_model.description or "",
        tests=[test.to_dict() for test in ai_model.tests] if ai_model.tests else [],
        dependencies=ai_model.dependencies or [],
    )


@router.post("/generate", response_model=Union[ProjectGenerationResult, TaskResult])
async def generate_models(
    request: ModelGenerationRequest,
    db: Session = Depends(get_db)
) -> Union[ProjectGenerationResult, TaskResult]:
    """Generate dbt models from schema analysis using AI."""
    logger.info("Starting model generation",
                model_types=request.model_types,
                ai_model=request.ai_model,
                async_mode=request.async_mode)
    
    try:
        # Check if async mode is requested
        if request.async_mode:
            # Generate project ID
            project_id = f"proj_{uuid.uuid4().hex[:8]}"
            
            # Create generation config for the background task
            generation_config = {
                "ai_model": request.ai_model,
                "model_types": request.model_types,
                "include_tests": request.include_tests,
                "include_docs": request.include_docs,
                "project_name": request.project_name or f"cartridge_project_{project_id}",
                "business_context": request.business_context,
                "target_warehouse": "postgresql",  # Default for now
                "include_staging": "staging" in request.model_types,
                "include_intermediate": "intermediate" in request.model_types,
                "include_marts": "marts" in request.model_types,
            }
            
            # Queue the background task
            task = generate_dbt_models.delay(project_id, request.schema_data, generation_config)
            
            return TaskResult(
                task_id=task.id,
                status="PENDING",
                message="Model generation queued for background processing",
                result={"project_id": project_id}
            )
        
        # Synchronous execution (existing logic)
        # Validate AI model
        supported_models = AIProviderFactory.get_supported_models()
        if request.ai_model not in supported_models:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported AI model: {request.ai_model}. "
                      f"Supported models: {supported_models}"
            )
        
        # Convert schema data to table mappings
        tables = _convert_schema_data_to_tables(request.schema_data)
        if not tables:
            raise HTTPException(
                status_code=400,
                detail="No tables found in schema data"
            )
        
        # Convert model types to ModelType enum
        model_types = []
        for model_type in request.model_types:
            try:
                model_types.append(ModelType(model_type.lower()))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid model type: {model_type}. "
                          f"Valid types: {[mt.value for mt in ModelType]}"
                )
        
        # Analyze schema for fact/dimension detection
        # Create a mock scan result for the analyzer
        from cartridge.scanner.base import ScanResult as ScannerScanResult, DatabaseInfo
        mock_db_info = DatabaseInfo(
            database_type="unknown",
            version="unknown",
            host="unknown",
            port=0,
            database_name="unknown",
            schema_name="unknown",
            total_tables=len(tables),
            total_views=0
        )
        
        # Convert TableMapping back to scanner TableInfo for analysis
        scanner_tables = []
        for table_mapping in tables:
            from cartridge.scanner.base import TableInfo as ScannerTableInfo, ColumnInfo as ScannerColumnInfo
            scanner_columns = []
            for col in table_mapping.columns:
                from cartridge.scanner.base import DataType
                try:
                    data_type = DataType(col.data_type.lower())
                except ValueError:
                    data_type = DataType.UNKNOWN
                
                scanner_col = ScannerColumnInfo(
                    name=col.name,
                    data_type=data_type,
                    nullable=col.nullable,
                    is_primary_key=col.is_primary_key,
                    is_foreign_key=col.is_foreign_key,
                    default_value=col.default_value,
                    comment=col.comment,
                    max_length=col.max_length,
                    precision=col.precision,
                    scale=col.scale,
                )
                scanner_columns.append(scanner_col)
            
            scanner_table = ScannerTableInfo(
                name=table_mapping.name,
                schema=table_mapping.schema,
                table_type=table_mapping.table_type,
                columns=scanner_columns,
                constraints=[],
                indexes=[],
                row_count=table_mapping.row_count,
                comment=table_mapping.comment,
            )
            scanner_tables.append(scanner_table)
        
        mock_scan_result = ScannerScanResult(
            database_info=mock_db_info,
            tables=scanner_tables,
            scan_duration_seconds=0.0,
            scan_timestamp=datetime.utcnow().isoformat() + "Z"
        )
        
        analyzer = SchemaAnalyzer(mock_scan_result)
        fact_tables = analyzer.detect_fact_tables()
        
        # Create AI model generation request
        ai_request = AIModelRequest(
            tables=tables,
            model_types=model_types,
            business_context="Generated from API request",
            include_tests=request.include_tests,
            include_documentation=request.include_docs,
            fact_tables=fact_tables,
        )
        
        # Create AI provider and generate models
        ai_config = {"model": request.ai_model}
        ai_provider = AIProviderFactory.create_provider(request.ai_model, ai_config)
        
        # Generate models using AI
        generation_result = await ai_provider.generate_models(ai_request)
        
        # Convert AI models to API format
        api_models = [_convert_ai_model_to_api(model) for model in generation_result.models]
        
        # Generate project ID
        project_id = f"proj_{uuid.uuid4().hex[:8]}"
        
        # Create project structure info
        project_structure = {
            "models": {},
            "tests": [],
            "docs": []
        }
        
        # Group models by type
        for model in api_models:
            model_type = model.type
            if model_type not in project_structure["models"]:
                project_structure["models"][model_type] = []
            project_structure["models"][model_type].append(f"{model.name}.sql")
        
        if request.include_tests:
            project_structure["tests"].append("schema.yml")
        
        if request.include_docs:
            project_structure["docs"].append("README.md")
        
        result = ProjectGenerationResult(
            project_id=project_id,
            models=api_models,
            project_structure=project_structure,
            generation_timestamp=datetime.utcnow().isoformat() + "Z",
            ai_model_used=request.ai_model
        )
        
        logger.info("Model generation completed",
                   project_id=result.project_id,
                   model_count=len(result.models))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Model generation failed", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Model generation failed: {str(e)}"
        )


@router.post("/test-run", response_model=TestRunResult)
async def test_run_models(
    request: TestRunRequest,
    db: Session = Depends(get_db)
) -> TestRunResult:
    """Test run generated dbt models."""
    logger.info("Starting test run",
                project_id=request.project_id,
                dry_run=request.dry_run)
    
    try:
        # TODO: Implement actual test run logic
        # This is a placeholder response
        
        result = TestRunResult(
            project_id=request.project_id,
            status="success",
            results=[
                {
                    "model": "stg_customers",
                    "status": "success",
                    "rows_affected": 1000,
                    "execution_time": 0.5
                },
                {
                    "model": "int_customer_metrics",
                    "status": "success",
                    "rows_affected": 500,
                    "execution_time": 0.3
                },
                {
                    "model": "dim_customers",
                    "status": "success",
                    "rows_affected": 1000,
                    "execution_time": 0.7
                }
            ],
            execution_time_seconds=1.5,
            errors=[]
        )
        
        logger.info("Test run completed",
                   project_id=request.project_id,
                   status=result.status,
                   execution_time=result.execution_time_seconds)
        
        return result
        
    except Exception as e:
        logger.error("Test run failed", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Test run failed: {str(e)}"
        )


@router.get("/{project_id}/download")
async def download_project(project_id: str):
    """Download generated dbt project as tar file."""
    logger.info("Downloading project", project_id=project_id)
    
    try:
        # In a full implementation, this would:
        # 1. Retrieve project from database
        # 2. Get generated models and metadata
        # 3. Generate complete dbt project
        # 4. Create tar archive
        # 
        # For now, we'll create a basic demo project
        
        # Create temporary directory for project
        with tempfile.TemporaryDirectory() as temp_dir:
            project_name = f"cartridge_project_{project_id}"
            project_path = os.path.join(temp_dir, project_name)
            
            # Create basic dbt project structure
            os.makedirs(project_path)
            os.makedirs(os.path.join(project_path, "models"))
            os.makedirs(os.path.join(project_path, "models", "staging"))
            os.makedirs(os.path.join(project_path, "models", "marts"))
            os.makedirs(os.path.join(project_path, "macros"))
            os.makedirs(os.path.join(project_path, "tests"))
            
            # Create basic dbt_project.yml
            dbt_project_content = f"""name: '{project_name}'
version: '1.0.0'
config-version: 2

profile: '{project_name}'

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"
  - "logs"

models:
  {project_name}:
    staging:
      +materialized: view
      +tags: ["staging"]
    marts:
      +materialized: table
      +tags: ["marts"]
"""
            
            with open(os.path.join(project_path, "dbt_project.yml"), "w") as f:
                f.write(dbt_project_content)
            
            # Create basic README
            readme_content = f"""# {project_name}

This dbt project was generated by Cartridge.

## Getting Started

1. Install dbt:
   ```bash
   pip install dbt-postgres
   ```

2. Configure your profile in `~/.dbt/profiles.yml`

3. Run the models:
   ```bash
   dbt run
   ```

4. Test the models:
   ```bash
   dbt test
   ```

Generated on: {datetime.utcnow().isoformat()}Z
Project ID: {project_id}
"""
            
            with open(os.path.join(project_path, "README.md"), "w") as f:
                f.write(readme_content)
            
            # Create sample staging model
            staging_model = """{{ config(materialized='view') }}

SELECT 
    id,
    name,
    email,
    created_at
FROM {{ source('raw', 'customers') }}
"""
            
            with open(os.path.join(project_path, "models", "staging", "stg_customers.sql"), "w") as f:
                f.write(staging_model)
            
            # Create sample mart model
            mart_model = """{{ config(materialized='table') }}

SELECT 
    id,
    name,
    email,
    created_at,
    CURRENT_TIMESTAMP as updated_at
FROM {{ ref('stg_customers') }}
"""
            
            with open(os.path.join(project_path, "models", "marts", "dim_customers.sql"), "w") as f:
                f.write(mart_model)
            
            # Create tar archive
            import tarfile
            import io
            
            tar_buffer = io.BytesIO()
            with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
                tar.add(project_path, arcname=project_name)
            
            tar_buffer.seek(0)
            
            # Return as streaming response
            return StreamingResponse(
                io.BytesIO(tar_buffer.read()),
                media_type="application/gzip",
                headers={"Content-Disposition": f"attachment; filename={project_name}.tar.gz"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Project download failed", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Project download failed: {str(e)}"
        )


@router.get("/{project_id}")
async def get_project(project_id: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Get project information by ID."""
    logger.info("Fetching project", project_id=project_id)
    
    try:
        # TODO: Implement actual project retrieval from database
        # This is a placeholder response
        
        return {
            "project_id": project_id,
            "status": "completed",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
            "model_count": 3,
            "generation_settings": {
                "ai_model": "gpt-4",
                "model_types": ["staging", "intermediate", "marts"],
                "include_tests": True,
                "include_docs": True
            }
        }
        
    except Exception as e:
        logger.error("Failed to fetch project", project_id=project_id, error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch project: {str(e)}"
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