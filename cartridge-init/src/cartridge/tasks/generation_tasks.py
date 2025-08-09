"""Background tasks for dbt model generation."""

from typing import Dict, Any, List
import uuid
import asyncio
import tempfile
import os

from cartridge.tasks.celery_app import celery_app
from cartridge.ai.factory import AIProviderFactory
from cartridge.ai.base import ModelGenerationRequest, ModelType, TableMapping, ColumnMapping
from cartridge.dbt.project_generator import DBTProjectGenerator
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True)
def generate_dbt_models(
    self, 
    project_id: str, 
    schema_data: Dict[str, Any], 
    generation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Background task to generate dbt models using AI.
    
    Args:
        project_id: UUID of the project
        schema_data: Schema scan results
        generation_config: Model generation configuration
        
    Returns:
        Dict with generation results
    """
    logger.info("Starting dbt model generation", 
                project_id=project_id,
                ai_model=generation_config.get("ai_model"))
    
    try:
        # Update task status
        self.update_state(
            state="PROGRESS",
            meta={"current": 0, "total": 100, "status": "Analyzing schema..."}
        )
        
        # Convert schema data to AI-friendly format
        tables = []
        for table_data in schema_data.get("tables", []):
            columns = []
            for col_data in table_data.get("columns", []):
                column = ColumnMapping(
                    name=col_data["name"],
                    data_type=col_data["data_type"],
                    nullable=col_data["nullable"],
                    is_primary_key=col_data.get("is_primary_key", False),
                    is_foreign_key=col_data.get("is_foreign_key", False),
                    foreign_key_table=col_data.get("foreign_key_table"),
                    foreign_key_column=col_data.get("foreign_key_column"),
                    description=col_data.get("comment"),
                    sample_values=col_data.get("sample_values")
                )
                columns.append(column)
            
            table = TableMapping(
                name=table_data["name"],
                schema=table_data["schema"],
                table_type=table_data["table_type"],
                columns=columns,
                row_count=table_data.get("row_count"),
                description=table_data.get("comment"),
                primary_key_columns=table_data.get("primary_key_columns"),
                foreign_key_relationships=table_data.get("foreign_key_relationships")
            )
            tables.append(table)
        
        # Create AI generation request
        model_types = []
        if generation_config.get("include_staging", True):
            model_types.append(ModelType.STAGING)
        if generation_config.get("include_intermediate", False):
            model_types.append(ModelType.INTERMEDIATE)
        if generation_config.get("include_marts", True):
            model_types.append(ModelType.MARTS)
        
        ai_request = ModelGenerationRequest(
            tables=tables,
            model_types=model_types,
            business_context=generation_config.get("business_context"),
            naming_convention=generation_config.get("naming_convention"),
            include_tests=generation_config.get("include_tests", True),
            include_documentation=generation_config.get("include_documentation", True),
            target_warehouse=generation_config.get("target_warehouse", "postgresql"),
            fact_tables=schema_data.get("analysis", {}).get("fact_tables"),
            dimension_tables=schema_data.get("analysis", {}).get("dimension_tables"),
            bridge_tables=schema_data.get("analysis", {}).get("bridge_tables"),
            relationships=schema_data.get("relationships")
        )
        
        self.update_state(
            state="PROGRESS",
            meta={"current": 20, "total": 100, "status": "Generating models with AI..."}
        )
        
        # Run AI generation in async context
        async def run_generation():
            # Create AI provider
            ai_provider = AIProviderFactory.create_provider(
                generation_config.get("ai_model", "mock"),
                generation_config.get("ai_config", {})
            )
            
            # Generate models
            generation_result = await ai_provider.generate_models(ai_request)
            return generation_result
        
        # Execute AI generation
        generation_result = asyncio.run(run_generation())
        
        self.update_state(
            state="PROGRESS",
            meta={"current": 70, "total": 100, "status": "Creating dbt project..."}
        )
        
        # Generate dbt project
        project_name = generation_config.get("project_name", f"cartridge_project_{project_id[:8]}")
        dbt_generator = DBTProjectGenerator(
            project_name=project_name,
            target_warehouse=generation_config.get("target_warehouse", "postgresql")
        )
        
        # Create project in temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            project_path = os.path.join(temp_dir, project_name)
            
            project_info = dbt_generator.generate_project(
                generation_result=generation_result,
                output_dir=project_path,
                connection_config=generation_config.get("connection_config")
            )
            
            self.update_state(
                state="PROGRESS",
                meta={"current": 90, "total": 100, "status": "Creating project archive..."}
            )
            
            # Create archive
            archive_path = dbt_generator.create_project_archive(project_path)
            
            # Move archive to persistent location (would be cloud storage in production)
            final_archive_path = f"/tmp/dbt_projects/{project_id}.tar.gz"
            os.makedirs(os.path.dirname(final_archive_path), exist_ok=True)
            os.rename(archive_path, final_archive_path)
            
            result = {
                "project_id": project_id,
                "status": "completed",
                "models_generated": len(generation_result.models),
                "files_created": sum(project_info["files_created"].values()),
                "ai_model_used": generation_result.generation_metadata.get("model_used"),
                "ai_provider": generation_result.generation_metadata.get("ai_provider"),
                "generation_duration_seconds": 0,  # Would track actual time
                "models": [
                    {
                        "name": model.name,
                        "type": model.model_type.value,
                        "description": model.description,
                        "materialization": model.materialization,
                        "tags": model.tags
                    }
                    for model in generation_result.models
                ],
                "project_info": project_info,
                "project_path": project_path,
                "archive_path": final_archive_path,
                "download_url": f"/api/projects/{project_id}/download",
                "errors": generation_result.errors,
                "warnings": generation_result.warnings
            }
        
        logger.info("dbt model generation completed",
                   project_id=project_id,
                   models_generated=len(generation_result.models))
        
        return result
        
    except Exception as e:
        logger.error("dbt model generation failed",
                    project_id=project_id,
                    error=str(e))
        
        self.update_state(
            state="FAILURE",
            meta={"error": str(e), "project_id": project_id}
        )
        raise


@celery_app.task(bind=True)
def create_project_archive(self, project_id: str, project_path: str) -> Dict[str, Any]:
    """
    Create tar archive of generated dbt project.
    
    Args:
        project_id: UUID of the project
        project_path: Path to the generated project
        
    Returns:
        Dict with archive creation results
    """
    logger.info("Creating project archive", project_id=project_id)
    
    try:
        # Create actual tar archive
        import tarfile
        
        archive_path = f"/tmp/dbt_projects/{project_id}.tar.gz"
        
        # Ensure archive directory exists
        os.makedirs(os.path.dirname(archive_path), exist_ok=True)
        
        # Create tar.gz archive
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(project_path, arcname=os.path.basename(project_path))
        
        # Get archive size
        archive_size = os.path.getsize(archive_path)
        
        result = {
            "project_id": project_id,
            "status": "completed",
            "archive_path": archive_path,
            "archive_size_bytes": archive_size
        }
        
        logger.info("Project archive created", 
                   project_id=project_id,
                   archive_path=archive_path)
        
        return result
        
    except Exception as e:
        logger.error("Project archive creation failed",
                    project_id=project_id, 
                    error=str(e))
        raise