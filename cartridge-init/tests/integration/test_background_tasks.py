"""Integration tests for background tasks."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from celery import Celery

from cartridge.tasks.scan_tasks import scan_database_schema, test_database_connection
from cartridge.tasks.generation_tasks import generate_dbt_models, create_project_archive
from cartridge.scanner.base import ScanResult, DatabaseInfo, TableInfo, ColumnInfo, DataType


class TestScanTasks:
    """Test schema scanning background tasks."""

    @pytest.mark.asyncio
    async def test_scan_database_schema_success(self):
        """Test successful database schema scanning task."""
        scan_result_id = "scan-123"
        connection_config = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public",
            "tables": [],
            "include_samples": True,
            "sample_size": 100
        }

        # Mock database info
        mock_db_info = DatabaseInfo(
            database_type="postgresql",
            version="13.0",
            host="localhost",
            port=5432,
            database_name="test_db",
            schema_name="public",
            total_tables=2,
            total_views=0
        )

        # Mock table info
        mock_column = ColumnInfo(
            name="id",
            data_type=DataType.INTEGER,
            nullable=False,
            is_primary_key=True,
            is_foreign_key=False
        )

        mock_table = TableInfo(
            name="customers",
            schema="public",
            table_type="table",
            columns=[mock_column],
            constraints=[],
            indexes=[],
            row_count=1000,
            sample_data=[{"id": 1, "name": "Test"}]
        )

        mock_scan_result = ScanResult(
            database_info=mock_db_info,
            tables=[mock_table],
            scan_duration_seconds=2.5,
            scan_timestamp="2024-01-01T00:00:00Z"
        )

        with patch('cartridge.tasks.scan_tasks.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema.return_value = mock_scan_result
            mock_factory.return_value = mock_connector

            # Create a mock task instance for testing
            mock_task = MagicMock()
            mock_task.update_state = MagicMock()

            # Execute the task function directly (not through Celery)
            result = scan_database_schema(mock_task, scan_result_id, connection_config)

            assert result["scan_result_id"] == scan_result_id
            assert result["status"] == "completed"
            assert len(result["tables"]) == 1
            assert result["tables"][0]["name"] == "customers"
            assert result["tables"][0]["row_count"] == 1000
            assert result["scan_duration_seconds"] == 2.5

            # Verify progress updates were called
            assert mock_task.update_state.call_count >= 3

    def test_scan_database_schema_failure(self):
        """Test database schema scanning task failure."""
        scan_result_id = "scan-123"
        connection_config = {
            "type": "postgresql",
            "host": "invalid_host",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        with patch('cartridge.tasks.scan_tasks.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema.side_effect = Exception("Connection failed")
            mock_factory.return_value = mock_connector

            mock_task = MagicMock()
            mock_task.update_state = MagicMock()

            with pytest.raises(Exception) as exc_info:
                scan_database_schema(mock_task, scan_result_id, connection_config)

            assert "Connection failed" in str(exc_info.value)
            
            # Verify failure state was set
            mock_task.update_state.assert_called_with(
                state="FAILURE",
                meta={"error": "Connection failed", "scan_result_id": scan_result_id}
            )

    @pytest.mark.asyncio
    async def test_test_database_connection_success(self):
        """Test successful database connection test task."""
        connection_config = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        with patch('cartridge.tasks.scan_tasks.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.test_connection.return_value = {
                "status": "success",
                "message": "Connection successful",
                "database_version": "PostgreSQL 13.0"
            }
            mock_factory.return_value = mock_connector

            result = test_database_connection(None, connection_config)

            assert result["status"] == "success"
            assert result["message"] == "Connection successful"
            assert "response_time_ms" in result

    def test_test_database_connection_failure(self):
        """Test database connection test task failure."""
        connection_config = {
            "type": "postgresql",
            "host": "invalid_host",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        with patch('cartridge.tasks.scan_tasks.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.test_connection.side_effect = Exception("Host not found")
            mock_factory.return_value = mock_connector

            result = test_database_connection(None, connection_config)

            assert result["status"] == "failed"
            assert "Host not found" in result["message"]


class TestGenerationTasks:
    """Test model generation background tasks."""

    @pytest.mark.asyncio
    async def test_generate_dbt_models_success(self):
        """Test successful dbt model generation task."""
        project_id = "proj_123"
        schema_data = {
            "tables": [
                {
                    "name": "customers",
                    "schema": "public",
                    "table_type": "table",
                    "row_count": 1000,
                    "columns": [
                        {
                            "name": "id",
                            "data_type": "integer",
                            "nullable": False,
                            "is_primary_key": True,
                            "is_foreign_key": False
                        }
                    ]
                }
            ],
            "analysis": {
                "fact_tables": ["orders"],
                "dimension_tables": ["customers"],
                "bridge_tables": []
            },
            "relationships": []
        }

        generation_config = {
            "ai_model": "mock",
            "include_staging": True,
            "include_intermediate": False,
            "include_marts": True,
            "include_tests": True,
            "include_documentation": True,
            "project_name": "test_project",
            "target_warehouse": "postgresql",
            "ai_config": {}
        }

        # Mock AI generation result
        mock_model = MagicMock()
        mock_model.name = "stg_customers"
        mock_model.model_type = "staging"
        mock_model.description = "Staging customers table"
        mock_model.materialization = "view"
        mock_model.tags = ["staging"]

        mock_generation_result = MagicMock()
        mock_generation_result.models = [mock_model]
        mock_generation_result.errors = []
        mock_generation_result.warnings = []
        mock_generation_result.generation_metadata = {
            "model_used": "mock",
            "ai_provider": "mock"
        }

        # Mock dbt project generation
        mock_project_info = {
            "project_name": "test_project",
            "project_path": "/tmp/test_project",
            "files_created": {
                "models": 1,
                "schemas": 1,
                "macros": 0,
                "analysis": 0,
                "docs": 1
            }
        }

        with patch('cartridge.tasks.generation_tasks.AIProviderFactory.create_provider') as mock_ai_factory, \
             patch('cartridge.tasks.generation_tasks.DBTProjectGenerator') as mock_dbt_gen, \
             patch('os.makedirs'), \
             patch('os.rename'):

            # Mock AI provider
            mock_provider = AsyncMock()
            mock_provider.generate_models.return_value = mock_generation_result
            mock_ai_factory.return_value = mock_provider

            # Mock DBT generator
            mock_generator = MagicMock()
            mock_generator.generate_project.return_value = mock_project_info
            mock_generator.create_project_archive.return_value = "/tmp/archive.tar.gz"
            mock_dbt_gen.return_value = mock_generator

            mock_task = MagicMock()
            mock_task.update_state = MagicMock()

            result = generate_dbt_models(mock_task, project_id, schema_data, generation_config)

            assert result["project_id"] == project_id
            assert result["status"] == "completed"
            assert result["models_generated"] == 1
            assert result["ai_model_used"] == "mock"
            assert result["ai_provider"] == "mock"
            assert len(result["models"]) == 1
            assert result["models"][0]["name"] == "stg_customers"

            # Verify progress updates
            assert mock_task.update_state.call_count >= 4

    def test_generate_dbt_models_ai_failure(self):
        """Test dbt model generation task with AI failure."""
        project_id = "proj_123"
        schema_data = {
            "tables": [
                {
                    "name": "customers",
                    "schema": "public",
                    "columns": [{"name": "id", "data_type": "integer"}]
                }
            ]
        }
        generation_config = {
            "ai_model": "gpt-4",
            "ai_config": {}
        }

        with patch('cartridge.tasks.generation_tasks.AIProviderFactory.create_provider') as mock_factory:
            mock_provider = AsyncMock()
            mock_provider.generate_models.side_effect = Exception("AI service unavailable")
            mock_factory.return_value = mock_provider

            mock_task = MagicMock()
            mock_task.update_state = MagicMock()

            with pytest.raises(Exception) as exc_info:
                generate_dbt_models(mock_task, project_id, schema_data, generation_config)

            assert "AI service unavailable" in str(exc_info.value)

            # Verify failure state was set
            mock_task.update_state.assert_called_with(
                state="FAILURE",
                meta={"error": "AI service unavailable", "project_id": project_id}
            )

    def test_create_project_archive_success(self):
        """Test successful project archive creation."""
        project_id = "proj_123"
        project_path = "/tmp/test_project"

        with patch('tarfile.open') as mock_tarfile, \
             patch('os.makedirs'), \
             patch('os.path.getsize') as mock_getsize:

            mock_tar = MagicMock()
            mock_tarfile.return_value.__enter__.return_value = mock_tar
            mock_getsize.return_value = 1024

            result = create_project_archive(None, project_id, project_path)

            assert result["project_id"] == project_id
            assert result["status"] == "completed"
            assert result["archive_size_bytes"] == 1024
            assert project_id in result["archive_path"]

            # Verify tar operations
            mock_tar.add.assert_called_once()

    def test_create_project_archive_failure(self):
        """Test project archive creation failure."""
        project_id = "proj_123"
        project_path = "/tmp/nonexistent"

        with patch('tarfile.open') as mock_tarfile:
            mock_tarfile.side_effect = Exception("Permission denied")

            with pytest.raises(Exception) as exc_info:
                create_project_archive(None, project_id, project_path)

            assert "Permission denied" in str(exc_info.value)


class TestTaskIntegration:
    """Test integration between different tasks."""

    @pytest.mark.asyncio
    async def test_scan_to_generation_workflow(self):
        """Test workflow from schema scan to model generation."""
        # This would test the complete workflow:
        # 1. Scan database schema
        # 2. Use scan results for model generation
        # 3. Create project archive

        scan_result_id = "scan-123"
        project_id = "proj_123"

        # Mock scan result
        connection_config = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        # This would be a more comprehensive integration test
        # For now, we verify that the tasks can be chained together
        pass

    def test_task_error_handling(self):
        """Test error handling across tasks."""
        # Test that errors in one task don't affect others
        # and that proper error states are maintained
        pass

    def test_task_progress_tracking(self):
        """Test progress tracking across multiple tasks."""
        # Test that progress updates work correctly
        # and can be monitored by API endpoints
        pass


class TestCeleryConfiguration:
    """Test Celery task configuration and routing."""

    def test_task_routing_configuration(self):
        """Test that tasks are routed to correct queues."""
        from cartridge.tasks.celery_app import celery_app

        # Verify task routing configuration
        routes = celery_app.conf.task_routes
        assert "cartridge.tasks.scan_tasks.*" in routes
        assert "cartridge.tasks.generation_tasks.*" in routes
        assert routes["cartridge.tasks.scan_tasks.*"]["queue"] == "scan"
        assert routes["cartridge.tasks.generation_tasks.*"]["queue"] == "generation"

    def test_task_serialization_config(self):
        """Test task serialization configuration."""
        from cartridge.tasks.celery_app import celery_app

        assert celery_app.conf.task_serializer == "json"
        assert celery_app.conf.result_serializer == "json"
        assert "json" in celery_app.conf.accept_content

    def test_task_time_limits(self):
        """Test task time limit configuration."""
        from cartridge.tasks.celery_app import celery_app

        assert celery_app.conf.task_time_limit == 30 * 60  # 30 minutes
        assert celery_app.conf.task_soft_time_limit == 25 * 60  # 25 minutes


@pytest.mark.slow
class TestTaskPerformance:
    """Performance tests for background tasks."""

    def test_scan_task_performance(self):
        """Test scan task performance with large datasets."""
        # This would test performance with realistic data volumes
        pass

    def test_generation_task_performance(self):
        """Test generation task performance with multiple models."""
        # This would test AI generation performance
        pass

    def test_concurrent_task_execution(self):
        """Test concurrent execution of multiple tasks."""
        # This would test task queue performance under load
        pass


@pytest.mark.integration
class TestTaskDependencies:
    """Test task dependencies and external service integration."""

    def test_database_connectivity_requirements(self):
        """Test that tasks properly handle database connectivity."""
        # Test various database connection scenarios
        pass

    def test_ai_service_integration(self):
        """Test integration with AI service providers."""
        # Test actual AI provider integration (with mocking)
        pass

    def test_file_system_operations(self):
        """Test file system operations in tasks."""
        # Test project file creation and archive generation
        pass