"""Tests for Celery background tasks."""

import pytest
from unittest.mock import patch, MagicMock
from celery import Celery

from cartridge.tasks.scan_tasks import scan_database_schema, test_database_connection
from cartridge.tasks.generation_tasks import generate_dbt_models, create_project_archive
from cartridge.tasks.test_tasks import test_dbt_models, validate_dbt_project


class TestScanTasks:
    """Test schema scanning tasks."""
    
    @patch("cartridge.tasks.scan_tasks.time.sleep")
    def test_scan_database_schema_success(self, mock_sleep):
        """Test successful database schema scan."""
        scan_result_id = "test-scan-123"
        connection_config = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password"
        }
        
        # Mock the task to avoid actual execution
        with patch.object(scan_database_schema, 'update_state') as mock_update:
            result = scan_database_schema.apply(
                args=[scan_result_id, connection_config]
            ).get()
            
            assert result["scan_result_id"] == scan_result_id
            assert result["status"] == "completed"
            assert result["tables_found"] == 5
            assert result["columns_analyzed"] == 25
            assert result["duration_seconds"] == 4.0
            assert len(result["tables"]) == 1
            
            # Check that progress updates were called
            assert mock_update.call_count >= 2
    
    @patch("cartridge.tasks.scan_tasks.time.sleep")
    def test_scan_database_schema_failure(self, mock_sleep):
        """Test database schema scan failure."""
        scan_result_id = "test-scan-123"
        connection_config = {"type": "invalid"}
        
        with patch.object(scan_database_schema, 'update_state') as mock_update:
            # Mock an exception during execution
            with patch("cartridge.tasks.scan_tasks.time.sleep", side_effect=Exception("Database error")):
                with pytest.raises(Exception):
                    scan_database_schema.apply(
                        args=[scan_result_id, connection_config]
                    ).get()
                
                # Check that failure state was set
                mock_update.assert_called_with(
                    state="FAILURE",
                    meta={"error": "Database error", "scan_result_id": scan_result_id}
                )
    
    @patch("cartridge.tasks.scan_tasks.time.sleep")
    def test_test_database_connection_success(self, mock_sleep):
        """Test successful database connection test."""
        connection_config = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db"
        }
        
        result = test_database_connection.apply(
            args=[connection_config]
        ).get()
        
        assert result["status"] == "success"
        assert result["message"] == "Connection successful"
        assert "database_version" in result
        assert "response_time_ms" in result
    
    @patch("cartridge.tasks.scan_tasks.time.sleep", side_effect=Exception("Connection failed"))
    def test_test_database_connection_failure(self, mock_sleep):
        """Test database connection test failure."""
        connection_config = {"type": "postgresql"}
        
        result = test_database_connection.apply(
            args=[connection_config]
        ).get()
        
        assert result["status"] == "failed"
        assert "Connection failed" in result["message"]
        assert "error" in result


class TestGenerationTasks:
    """Test model generation tasks."""
    
    @patch("cartridge.tasks.generation_tasks.time.sleep")
    def test_generate_dbt_models_success(self, mock_sleep):
        """Test successful dbt model generation."""
        project_id = "test-project-123"
        schema_data = {
            "tables": [
                {
                    "name": "customers",
                    "columns": [
                        {"name": "id", "type": "integer"},
                        {"name": "email", "type": "varchar"}
                    ]
                }
            ]
        }
        generation_config = {
            "ai_model": "gpt-4",
            "model_types": ["staging", "marts"],
            "include_tests": True
        }
        
        with patch.object(generate_dbt_models, 'update_state') as mock_update:
            result = generate_dbt_models.apply(
                args=[project_id, schema_data, generation_config]
            ).get()
            
            assert result["project_id"] == project_id
            assert result["status"] == "completed"
            assert result["models_generated"] == 2
            assert len(result["models"]) == 2
            assert result["ai_model_used"] == "gpt-4"
            
            # Check model structure
            staging_model = next(m for m in result["models"] if m["type"] == "staging")
            assert staging_model["name"] == "stg_customers"
            assert "SELECT" in staging_model["sql"]
            assert len(staging_model["tests"]) == 2
            
            marts_model = next(m for m in result["models"] if m["type"] == "marts")
            assert marts_model["name"] == "dim_customers"
            assert "ref('stg_customers')" in marts_model["sql"]
            
            # Check that progress updates were called
            assert mock_update.call_count >= 4
    
    @patch("cartridge.tasks.generation_tasks.time.sleep", side_effect=Exception("AI API error"))
    def test_generate_dbt_models_failure(self, mock_sleep):
        """Test dbt model generation failure."""
        project_id = "test-project-123"
        schema_data = {"tables": []}
        generation_config = {"ai_model": "gpt-4"}
        
        with patch.object(generate_dbt_models, 'update_state') as mock_update:
            with pytest.raises(Exception):
                generate_dbt_models.apply(
                    args=[project_id, schema_data, generation_config]
                ).get()
            
            # Check that failure state was set
            mock_update.assert_called_with(
                state="FAILURE",
                meta={"error": "AI API error", "project_id": project_id}
            )
    
    @patch("cartridge.tasks.generation_tasks.time.sleep")
    def test_create_project_archive_success(self, mock_sleep):
        """Test successful project archive creation."""
        project_id = "test-project-123"
        project_path = "/app/output/test-project-123"
        
        result = create_project_archive.apply(
            args=[project_id, project_path]
        ).get()
        
        assert result["project_id"] == project_id
        assert result["status"] == "completed"
        assert result["archive_path"] == f"/app/output/{project_id}.tar.gz"
        assert result["archive_size_bytes"] > 0
    
    @patch("cartridge.tasks.generation_tasks.time.sleep", side_effect=Exception("Archive error"))
    def test_create_project_archive_failure(self, mock_sleep):
        """Test project archive creation failure."""
        project_id = "test-project-123"
        project_path = "/app/output/test-project-123"
        
        with pytest.raises(Exception):
            create_project_archive.apply(
                args=[project_id, project_path]
            ).get()


class TestTestTasks:
    """Test dbt model testing tasks."""
    
    @patch("cartridge.tasks.test_tasks.time.sleep")
    def test_test_dbt_models_success_dry_run(self, mock_sleep):
        """Test successful dbt model testing (dry run)."""
        project_id = "test-project-123"
        project_path = "/app/output/test-project-123"
        test_config = {"dry_run": True}
        
        with patch.object(test_dbt_models, 'update_state') as mock_update:
            result = test_dbt_models.apply(
                args=[project_id, project_path, test_config]
            ).get()
            
            assert result["project_id"] == project_id
            assert result["status"] == "success"
            assert result["dry_run"] is True
            assert result["models_tested"] == 2
            assert result["models_passed"] == 2
            assert result["models_failed"] == 0
            assert len(result["results"]) == 2
            assert len(result["errors"]) == 0
            
            # Check that no rows were affected in dry run
            for model_result in result["results"]:
                assert model_result["rows_affected"] == 0
            
            # Check progress updates
            assert mock_update.call_count >= 4
    
    @patch("cartridge.tasks.test_tasks.time.sleep")
    def test_test_dbt_models_success_actual_run(self, mock_sleep):
        """Test successful dbt model testing (actual run)."""
        project_id = "test-project-123"
        project_path = "/app/output/test-project-123"
        test_config = {"dry_run": False}
        
        with patch.object(test_dbt_models, 'update_state') as mock_update:
            result = test_dbt_models.apply(
                args=[project_id, project_path, test_config]
            ).get()
            
            assert result["dry_run"] is False
            
            # Check that rows were affected in actual run
            for model_result in result["results"]:
                assert model_result["rows_affected"] > 0
    
    @patch("cartridge.tasks.test_tasks.time.sleep", side_effect=Exception("dbt error"))
    def test_test_dbt_models_failure(self, mock_sleep):
        """Test dbt model testing failure."""
        project_id = "test-project-123"
        project_path = "/app/output/test-project-123"
        test_config = {"dry_run": True}
        
        with patch.object(test_dbt_models, 'update_state') as mock_update:
            with pytest.raises(Exception):
                test_dbt_models.apply(
                    args=[project_id, project_path, test_config]
                ).get()
            
            # Check that failure state was set
            mock_update.assert_called_with(
                state="FAILURE",
                meta={"error": "dbt error", "project_id": project_id}
            )
    
    @patch("cartridge.tasks.test_tasks.time.sleep")
    def test_validate_dbt_project_success(self, mock_sleep):
        """Test successful dbt project validation."""
        project_id = "test-project-123"
        project_path = "/app/output/test-project-123"
        
        result = validate_dbt_project.apply(
            args=[project_id, project_path]
        ).get()
        
        assert result["project_id"] == project_id
        assert result["status"] == "valid"
        assert isinstance(result["issues"], list)
        assert isinstance(result["warnings"], list)
        assert result["models_validated"] == 2
        assert result["tests_validated"] == 3
        
        # Check warning structure
        if result["warnings"]:
            warning = result["warnings"][0]
            assert "type" in warning
            assert "message" in warning
            assert "severity" in warning
    
    @patch("cartridge.tasks.test_tasks.time.sleep", side_effect=Exception("Validation error"))
    def test_validate_dbt_project_failure(self, mock_sleep):
        """Test dbt project validation failure."""
        project_id = "test-project-123"
        project_path = "/app/output/test-project-123"
        
        with pytest.raises(Exception):
            validate_dbt_project.apply(
                args=[project_id, project_path]
            ).get()


class TestTaskConfiguration:
    """Test task configuration and routing."""
    
    def test_task_registration(self):
        """Test that all tasks are properly registered."""
        from cartridge.tasks.celery_app import celery_app
        
        registered_tasks = list(celery_app.tasks.keys())
        
        # Check that our custom tasks are registered
        assert "cartridge.tasks.scan_tasks.scan_database_schema" in registered_tasks
        assert "cartridge.tasks.scan_tasks.test_database_connection" in registered_tasks
        assert "cartridge.tasks.generation_tasks.generate_dbt_models" in registered_tasks
        assert "cartridge.tasks.generation_tasks.create_project_archive" in registered_tasks
        assert "cartridge.tasks.test_tasks.test_dbt_models" in registered_tasks
        assert "cartridge.tasks.test_tasks.validate_dbt_project" in registered_tasks
    
    def test_task_routing_configuration(self):
        """Test task routing configuration."""
        from cartridge.tasks.celery_app import celery_app
        
        routes = celery_app.conf.task_routes
        
        assert "cartridge.tasks.scan_tasks.*" in routes
        assert "cartridge.tasks.generation_tasks.*" in routes
        assert "cartridge.tasks.test_tasks.*" in routes
        
        assert routes["cartridge.tasks.scan_tasks.*"]["queue"] == "scan"
        assert routes["cartridge.tasks.generation_tasks.*"]["queue"] == "generation"
        assert routes["cartridge.tasks.test_tasks.*"]["queue"] == "test"
    
    def test_celery_configuration(self):
        """Test Celery app configuration."""
        from cartridge.tasks.celery_app import celery_app
        
        assert celery_app.conf.task_serializer == "json"
        assert celery_app.conf.accept_content == ["json"]
        assert celery_app.conf.result_serializer == "json"
        assert celery_app.conf.timezone == "UTC"
        assert celery_app.conf.enable_utc is True
        assert celery_app.conf.task_track_started is True
        assert celery_app.conf.task_time_limit == 30 * 60  # 30 minutes
        assert celery_app.conf.task_soft_time_limit == 25 * 60  # 25 minutes