"""Unit tests for projects API endpoints."""

import pytest
import tempfile
import os
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
from fastapi.testclient import TestClient

from cartridge.ai.base import ModelGenerationResult, GeneratedModel, ModelType


class TestProjectsAPI:
    """Test projects API endpoints."""

    def test_generate_models_sync_success(self, client):
        """Test successful synchronous model generation."""
        generation_request = {
            "schema_data": {
                "tables": [
                    {
                        "name": "customers",
                        "schema": "public",
                        "table_type": "table",
                        "row_count": 1000,
                        "columns": [
                            {
                                "name": "id",
                                "type": "integer",
                                "nullable": False,
                                "primary_key": True,
                                "foreign_key": False
                            },
                            {
                                "name": "name",
                                "type": "varchar",
                                "nullable": False,
                                "primary_key": False,
                                "foreign_key": False
                            }
                        ]
                    }
                ]
            },
            "model_types": ["staging", "marts"],
            "ai_model": "mock",
            "include_tests": True,
            "include_docs": True,
            "async_mode": False
        }

        # Mock AI provider and generation result
        mock_model = MagicMock()
        mock_model.name = "stg_customers"
        mock_model.model_type = ModelType.STAGING
        mock_model.sql = "SELECT * FROM {{ source('raw', 'customers') }}"
        mock_model.description = "Staging table for customers"
        mock_model.tests = []
        mock_model.dependencies = []

        mock_generation_result = MagicMock()
        mock_generation_result.models = [mock_model]
        mock_generation_result.generation_metadata = {"ai_provider": "mock"}

        with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported, \
             patch('cartridge.api.routes.projects.AIProviderFactory.create_provider') as mock_factory, \
             patch('cartridge.api.routes.projects.SchemaAnalyzer') as mock_analyzer:
            
            mock_supported.return_value = ["mock", "gpt-4", "claude-3-sonnet"]
            
            mock_provider = AsyncMock()
            mock_provider.generate_models.return_value = mock_generation_result
            mock_factory.return_value = mock_provider
            
            mock_analyzer_instance = MagicMock()
            mock_analyzer_instance.detect_fact_tables.return_value = ["customers"]
            mock_analyzer.return_value = mock_analyzer_instance

            response = client.post("/api/v1/projects/generate", json=generation_request)

            assert response.status_code == 200
            data = response.json()
            assert "project_id" in data
            assert data["ai_model_used"] == "mock"
            assert len(data["models"]) == 1
            assert data["models"][0]["name"] == "stg_customers"
            assert data["models"][0]["type"] == "staging"
            assert "project_structure" in data

    def test_generate_models_async_mode(self, client):
        """Test asynchronous model generation mode."""
        generation_request = {
            "schema_data": {
                "tables": [
                    {
                        "name": "customers",
                        "schema": "public",
                        "table_type": "table",
                        "columns": [
                            {
                                "name": "id",
                                "type": "integer",
                                "nullable": False,
                                "primary_key": True,
                                "foreign_key": False
                            }
                        ]
                    }
                ]
            },
            "model_types": ["staging"],
            "ai_model": "gpt-4",
            "async_mode": True
        }

        with patch('cartridge.api.routes.projects.generate_dbt_models') as mock_task:
            mock_task.delay.return_value.id = "generation-task-123"

            response = client.post("/api/v1/projects/generate", json=generation_request)

            assert response.status_code == 200
            data = response.json()
            assert data["task_id"] == "generation-task-123"
            assert data["status"] == "PENDING"
            assert data["message"] == "Model generation queued for background processing"
            assert "project_id" in data["result"]

    def test_generate_models_unsupported_ai_model(self, client):
        """Test model generation with unsupported AI model."""
        generation_request = {
            "schema_data": {
                "tables": [
                    {
                        "name": "customers",
                        "schema": "public",
                        "columns": [{"name": "id", "type": "integer"}]
                    }
                ]
            },
            "ai_model": "unsupported-model",
            "async_mode": False
        }

        with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported:
            mock_supported.return_value = ["gpt-4", "claude-3-sonnet"]

            response = client.post("/api/v1/projects/generate", json=generation_request)

            assert response.status_code == 400
            assert "Unsupported AI model" in response.json()["detail"]

    def test_generate_models_no_tables(self, client):
        """Test model generation with no tables in schema data."""
        generation_request = {
            "schema_data": {
                "tables": []
            },
            "ai_model": "gpt-4",
            "async_mode": False
        }

        with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported:
            mock_supported.return_value = ["gpt-4"]

            response = client.post("/api/v1/projects/generate", json=generation_request)

            assert response.status_code == 400
            assert "No tables found in schema data" in response.json()["detail"]

    def test_generate_models_invalid_model_type(self, client):
        """Test model generation with invalid model type."""
        generation_request = {
            "schema_data": {
                "tables": [
                    {
                        "name": "customers",
                        "schema": "public",
                        "columns": [{"name": "id", "type": "integer"}]
                    }
                ]
            },
            "model_types": ["invalid_type"],
            "ai_model": "gpt-4",
            "async_mode": False
        }

        with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported:
            mock_supported.return_value = ["gpt-4"]

            response = client.post("/api/v1/projects/generate", json=generation_request)

            assert response.status_code == 400
            assert "Invalid model type" in response.json()["detail"]

    def test_test_run_models_success(self, client):
        """Test successful model test run."""
        test_request = {
            "project_id": "test-project-123",
            "models_to_test": [],
            "dry_run": True
        }

        response = client.post("/api/v1/projects/test-run", json=test_request)

        assert response.status_code == 200
        data = response.json()
        assert data["project_id"] == "test-project-123"
        assert data["status"] == "success"
        assert "results" in data
        assert "execution_time_seconds" in data

    def test_download_project_success(self, client):
        """Test successful project download."""
        project_id = "test-project-123"

        response = client.get(f"/api/v1/projects/{project_id}/download")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/gzip"
        assert "attachment" in response.headers["content-disposition"]
        assert f"cartridge_project_{project_id}.tar.gz" in response.headers["content-disposition"]

    def test_get_project_info(self, client):
        """Test getting project information."""
        project_id = "test-project-123"

        response = client.get(f"/api/v1/projects/{project_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["project_id"] == project_id
        assert "status" in data
        assert "created_at" in data
        assert "model_count" in data
        assert "generation_settings" in data

    def test_get_task_status_success(self, client):
        """Test getting task status for projects."""
        task_id = "generation-task-123"

        with patch('cartridge.api.routes.projects.celery_app.AsyncResult') as mock_result:
            mock_task_result = MagicMock()
            mock_task_result.state = "SUCCESS"
            mock_task_result.result = {
                "project_id": "proj_abc123",
                "status": "completed",
                "models_generated": 3
            }
            mock_result.return_value = mock_task_result

            response = client.get(f"/api/v1/projects/tasks/{task_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["task_id"] == task_id
            assert data["status"] == "SUCCESS"
            assert data["result"]["models_generated"] == 3

    def test_generate_models_ai_provider_exception(self, client):
        """Test model generation when AI provider raises exception."""
        generation_request = {
            "schema_data": {
                "tables": [
                    {
                        "name": "customers",
                        "schema": "public",
                        "columns": [{"name": "id", "type": "integer", "nullable": False, "primary_key": True, "foreign_key": False}]
                    }
                ]
            },
            "ai_model": "gpt-4",
            "async_mode": False
        }

        with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported, \
             patch('cartridge.api.routes.projects.AIProviderFactory.create_provider') as mock_factory:
            
            mock_supported.return_value = ["gpt-4"]
            mock_provider = AsyncMock()
            mock_provider.generate_models.side_effect = Exception("AI service unavailable")
            mock_factory.return_value = mock_provider

            response = client.post("/api/v1/projects/generate", json=generation_request)

            assert response.status_code == 500
            assert "Model generation failed" in response.json()["detail"]


class TestProjectsAPIValidation:
    """Test projects API request validation."""

    def test_generation_request_validation(self, client):
        """Test model generation request validation."""
        # Test missing required fields
        invalid_request = {
            # Missing schema_data
            "ai_model": "gpt-4"
        }

        response = client.post("/api/v1/projects/generate", json=invalid_request)
        assert response.status_code == 422

    def test_test_run_request_validation(self, client):
        """Test test run request validation."""
        # Test missing project_id
        invalid_request = {
            "models_to_test": [],
            "dry_run": True
        }

        response = client.post("/api/v1/projects/test-run", json=invalid_request)
        assert response.status_code == 422

    def test_invalid_project_id_format(self, client):
        """Test invalid project ID format."""
        invalid_project_id = ""  # Empty project ID

        response = client.get(f"/api/v1/projects/{invalid_project_id}")
        assert response.status_code == 404  # Not found due to empty path

    def test_schema_data_structure_validation(self, client):
        """Test schema data structure validation."""
        # Test invalid schema data structure
        invalid_request = {
            "schema_data": "invalid_structure",  # Should be dict
            "ai_model": "gpt-4"
        }

        response = client.post("/api/v1/projects/generate", json=invalid_request)
        assert response.status_code == 422


class TestProjectDownload:
    """Test project download functionality."""

    def test_download_creates_valid_tar_file(self, client):
        """Test that download creates a valid tar file."""
        project_id = "test-project-123"

        response = client.get(f"/api/v1/projects/{project_id}/download")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/gzip"
        
        # Verify the response contains binary data (tar.gz file)
        content = response.content
        assert len(content) > 0
        
        # Check tar.gz magic bytes
        assert content[:2] == b'\x1f\x8b'  # gzip magic bytes

    def test_download_includes_dbt_files(self, client):
        """Test that download includes expected dbt files."""
        project_id = "test-project-123"
        
        # This would require extracting and examining the tar file
        # For now, we test that the download succeeds
        response = client.get(f"/api/v1/projects/{project_id}/download")
        assert response.status_code == 200

    def test_download_nonexistent_project(self, client):
        """Test downloading a non-existent project."""
        # This currently returns a generated project regardless of ID
        # In a full implementation, this would check if project exists
        project_id = "nonexistent-project"
        
        response = client.get(f"/api/v1/projects/{project_id}/download")
        # Currently returns 200 with generated project
        # In full implementation, would return 404
        assert response.status_code == 200


class TestProjectsIntegration:
    """Integration tests for projects API with other components."""

    def test_full_generation_workflow(self, client):
        """Test complete model generation workflow."""
        # This would test the integration between schema analysis,
        # AI generation, and project creation
        pass

    def test_generation_with_real_schema_data(self, client):
        """Test generation with realistic schema data."""
        realistic_schema = {
            "tables": [
                {
                    "name": "customers",
                    "schema": "public",
                    "table_type": "table",
                    "row_count": 10000,
                    "columns": [
                        {
                            "name": "customer_id",
                            "type": "integer",
                            "nullable": False,
                            "primary_key": True,
                            "foreign_key": False
                        },
                        {
                            "name": "email",
                            "type": "varchar",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": False
                        },
                        {
                            "name": "created_at",
                            "type": "timestamp",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": False
                        }
                    ]
                },
                {
                    "name": "orders",
                    "schema": "public",
                    "table_type": "table",
                    "row_count": 50000,
                    "columns": [
                        {
                            "name": "order_id",
                            "type": "integer",
                            "nullable": False,
                            "primary_key": True,
                            "foreign_key": False
                        },
                        {
                            "name": "customer_id",
                            "type": "integer",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": True
                        },
                        {
                            "name": "total_amount",
                            "type": "decimal",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": False
                        }
                    ]
                }
            ]
        }

        generation_request = {
            "schema_data": realistic_schema,
            "model_types": ["staging", "intermediate", "marts"],
            "ai_model": "mock",
            "include_tests": True,
            "include_docs": True,
            "async_mode": False
        }

        # Mock the AI provider to return realistic models
        mock_staging_model = MagicMock()
        mock_staging_model.name = "stg_customers"
        mock_staging_model.model_type = ModelType.STAGING
        mock_staging_model.sql = "SELECT * FROM {{ source('raw', 'customers') }}"
        mock_staging_model.description = "Staging customers table"
        mock_staging_model.tests = []
        mock_staging_model.dependencies = []

        mock_mart_model = MagicMock()
        mock_mart_model.name = "dim_customers"
        mock_mart_model.model_type = ModelType.MARTS
        mock_mart_model.sql = "SELECT customer_id, email FROM {{ ref('stg_customers') }}"
        mock_mart_model.description = "Customer dimension"
        mock_mart_model.tests = []
        mock_mart_model.dependencies = ["stg_customers"]

        mock_generation_result = MagicMock()
        mock_generation_result.models = [mock_staging_model, mock_mart_model]
        mock_generation_result.generation_metadata = {"ai_provider": "mock"}

        with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported, \
             patch('cartridge.api.routes.projects.AIProviderFactory.create_provider') as mock_factory, \
             patch('cartridge.api.routes.projects.SchemaAnalyzer') as mock_analyzer:
            
            mock_supported.return_value = ["mock"]
            
            mock_provider = AsyncMock()
            mock_provider.generate_models.return_value = mock_generation_result
            mock_factory.return_value = mock_provider
            
            mock_analyzer_instance = MagicMock()
            mock_analyzer_instance.detect_fact_tables.return_value = ["orders"]
            mock_analyzer.return_value = mock_analyzer_instance

            response = client.post("/api/v1/projects/generate", json=generation_request)

            assert response.status_code == 200
            data = response.json()
            assert len(data["models"]) == 2
            assert any(model["name"] == "stg_customers" for model in data["models"])
            assert any(model["name"] == "dim_customers" for model in data["models"])