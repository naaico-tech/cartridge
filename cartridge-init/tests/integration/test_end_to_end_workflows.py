"""End-to-end workflow tests for Cartridge Init."""

import pytest
import asyncio
import json
import tempfile
import tarfile
import io
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from cartridge.scanner.base import ScanResult, DatabaseInfo, TableInfo, ColumnInfo, DataType


class TestCompleteWorkflows:
    """Test complete end-to-end workflows."""

    def test_full_sync_workflow_schema_to_download(self, client):
        """Test complete synchronous workflow from schema scan to project download."""
        # Step 1: Test database connection
        connection_data = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_scanner_factory:
            mock_connector = AsyncMock()
            mock_connector.test_connection.return_value = {
                "status": "success",
                "message": "Connection successful",
                "database_version": "PostgreSQL 13.0",
                "database_type": "postgresql"
            }
            mock_scanner_factory.return_value = mock_connector

            # Test connection
            response = client.post("/api/v1/scanner/test-connection", json=connection_data)
            assert response.status_code == 200
            assert response.json()["status"] == "success"

        # Step 2: Scan database schema
        scan_request = {
            "connection": connection_data,
            "tables": [],
            "include_samples": True,
            "sample_size": 100,
            "async_mode": False
        }

        # Mock scan result
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

        mock_columns = [
            ColumnInfo(
                name="customer_id",
                data_type=DataType.INTEGER,
                nullable=False,
                is_primary_key=True,
                is_foreign_key=False
            ),
            ColumnInfo(
                name="email",
                data_type=DataType.VARCHAR,
                nullable=False,
                is_primary_key=False,
                is_foreign_key=False
            )
        ]

        mock_table = TableInfo(
            name="customers",
            schema="public",
            table_type="table",
            columns=mock_columns,
            constraints=[],
            indexes=[],
            row_count=1000,
            sample_data=[
                {"customer_id": 1, "email": "test1@example.com"},
                {"customer_id": 2, "email": "test2@example.com"}
            ]
        )

        mock_scan_result = ScanResult(
            database_info=mock_db_info,
            tables=[mock_table],
            scan_duration_seconds=2.5,
            scan_timestamp="2024-01-01T00:00:00Z"
        )

        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_scanner_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema.return_value = mock_scan_result
            mock_scanner_factory.return_value = mock_connector

            # Perform schema scan
            response = client.post("/api/v1/scanner/scan", json=scan_request)
            assert response.status_code == 200
            scan_data = response.json()
            assert len(scan_data["tables"]) == 1
            assert scan_data["tables"][0]["name"] == "customers"
            assert len(scan_data["tables"][0]["columns"]) == 2

        # Step 3: Generate dbt models using scan results
        generation_request = {
            "schema_data": scan_data,
            "model_types": ["staging", "marts"],
            "ai_model": "mock",
            "include_tests": True,
            "include_docs": True,
            "async_mode": False
        }

        # Mock AI generation
        mock_staging_model = MagicMock()
        mock_staging_model.name = "stg_customers"
        mock_staging_model.model_type.value = "staging"
        mock_staging_model.sql = "SELECT * FROM {{ source('raw', 'customers') }}"
        mock_staging_model.description = "Staging customers table"
        mock_staging_model.tests = []
        mock_staging_model.dependencies = []

        mock_mart_model = MagicMock()
        mock_mart_model.name = "dim_customers"
        mock_mart_model.model_type.value = "marts"
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

            mock_supported.return_value = ["mock", "gpt-4"]
            
            mock_provider = AsyncMock()
            mock_provider.generate_models.return_value = mock_generation_result
            mock_factory.return_value = mock_provider
            
            mock_analyzer_instance = MagicMock()
            mock_analyzer_instance.detect_fact_tables.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance

            # Generate models
            response = client.post("/api/v1/projects/generate", json=generation_request)
            assert response.status_code == 200
            generation_data = response.json()
            assert len(generation_data["models"]) == 2
            assert generation_data["ai_model_used"] == "mock"
            
            project_id = generation_data["project_id"]

        # Step 4: Download the generated project
        response = client.get(f"/api/v1/projects/{project_id}/download")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/gzip"
        
        # Verify the downloaded file is a valid tar.gz
        content = response.content
        assert len(content) > 0
        assert content[:2] == b'\x1f\x8b'  # gzip magic bytes

        # Step 5: Verify project information can be retrieved
        response = client.get(f"/api/v1/projects/{project_id}")
        assert response.status_code == 200
        project_info = response.json()
        assert project_info["project_id"] == project_id

    def test_full_async_workflow_with_task_monitoring(self, client):
        """Test complete asynchronous workflow with task monitoring."""
        # Step 1: Start async schema scan
        scan_request = {
            "connection": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_password",
                "schema": "public"
            },
            "async_mode": True
        }

        with patch('cartridge.api.routes.scanner.scan_database_schema') as mock_scan_task:
            mock_scan_task.delay.return_value.id = "scan-task-123"

            # Start scan task
            response = client.post("/api/v1/scanner/scan", json=scan_request)
            assert response.status_code == 200
            scan_task_data = response.json()
            assert scan_task_data["task_id"] == "scan-task-123"
            assert scan_task_data["status"] == "PENDING"
            
            scan_task_id = scan_task_data["task_id"]

        # Step 2: Monitor scan task progress
        with patch('cartridge.api.routes.scanner.celery_app.AsyncResult') as mock_result:
            # Simulate task progression
            mock_task_result = MagicMock()
            
            # First check - task is in progress
            mock_task_result.state = "PROGRESS"
            mock_task_result.info = {
                "current": 50,
                "total": 100,
                "status": "Scanning tables..."
            }
            mock_result.return_value = mock_task_result

            response = client.get(f"/api/v1/scanner/tasks/{scan_task_id}")
            assert response.status_code == 200
            progress_data = response.json()
            assert progress_data["status"] == "PROGRESS"
            assert progress_data["progress"]["current"] == 50

            # Second check - task completed
            mock_task_result.state = "SUCCESS"
            mock_task_result.result = {
                "scan_result_id": "scan-123",
                "status": "completed",
                "tables": [
                    {
                        "name": "customers",
                        "schema": "public",
                        "row_count": 1000,
                        "columns": [
                            {
                                "name": "customer_id",
                                "data_type": "integer",
                                "nullable": False,
                                "is_primary_key": True
                            }
                        ]
                    }
                ]
            }

            response = client.get(f"/api/v1/scanner/tasks/{scan_task_id}")
            assert response.status_code == 200
            completed_data = response.json()
            assert completed_data["status"] == "SUCCESS"
            assert len(completed_data["result"]["tables"]) == 1

        # Step 3: Start async model generation using scan results
        generation_request = {
            "schema_data": completed_data["result"],
            "model_types": ["staging", "marts"],
            "ai_model": "gpt-4",
            "async_mode": True
        }

        with patch('cartridge.api.routes.projects.generate_dbt_models') as mock_gen_task:
            mock_gen_task.delay.return_value.id = "gen-task-456"

            response = client.post("/api/v1/projects/generate", json=generation_request)
            assert response.status_code == 200
            gen_task_data = response.json()
            assert gen_task_data["task_id"] == "gen-task-456"
            
            gen_task_id = gen_task_data["task_id"]

        # Step 4: Monitor generation task
        with patch('cartridge.api.routes.projects.celery_app.AsyncResult') as mock_result:
            mock_task_result = MagicMock()
            mock_task_result.state = "SUCCESS"
            mock_task_result.result = {
                "project_id": "proj_abc123",
                "status": "completed",
                "models_generated": 2,
                "archive_path": "/tmp/dbt_projects/proj_abc123.tar.gz"
            }
            mock_result.return_value = mock_task_result

            response = client.get(f"/api/v1/projects/tasks/{gen_task_id}")
            assert response.status_code == 200
            gen_completed_data = response.json()
            assert gen_completed_data["status"] == "SUCCESS"
            assert gen_completed_data["result"]["models_generated"] == 2
            
            project_id = gen_completed_data["result"]["project_id"]

        # Step 5: Download the generated project
        response = client.get(f"/api/v1/projects/{project_id}/download")
        assert response.status_code == 200

    def test_error_handling_in_workflow(self, client):
        """Test error handling throughout the workflow."""
        # Step 1: Test connection failure
        connection_data = {
            "type": "postgresql",
            "host": "invalid_host",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.test_connection.return_value = {
                "status": "failed",
                "message": "Connection failed: Host not found",
                "error": "Host not found"
            }
            mock_factory.return_value = mock_connector

            response = client.post("/api/v1/scanner/test-connection", json=connection_data)
            assert response.status_code == 400
            assert "Connection test failed" in response.json()["detail"]

        # Step 2: Test scan failure
        scan_request = {
            "connection": connection_data,
            "async_mode": False
        }

        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema.side_effect = Exception("Database connection timeout")
            mock_factory.return_value = mock_connector

            response = client.post("/api/v1/scanner/scan", json=scan_request)
            assert response.status_code == 500
            assert "Schema scan failed" in response.json()["detail"]

        # Step 3: Test generation failure with invalid AI model
        generation_request = {
            "schema_data": {"tables": [{"name": "test", "columns": []}]},
            "ai_model": "invalid-model",
            "async_mode": False
        }

        with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported:
            mock_supported.return_value = ["gpt-4", "claude-3-sonnet"]

            response = client.post("/api/v1/projects/generate", json=generation_request)
            assert response.status_code == 400
            assert "Unsupported AI model" in response.json()["detail"]

        # Step 4: Test task failure monitoring
        task_id = "failed-task-789"

        with patch('cartridge.api.routes.scanner.celery_app.AsyncResult') as mock_result:
            mock_task_result = MagicMock()
            mock_task_result.state = "FAILURE"
            mock_task_result.info = Exception("Task execution failed")
            mock_result.return_value = mock_task_result

            response = client.get(f"/api/v1/scanner/tasks/{task_id}")
            assert response.status_code == 200
            error_data = response.json()
            assert error_data["status"] == "FAILURE"
            assert "Task execution failed" in error_data["message"]

    def test_workflow_with_different_database_types(self, client):
        """Test workflow with different supported database types."""
        database_types = ["postgresql", "mysql"]  # Add more when implemented

        for db_type in database_types:
            connection_data = {
                "type": db_type,
                "host": "localhost",
                "port": 5432 if db_type == "postgresql" else 3306,
                "database": "test_db",
                "username": "test_user",
                "password": "test_password",
                "schema": "public"
            }

            with patch('cartridge.api.routes.scanner.ConnectorFactory.get_supported_databases') as mock_supported:
                mock_supported.return_value = database_types

                with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
                    mock_connector = AsyncMock()
                    mock_connector.test_connection.return_value = {
                        "status": "success",
                        "message": "Connection successful",
                        "database_type": db_type
                    }
                    mock_factory.return_value = mock_connector

                    response = client.post("/api/v1/scanner/test-connection", json=connection_data)
                    assert response.status_code == 200
                    assert response.json()["database_info"]["type"] == db_type

    def test_workflow_with_different_ai_models(self, client):
        """Test workflow with different AI models."""
        ai_models = ["mock", "gpt-4", "claude-3-sonnet", "gemini-1.5-pro"]
        
        schema_data = {
            "tables": [
                {
                    "name": "customers",
                    "schema": "public",
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
        }

        for ai_model in ai_models:
            generation_request = {
                "schema_data": schema_data,
                "model_types": ["staging"],
                "ai_model": ai_model,
                "async_mode": False
            }

            # Mock AI generation for each model
            mock_model = MagicMock()
            mock_model.name = "stg_customers"
            mock_model.model_type.value = "staging"
            mock_model.sql = f"-- Generated by {ai_model}\nSELECT * FROM customers"
            mock_model.description = f"Model generated by {ai_model}"
            mock_model.tests = []
            mock_model.dependencies = []

            mock_generation_result = MagicMock()
            mock_generation_result.models = [mock_model]
            mock_generation_result.generation_metadata = {"ai_provider": ai_model}

            with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported, \
                 patch('cartridge.api.routes.projects.AIProviderFactory.create_provider') as mock_factory, \
                 patch('cartridge.api.routes.projects.SchemaAnalyzer') as mock_analyzer:

                mock_supported.return_value = ai_models
                
                mock_provider = AsyncMock()
                mock_provider.generate_models.return_value = mock_generation_result
                mock_factory.return_value = mock_provider
                
                mock_analyzer_instance = MagicMock()
                mock_analyzer_instance.detect_fact_tables.return_value = []
                mock_analyzer.return_value = mock_analyzer_instance

                response = client.post("/api/v1/projects/generate", json=generation_request)
                assert response.status_code == 200
                data = response.json()
                assert data["ai_model_used"] == ai_model
                assert len(data["models"]) == 1


class TestWorkflowValidation:
    """Test validation across workflow steps."""

    def test_schema_data_consistency(self, client):
        """Test that schema data is consistent across workflow steps."""
        # Test that the schema data from scan can be used directly in generation
        pass

    def test_model_dependency_validation(self, client):
        """Test validation of model dependencies in generated projects."""
        # Test that generated models have valid dependencies
        pass

    def test_project_structure_validation(self, client):
        """Test validation of generated project structure."""
        # Test that downloaded projects have valid dbt structure
        pass


class TestWorkflowPerformance:
    """Performance tests for complete workflows."""

    @pytest.mark.slow
    def test_large_schema_workflow_performance(self, client):
        """Test workflow performance with large schemas."""
        # Test with many tables and columns
        pass

    @pytest.mark.slow
    def test_multiple_model_generation_performance(self, client):
        """Test performance when generating many models."""
        # Test with all model types and many tables
        pass

    @pytest.mark.slow
    def test_concurrent_workflow_execution(self, client):
        """Test concurrent execution of multiple workflows."""
        # Test system behavior under concurrent load
        pass


class TestWorkflowRecovery:
    """Test workflow recovery and resilience."""

    def test_task_retry_behavior(self, client):
        """Test task retry behavior on failures."""
        # Test that failed tasks can be retried
        pass

    def test_partial_failure_recovery(self, client):
        """Test recovery from partial failures."""
        # Test behavior when some models fail to generate
        pass

    def test_resource_cleanup(self, client):
        """Test that resources are properly cleaned up."""
        # Test that temporary files and connections are cleaned up
        pass


@pytest.mark.integration
class TestRealWorldScenarios:
    """Test realistic usage scenarios."""

    def test_ecommerce_schema_workflow(self, client):
        """Test workflow with realistic e-commerce schema."""
        ecommerce_schema = {
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
                            "name": "first_name",
                            "type": "varchar",
                            "nullable": True,
                            "primary_key": False,
                            "foreign_key": False
                        },
                        {
                            "name": "last_name",
                            "type": "varchar",
                            "nullable": True,
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
                            "name": "order_date",
                            "type": "date",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": False
                        },
                        {
                            "name": "total_amount",
                            "type": "decimal",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": False
                        },
                        {
                            "name": "status",
                            "type": "varchar",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": False
                        }
                    ]
                },
                {
                    "name": "order_items",
                    "schema": "public",
                    "table_type": "table",
                    "row_count": 150000,
                    "columns": [
                        {
                            "name": "item_id",
                            "type": "integer",
                            "nullable": False,
                            "primary_key": True,
                            "foreign_key": False
                        },
                        {
                            "name": "order_id",
                            "type": "integer",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": True
                        },
                        {
                            "name": "product_id",
                            "type": "integer",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": True
                        },
                        {
                            "name": "quantity",
                            "type": "integer",
                            "nullable": False,
                            "primary_key": False,
                            "foreign_key": False
                        },
                        {
                            "name": "unit_price",
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
            "schema_data": ecommerce_schema,
            "model_types": ["staging", "intermediate", "marts"],
            "ai_model": "mock",
            "include_tests": True,
            "include_docs": True,
            "business_context": "E-commerce analytics platform",
            "async_mode": False
        }

        # Mock realistic model generation
        mock_models = []
        
        # Staging models
        for table in ecommerce_schema["tables"]:
            mock_model = MagicMock()
            mock_model.name = f"stg_{table['name']}"
            mock_model.model_type.value = "staging"
            mock_model.sql = f"SELECT * FROM {{{{ source('raw', '{table['name']}') }}}}"
            mock_model.description = f"Staging table for {table['name']}"
            mock_model.tests = []
            mock_model.dependencies = []
            mock_models.append(mock_model)

        # Intermediate model
        intermediate_model = MagicMock()
        intermediate_model.name = "int_customer_order_summary"
        intermediate_model.model_type.value = "intermediate"
        intermediate_model.sql = "SELECT customer_id, COUNT(*) as order_count FROM {{ ref('stg_orders') }} GROUP BY customer_id"
        intermediate_model.description = "Customer order summary"
        intermediate_model.tests = []
        intermediate_model.dependencies = ["stg_orders"]
        mock_models.append(intermediate_model)

        # Mart models
        mart_model = MagicMock()
        mart_model.name = "dim_customers"
        mart_model.model_type.value = "marts"
        mart_model.sql = "SELECT c.*, cos.order_count FROM {{ ref('stg_customers') }} c LEFT JOIN {{ ref('int_customer_order_summary') }} cos ON c.customer_id = cos.customer_id"
        mart_model.description = "Customer dimension with order metrics"
        mart_model.tests = []
        mart_model.dependencies = ["stg_customers", "int_customer_order_summary"]
        mock_models.append(mart_model)

        mock_generation_result = MagicMock()
        mock_generation_result.models = mock_models
        mock_generation_result.generation_metadata = {"ai_provider": "mock"}

        with patch('cartridge.api.routes.projects.AIProviderFactory.get_supported_models') as mock_supported, \
             patch('cartridge.api.routes.projects.AIProviderFactory.create_provider') as mock_factory, \
             patch('cartridge.api.routes.projects.SchemaAnalyzer') as mock_analyzer:

            mock_supported.return_value = ["mock"]
            
            mock_provider = AsyncMock()
            mock_provider.generate_models.return_value = mock_generation_result
            mock_factory.return_value = mock_provider
            
            mock_analyzer_instance = MagicMock()
            mock_analyzer_instance.detect_fact_tables.return_value = ["orders", "order_items"]
            mock_analyzer.return_value = mock_analyzer_instance

            response = client.post("/api/v1/projects/generate", json=generation_request)
            assert response.status_code == 200
            data = response.json()
            
            # Verify realistic model generation
            assert len(data["models"]) == 5  # 3 staging + 1 intermediate + 1 mart
            model_names = [model["name"] for model in data["models"]]
            assert "stg_customers" in model_names
            assert "stg_orders" in model_names
            assert "stg_order_items" in model_names
            assert "int_customer_order_summary" in model_names
            assert "dim_customers" in model_names

            # Test project download
            project_id = data["project_id"]
            response = client.get(f"/api/v1/projects/{project_id}/download")
            assert response.status_code == 200

    def test_data_warehouse_schema_workflow(self, client):
        """Test workflow with data warehouse schema patterns."""
        # Test with typical data warehouse patterns (fact/dimension tables)
        pass

    def test_saas_application_schema_workflow(self, client):
        """Test workflow with SaaS application schema."""
        # Test with typical SaaS patterns (users, subscriptions, events)
        pass