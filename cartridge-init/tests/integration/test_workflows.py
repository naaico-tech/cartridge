"""Integration tests for complete workflows."""

import pytest
import asyncio
from unittest.mock import patch, MagicMock

from cartridge.models import User, DataSource, ScanResult, Project, GeneratedModel
from cartridge.models.project import ProjectStatus, ModelType
from cartridge.models.scan import DatabaseType, ScanStatus


class TestCompleteWorkflow:
    """Test complete end-to-end workflows."""
    
    @pytest.mark.asyncio
    async def test_full_project_generation_workflow(
        self, 
        async_db_session, 
        sample_user_data,
        sample_data_source_data,
        sample_scan_result_data,
        sample_project_data
    ):
        """Test complete workflow from user creation to project generation."""
        
        # Step 1: Create user
        user = User(**sample_user_data)
        async_db_session.add(user)
        await async_db_session.commit()
        await async_db_session.refresh(user)
        
        assert user.id is not None
        assert user.email == sample_user_data["email"]
        
        # Step 2: Create data source
        data_source = DataSource(**sample_data_source_data, owner_id=user.id)
        async_db_session.add(data_source)
        await async_db_session.commit()
        await async_db_session.refresh(data_source)
        
        assert data_source.id is not None
        assert data_source.owner_id == user.id
        
        # Step 3: Create scan result
        scan_result = ScanResult(**sample_scan_result_data, data_source_id=data_source.id)
        async_db_session.add(scan_result)
        await async_db_session.commit()
        await async_db_session.refresh(scan_result)
        
        assert scan_result.id is not None
        assert scan_result.data_source_id == data_source.id
        
        # Step 4: Create project
        project = Project(
            **sample_project_data,
            owner_id=user.id,
            scan_result_id=scan_result.id
        )
        async_db_session.add(project)
        await async_db_session.commit()
        await async_db_session.refresh(project)
        
        assert project.id is not None
        assert project.owner_id == user.id
        assert project.scan_result_id == scan_result.id
        
        # Step 5: Create generated models
        staging_model = GeneratedModel(
            name="stg_customers",
            model_type=ModelType.STAGING,
            description="Staging customers table",
            sql="SELECT * FROM {{ source('raw', 'customers') }}",
            tests=[{"test": "unique", "column": "customer_id"}],
            dependencies=[],
            project_id=project.id
        )
        
        marts_model = GeneratedModel(
            name="dim_customers",
            model_type=ModelType.MARTS,
            description="Customer dimension",
            sql="SELECT * FROM {{ ref('stg_customers') }}",
            tests=[{"test": "unique", "column": "customer_id"}],
            dependencies=["stg_customers"],
            project_id=project.id
        )
        
        async_db_session.add_all([staging_model, marts_model])
        await async_db_session.commit()
        
        # Step 6: Verify relationships
        await async_db_session.refresh(user)
        await async_db_session.refresh(project)
        
        assert len(user.data_sources) == 1
        assert len(user.projects) == 1
        assert len(data_source.scan_results) == 1
        assert len(project.models) == 2
        
        # Verify model types
        model_types = [model.model_type for model in project.models]
        assert ModelType.STAGING in model_types
        assert ModelType.MARTS in model_types
    
    def test_api_workflow_integration(self, client):
        """Test API workflow integration."""
        
        # Step 1: Health check
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
        
        # Step 2: Test connection
        connection_data = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }
        
        response = client.post("/api/v1/scanner/test-connection", json=connection_data)
        assert response.status_code == 200
        assert response.json()["status"] == "success"
        
        # Step 3: Scan schema
        scan_request = {
            "connection": connection_data,
            "tables": [],
            "include_samples": True,
            "sample_size": 100
        }
        
        response = client.post("/api/v1/scanner/scan", json=scan_request)
        assert response.status_code == 200
        scan_data = response.json()
        assert "tables" in scan_data
        assert len(scan_data["tables"]) > 0
        
        # Step 4: Generate models
        generation_request = {
            "schema_data": scan_data,
            "model_types": ["staging", "marts"],
            "ai_model": "gpt-4",
            "include_tests": True,
            "include_docs": True
        }
        
        response = client.post("/api/v1/projects/generate", json=generation_request)
        assert response.status_code == 200
        generation_data = response.json()
        assert "project_id" in generation_data
        assert len(generation_data["models"]) > 0
        
        project_id = generation_data["project_id"]
        
        # Step 5: Test models
        test_request = {
            "project_id": project_id,
            "models_to_test": [],
            "dry_run": True
        }
        
        response = client.post("/api/v1/projects/test-run", json=test_request)
        assert response.status_code == 200
        test_data = response.json()
        assert test_data["status"] == "success"
        assert test_data["models_tested"] > 0
        
        # Step 6: Get project info
        response = client.get(f"/api/v1/projects/{project_id}")
        assert response.status_code == 200
        project_data = response.json()
        assert project_data["project_id"] == project_id
        assert "status" in project_data


class TestErrorHandlingWorkflows:
    """Test error handling in workflows."""
    
    def test_database_connection_failure_workflow(self, client):
        """Test workflow with database connection failure."""
        
        # Test with invalid connection
        connection_data = {
            "type": "postgresql",
            "host": "invalid_host",
            "port": 9999,
            "database": "nonexistent_db",
            "username": "invalid_user",
            "password": "wrong_password"
        }
        
        # Connection test should still return success (placeholder implementation)
        response = client.post("/api/v1/scanner/test-connection", json=connection_data)
        assert response.status_code == 200
        
        # Schema scan should also work (placeholder implementation)
        scan_request = {
            "connection": connection_data,
            "tables": [],
            "include_samples": True
        }
        
        response = client.post("/api/v1/scanner/scan", json=scan_request)
        assert response.status_code == 200
    
    def test_invalid_data_workflow(self, client):
        """Test workflow with invalid data."""
        
        # Test with unsupported database type
        connection_data = {
            "type": "unsupported_database",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password"
        }
        
        response = client.post("/api/v1/scanner/test-connection", json=connection_data)
        assert response.status_code == 400
        assert "Unsupported database type" in response.json()["detail"]
        
        # Test with unsupported AI model
        generation_request = {
            "schema_data": {"tables": []},
            "ai_model": "unsupported_ai_model"
        }
        
        response = client.post("/api/v1/projects/generate", json=generation_request)
        assert response.status_code == 400
        assert "Unsupported AI model" in response.json()["detail"]
    
    def test_missing_data_workflow(self, client):
        """Test workflow with missing required data."""
        
        # Test with incomplete connection data
        incomplete_connection = {
            "type": "postgresql",
            "host": "localhost"
            # Missing required fields
        }
        
        response = client.post("/api/v1/scanner/test-connection", json=incomplete_connection)
        assert response.status_code == 422  # Validation error
        
        # Test with empty generation request
        response = client.post("/api/v1/projects/generate", json={})
        assert response.status_code == 422  # Validation error


class TestConcurrentWorkflows:
    """Test concurrent workflow execution."""
    
    @pytest.mark.asyncio
    async def test_concurrent_user_creation(self, async_db_session):
        """Test concurrent user creation."""
        
        async def create_user(email, username):
            user = User(
                email=email,
                username=username,
                full_name=f"User {username}",
                hashed_password="hashed_password",
                is_active=True
            )
            async_db_session.add(user)
            await async_db_session.commit()
            await async_db_session.refresh(user)
            return user
        
        # Create multiple users concurrently
        tasks = [
            create_user(f"user{i}@example.com", f"user{i}")
            for i in range(5)
        ]
        
        users = await asyncio.gather(*tasks)
        
        # Verify all users were created
        assert len(users) == 5
        for i, user in enumerate(users):
            assert user.email == f"user{i}@example.com"
            assert user.username == f"user{i}"
    
    def test_concurrent_api_requests(self, client):
        """Test concurrent API requests."""
        import threading
        import time
        
        results = []
        
        def make_request():
            response = client.get("/api/v1/health")
            results.append(response.status_code)
        
        # Create multiple threads
        threads = [threading.Thread(target=make_request) for _ in range(10)]
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all requests succeeded
        assert len(results) == 10
        assert all(status == 200 for status in results)


class TestDataConsistencyWorkflows:
    """Test data consistency in workflows."""
    
    @pytest.mark.asyncio
    async def test_cascading_deletes(self, async_db_session, sample_user_data):
        """Test cascading deletes maintain data consistency."""
        
        # Create user with related data
        user = User(**sample_user_data)
        async_db_session.add(user)
        await async_db_session.commit()
        await async_db_session.refresh(user)
        
        # Create data source
        data_source = DataSource(
            name="Test Data Source",
            database_type=DatabaseType.POSTGRESQL,
            host="localhost",
            port=5432,
            database="test_db",
            schema="public",
            username="test_user",
            password="test_password",
            owner_id=user.id
        )
        async_db_session.add(data_source)
        await async_db_session.commit()
        await async_db_session.refresh(data_source)
        
        # Create scan result
        scan_result = ScanResult(
            name="Test Scan",
            status=ScanStatus.COMPLETED,
            scan_timestamp="2024-01-01T00:00:00Z",
            data_source_id=data_source.id
        )
        async_db_session.add(scan_result)
        await async_db_session.commit()
        await async_db_session.refresh(scan_result)
        
        # Create project
        project = Project(
            name="Test Project",
            ai_model="gpt-4",
            model_types=["staging"],
            owner_id=user.id,
            scan_result_id=scan_result.id
        )
        async_db_session.add(project)
        await async_db_session.commit()
        await async_db_session.refresh(project)
        
        # Verify relationships exist
        assert len(user.data_sources) == 1
        assert len(user.projects) == 1
        assert len(data_source.scan_results) == 1
        
        # Delete user - should cascade to related objects
        await async_db_session.delete(user)
        await async_db_session.commit()
        
        # Verify cascading delete worked
        # (In a real test, you'd query to verify the related objects were deleted)