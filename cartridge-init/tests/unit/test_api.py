"""Tests for API endpoints."""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from cartridge.api.main import app


class TestHealthEndpoints:
    """Test health check endpoints."""
    
    def test_health_check_success(self, client):
        """Test successful health check."""
        response = client.get("/api/v1/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert "version" in data
        assert "environment" in data
        assert "services" in data
        assert data["services"]["database"] == "healthy"
    
    @patch("cartridge.api.routes.health.Session.execute")
    def test_health_check_db_failure(self, mock_execute, client):
        """Test health check with database failure."""
        mock_execute.side_effect = Exception("Database connection failed")
        
        response = client.get("/api/v1/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["services"]["database"] == "unhealthy"
    
    def test_detailed_health_check(self, client):
        """Test detailed health check."""
        response = client.get("/api/v1/health/detailed")
        
        assert response.status_code == 200
        data = response.json()
        assert "components" in data
        assert "database" in data["components"]
        assert "redis" in data["components"]
        assert "ai_services" in data["components"]


class TestScannerEndpoints:
    """Test schema scanner endpoints."""
    
    def test_test_connection_success(self, client):
        """Test successful connection test."""
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
        data = response.json()
        assert data["status"] == "success"
        assert "connection_info" in data
        assert data["connection_info"]["type"] == "postgresql"
    
    def test_test_connection_unsupported_type(self, client):
        """Test connection test with unsupported database type."""
        connection_data = {
            "type": "unsupported_db",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password"
        }
        
        response = client.post("/api/v1/scanner/test-connection", json=connection_data)
        
        assert response.status_code == 400
        assert "Unsupported database type" in response.json()["detail"]
    
    def test_scan_schema_success(self, client):
        """Test successful schema scan."""
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
            "tables": [],
            "include_samples": True,
            "sample_size": 100
        }
        
        response = client.post("/api/v1/scanner/scan", json=scan_request)
        
        assert response.status_code == 200
        data = response.json()
        assert "connection_info" in data
        assert "tables" in data
        assert "scan_timestamp" in data
        assert "scan_duration_seconds" in data
        assert len(data["tables"]) > 0
    
    def test_scan_schema_unsupported_type(self, client):
        """Test schema scan with unsupported database type."""
        scan_request = {
            "connection": {
                "type": "unsupported_db",
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_password"
            }
        }
        
        response = client.post("/api/v1/scanner/scan", json=scan_request)
        
        assert response.status_code == 400
        assert "Unsupported database type" in response.json()["detail"]
    
    def test_scan_schema_missing_fields(self, client):
        """Test schema scan with missing required fields."""
        scan_request = {
            "connection": {
                "type": "postgresql",
                "host": "localhost"
                # Missing required fields
            }
        }
        
        response = client.post("/api/v1/scanner/scan", json=scan_request)
        
        assert response.status_code == 422  # Validation error


class TestProjectEndpoints:
    """Test project management endpoints."""
    
    def test_generate_models_success(self, client):
        """Test successful model generation."""
        generation_request = {
            "schema_data": {
                "tables": [
                    {
                        "name": "customers",
                        "columns": [
                            {"name": "id", "type": "integer"},
                            {"name": "email", "type": "varchar"}
                        ]
                    }
                ]
            },
            "model_types": ["staging", "marts"],
            "ai_model": "gpt-4",
            "include_tests": True,
            "include_docs": True
        }
        
        response = client.post("/api/v1/projects/generate", json=generation_request)
        
        assert response.status_code == 200
        data = response.json()
        assert "project_id" in data
        assert "models" in data
        assert len(data["models"]) > 0
        assert data["ai_model_used"] == "gpt-4"
        
        # Check model structure
        model = data["models"][0]
        assert "name" in model
        assert "type" in model
        assert "sql" in model
        assert "description" in model
    
    def test_generate_models_unsupported_ai_model(self, client):
        """Test model generation with unsupported AI model."""
        generation_request = {
            "schema_data": {"tables": []},
            "ai_model": "unsupported_model"
        }
        
        response = client.post("/api/v1/projects/generate", json=generation_request)
        
        assert response.status_code == 400
        assert "Unsupported AI model" in response.json()["detail"]
    
    def test_test_run_models_success(self, client):
        """Test successful model test run."""
        test_request = {
            "project_id": "test_project_123",
            "models_to_test": [],
            "dry_run": True
        }
        
        response = client.post("/api/v1/projects/test-run", json=test_request)
        
        assert response.status_code == 200
        data = response.json()
        assert data["project_id"] == "test_project_123"
        assert data["status"] == "success"
        assert "results" in data
        assert "execution_time_seconds" in data
        assert len(data["results"]) > 0
    
    def test_get_project_success(self, client):
        """Test getting project information."""
        project_id = "test_project_123"
        
        response = client.get(f"/api/v1/projects/{project_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["project_id"] == project_id
        assert "status" in data
        assert "created_at" in data
        assert "model_count" in data
        assert "generation_settings" in data
    
    def test_download_project_not_implemented(self, client):
        """Test project download (not yet implemented)."""
        project_id = "test_project_123"
        
        response = client.get(f"/api/v1/projects/{project_id}/download")
        
        assert response.status_code == 501
        assert "not yet implemented" in response.json()["detail"]


class TestRootEndpoint:
    """Test root endpoint."""
    
    def test_root_endpoint(self, client):
        """Test root endpoint response."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "version" in data
        assert "docs" in data
        assert "health" in data
        assert data["docs"] == "/docs"
        assert data["health"] == "/api/v1/health"


class TestCORSMiddleware:
    """Test CORS middleware."""
    
    def test_cors_headers_development(self):
        """Test CORS headers in development mode."""
        with patch("cartridge.core.config.settings.is_development", return_value=True):
            client = TestClient(app)
            response = client.options("/api/v1/health", 
                                    headers={"Origin": "http://localhost:3000"})
            
            # Should allow all origins in development
            assert response.status_code == 200


class TestErrorHandling:
    """Test error handling."""
    
    @patch("cartridge.api.routes.health.get_db")
    def test_database_error_handling(self, mock_get_db, client):
        """Test database error handling."""
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Database error")
        mock_get_db.return_value = mock_session
        
        response = client.get("/api/v1/health")
        
        # Should still return 200 but with unhealthy status
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "unhealthy"
    
    def test_validation_error_handling(self, client):
        """Test validation error handling."""
        # Send invalid JSON data
        response = client.post("/api/v1/scanner/test-connection", json={})
        
        assert response.status_code == 422
        assert "detail" in response.json()
    
    def test_method_not_allowed(self, client):
        """Test method not allowed error."""
        response = client.put("/api/v1/health")  # PUT not allowed
        
        assert response.status_code == 405