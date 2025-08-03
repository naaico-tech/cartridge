"""Unit tests for scanner API endpoints."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from cartridge.scanner.base import ScanResult as ScannerScanResult, DatabaseInfo, TableInfo, ColumnInfo, DataType


class TestScannerAPI:
    """Test scanner API endpoints."""

    def test_test_connection_success(self, client):
        """Test successful database connection test."""
        connection_data = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.test_connection.return_value = {
                "status": "success",
                "message": "Connection successful",
                "database_version": "PostgreSQL 13.0",
                "database_type": "postgresql"
            }
            mock_factory.return_value = mock_connector

            response = client.post("/api/v1/scanner/test-connection", json=connection_data)

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"
            assert data["message"] == "Connection test successful"
            assert "connection_info" in data
            assert "database_info" in data
            assert data["database_info"]["version"] == "PostgreSQL 13.0"

    def test_test_connection_failure(self, client):
        """Test failed database connection test."""
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

    def test_test_connection_unsupported_database(self, client):
        """Test connection test with unsupported database type."""
        connection_data = {
            "type": "unsupported_db",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        with patch('cartridge.api.routes.scanner.ConnectorFactory.get_supported_databases') as mock_supported:
            mock_supported.return_value = ["postgresql", "mysql"]

            response = client.post("/api/v1/scanner/test-connection", json=connection_data)

            assert response.status_code == 400
            assert "Unsupported database type" in response.json()["detail"]

    def test_scan_schema_sync_success(self, client):
        """Test successful synchronous schema scan."""
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
            "sample_size": 100,
            "async_mode": False
        }

        # Create mock scan result
        mock_db_info = DatabaseInfo(
            database_type="postgresql",
            version="13.0",
            host="localhost",
            port=5432,
            database_name="test_db",
            schema_name="public",
            total_tables=1,
            total_views=0
        )

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

        mock_scan_result = ScannerScanResult(
            database_info=mock_db_info,
            tables=[mock_table],
            scan_duration_seconds=2.5,
            scan_timestamp="2024-01-01T00:00:00Z"
        )

        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema.return_value = mock_scan_result
            mock_factory.return_value = mock_connector

            response = client.post("/api/v1/scanner/scan", json=scan_request)

            assert response.status_code == 200
            data = response.json()
            assert "connection_info" in data
            assert "tables" in data
            assert len(data["tables"]) == 1
            assert data["tables"][0]["name"] == "customers"
            assert data["tables"][0]["row_count"] == 1000
            assert len(data["tables"][0]["columns"]) == 1
            assert data["tables"][0]["columns"][0]["name"] == "id"
            assert data["scan_duration_seconds"] == 2.5

    def test_scan_schema_async_mode(self, client):
        """Test asynchronous schema scan mode."""
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
            "sample_size": 100,
            "async_mode": True
        }

        with patch('cartridge.api.routes.scanner.scan_database_schema') as mock_task:
            mock_task.delay.return_value.id = "test-task-id-123"

            response = client.post("/api/v1/scanner/scan", json=scan_request)

            assert response.status_code == 200
            data = response.json()
            assert data["task_id"] == "test-task-id-123"
            assert data["status"] == "PENDING"
            assert data["message"] == "Schema scan queued for background processing"
            assert "scan_result_id" in data["result"]

    def test_get_task_status_pending(self, client):
        """Test getting status of a pending task."""
        task_id = "test-task-id-123"

        with patch('cartridge.api.routes.scanner.celery_app.AsyncResult') as mock_result:
            mock_task_result = MagicMock()
            mock_task_result.state = "PENDING"
            mock_result.return_value = mock_task_result

            response = client.get(f"/api/v1/scanner/tasks/{task_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["task_id"] == task_id
            assert data["status"] == "PENDING"
            assert data["message"] == "Task is waiting to be processed"

    def test_get_task_status_progress(self, client):
        """Test getting status of a task in progress."""
        task_id = "test-task-id-123"

        with patch('cartridge.api.routes.scanner.celery_app.AsyncResult') as mock_result:
            mock_task_result = MagicMock()
            mock_task_result.state = "PROGRESS"
            mock_task_result.info = {
                "current": 50,
                "total": 100,
                "status": "Scanning tables..."
            }
            mock_result.return_value = mock_task_result

            response = client.get(f"/api/v1/scanner/tasks/{task_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["task_id"] == task_id
            assert data["status"] == "PROGRESS"
            assert data["message"] == "Task is being processed"
            assert data["progress"]["current"] == 50
            assert data["progress"]["total"] == 100

    def test_get_task_status_success(self, client):
        """Test getting status of a completed task."""
        task_id = "test-task-id-123"

        with patch('cartridge.api.routes.scanner.celery_app.AsyncResult') as mock_result:
            mock_task_result = MagicMock()
            mock_task_result.state = "SUCCESS"
            mock_task_result.result = {
                "scan_result_id": "scan-123",
                "status": "completed",
                "tables": [{"name": "customers", "row_count": 1000}]
            }
            mock_result.return_value = mock_task_result

            response = client.get(f"/api/v1/scanner/tasks/{task_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["task_id"] == task_id
            assert data["status"] == "SUCCESS"
            assert data["message"] == "Task completed successfully"
            assert data["result"]["status"] == "completed"

    def test_get_task_status_failure(self, client):
        """Test getting status of a failed task."""
        task_id = "test-task-id-123"

        with patch('cartridge.api.routes.scanner.celery_app.AsyncResult') as mock_result:
            mock_task_result = MagicMock()
            mock_task_result.state = "FAILURE"
            mock_task_result.info = Exception("Connection timeout")
            mock_result.return_value = mock_task_result

            response = client.get(f"/api/v1/scanner/tasks/{task_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["task_id"] == task_id
            assert data["status"] == "FAILURE"
            assert "Connection timeout" in data["message"]

    def test_scan_schema_invalid_request(self, client):
        """Test schema scan with invalid request data."""
        invalid_request = {
            "connection": {
                "type": "postgresql",
                # Missing required fields
            },
            "async_mode": False
        }

        response = client.post("/api/v1/scanner/scan", json=invalid_request)
        assert response.status_code == 422  # Validation error

    def test_scan_schema_connector_exception(self, client):
        """Test schema scan when connector raises exception."""
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
            "async_mode": False
        }

        with patch('cartridge.api.routes.scanner.ConnectorFactory.create_connector') as mock_factory:
            mock_connector = AsyncMock()
            mock_connector.scan_schema.side_effect = Exception("Database connection failed")
            mock_factory.return_value = mock_connector

            response = client.post("/api/v1/scanner/scan", json=scan_request)

            assert response.status_code == 500
            assert "Schema scan failed" in response.json()["detail"]


class TestScannerAPIValidation:
    """Test scanner API request validation."""

    def test_connection_data_validation(self, client):
        """Test connection data validation."""
        # Test missing required fields
        invalid_data = {
            "type": "postgresql",
            # Missing host, port, database, username, password
        }

        response = client.post("/api/v1/scanner/test-connection", json=invalid_data)
        assert response.status_code == 422

    def test_scan_request_validation(self, client):
        """Test scan request validation."""
        # Test invalid sample_size
        invalid_request = {
            "connection": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_password",
                "schema": "public"
            },
            "sample_size": -1  # Invalid negative size
        }

        response = client.post("/api/v1/scanner/scan", json=invalid_request)
        assert response.status_code == 422

    def test_invalid_database_type(self, client):
        """Test validation of database type."""
        connection_data = {
            "type": "",  # Empty type
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "schema": "public"
        }

        response = client.post("/api/v1/scanner/test-connection", json=connection_data)
        assert response.status_code == 422


@pytest.mark.asyncio
class TestScannerAPIAsync:
    """Test scanner API with async operations."""

    async def test_async_scan_operation(self, async_client):
        """Test async scan operations."""
        # This would test the actual async behavior
        # For now, we'll test the sync version with async client
        pass