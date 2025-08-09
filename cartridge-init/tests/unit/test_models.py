"""Tests for database models."""

import pytest
from datetime import datetime
from sqlalchemy.exc import IntegrityError

from cartridge.models import User, Project, DataSource, ScanResult, TableInfo, GeneratedModel
from cartridge.models.project import ProjectStatus, ModelType
from cartridge.models.scan import DatabaseType, ScanStatus


class TestUser:
    """Test User model."""
    
    def test_create_user(self, db_session, sample_user_data):
        """Test creating a new user."""
        user = User(**sample_user_data)
        db_session.add(user)
        db_session.commit()
        
        assert user.id is not None
        assert user.email == sample_user_data["email"]
        assert user.username == sample_user_data["username"]
        assert user.is_active is True
        assert user.created_at is not None
        assert user.updated_at is not None
    
    def test_user_unique_email(self, db_session, sample_user_data):
        """Test that user email must be unique."""
        user1 = User(**sample_user_data)
        db_session.add(user1)
        db_session.commit()
        
        # Try to create another user with same email
        user2_data = sample_user_data.copy()
        user2_data["username"] = "different_username"
        user2 = User(**user2_data)
        db_session.add(user2)
        
        with pytest.raises(IntegrityError):
            db_session.commit()
    
    def test_user_unique_username(self, db_session, sample_user_data):
        """Test that username must be unique."""
        user1 = User(**sample_user_data)
        db_session.add(user1)
        db_session.commit()
        
        # Try to create another user with same username
        user2_data = sample_user_data.copy()
        user2_data["email"] = "different@example.com"
        user2 = User(**user2_data)
        db_session.add(user2)
        
        with pytest.raises(IntegrityError):
            db_session.commit()
    
    def test_user_to_dict(self, db_session, sample_user_data):
        """Test user to_dict method."""
        user = User(**sample_user_data)
        db_session.add(user)
        db_session.commit()
        
        user_dict = user.to_dict()
        assert user_dict["email"] == sample_user_data["email"]
        assert user_dict["username"] == sample_user_data["username"]
        assert "id" in user_dict
        assert "created_at" in user_dict
    
    def test_user_repr(self, db_session, sample_user_data):
        """Test user string representation."""
        user = User(**sample_user_data)
        db_session.add(user)
        db_session.commit()
        
        repr_str = repr(user)
        assert "test@example.com" in repr_str
        assert "testuser" in repr_str


class TestDataSource:
    """Test DataSource model."""
    
    def test_create_data_source(self, db_session, user, sample_data_source_data):
        """Test creating a new data source."""
        data_source = DataSource(**sample_data_source_data, owner_id=user.id)
        db_session.add(data_source)
        db_session.commit()
        
        assert data_source.id is not None
        assert data_source.name == sample_data_source_data["name"]
        assert data_source.database_type == DatabaseType.POSTGRESQL
        assert data_source.owner_id == user.id
        assert data_source.is_active is True
    
    def test_data_source_owner_relationship(self, db_session, user, sample_data_source_data):
        """Test data source owner relationship."""
        data_source = DataSource(**sample_data_source_data, owner_id=user.id)
        db_session.add(data_source)
        db_session.commit()
        
        # Test relationship
        assert data_source.owner.email == user.email
        assert data_source in user.data_sources
    
    def test_data_source_database_types(self, db_session, user, sample_data_source_data):
        """Test different database types."""
        for db_type in DatabaseType:
            data_source_data = sample_data_source_data.copy()
            data_source_data["name"] = f"Test {db_type.value}"
            data_source_data["database_type"] = db_type
            
            data_source = DataSource(**data_source_data, owner_id=user.id)
            db_session.add(data_source)
            db_session.commit()
            
            assert data_source.database_type == db_type


class TestScanResult:
    """Test ScanResult model."""
    
    def test_create_scan_result(self, db_session, data_source, sample_scan_result_data):
        """Test creating a new scan result."""
        scan_result = ScanResult(**sample_scan_result_data, data_source_id=data_source.id)
        db_session.add(scan_result)
        db_session.commit()
        
        assert scan_result.id is not None
        assert scan_result.name == sample_scan_result_data["name"]
        assert scan_result.status == ScanStatus.COMPLETED
        assert scan_result.data_source_id == data_source.id
        assert scan_result.total_tables == 2
    
    def test_scan_result_data_source_relationship(self, db_session, data_source, sample_scan_result_data):
        """Test scan result data source relationship."""
        scan_result = ScanResult(**sample_scan_result_data, data_source_id=data_source.id)
        db_session.add(scan_result)
        db_session.commit()
        
        # Test relationship
        assert scan_result.data_source.name == data_source.name
        assert scan_result in data_source.scan_results
    
    def test_scan_status_enum(self, db_session, data_source, sample_scan_result_data):
        """Test scan status enumeration."""
        for status in ScanStatus:
            scan_data = sample_scan_result_data.copy()
            scan_data["name"] = f"Test {status.value}"
            scan_data["status"] = status
            
            scan_result = ScanResult(**scan_data, data_source_id=data_source.id)
            db_session.add(scan_result)
            db_session.commit()
            
            assert scan_result.status == status


class TestTableInfo:
    """Test TableInfo model."""
    
    def test_create_table_info(self, db_session, scan_result):
        """Test creating table info."""
        table_info = TableInfo(
            name="customers",
            schema_name="public",
            table_type="table",
            row_count=1000,
            columns=[
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "email", "type": "varchar", "nullable": False}
            ],
            constraints=[
                {"type": "primary_key", "columns": ["id"]}
            ],
            sample_data=[
                {"id": 1, "email": "user1@example.com"},
                {"id": 2, "email": "user2@example.com"}
            ],
            scan_result_id=scan_result.id
        )
        db_session.add(table_info)
        db_session.commit()
        
        assert table_info.id is not None
        assert table_info.name == "customers"
        assert table_info.row_count == 1000
        assert len(table_info.columns) == 2
        assert len(table_info.sample_data) == 2
    
    def test_table_info_scan_result_relationship(self, db_session, scan_result):
        """Test table info scan result relationship."""
        table_info = TableInfo(
            name="orders",
            schema_name="public",
            columns=[],
            scan_result_id=scan_result.id
        )
        db_session.add(table_info)
        db_session.commit()
        
        # Test relationship
        assert table_info.scan_result.name == scan_result.name
        assert table_info in scan_result.tables


class TestProject:
    """Test Project model."""
    
    def test_create_project(self, db_session, user, scan_result, sample_project_data):
        """Test creating a new project."""
        project = Project(
            **sample_project_data,
            owner_id=user.id,
            scan_result_id=scan_result.id
        )
        db_session.add(project)
        db_session.commit()
        
        assert project.id is not None
        assert project.name == sample_project_data["name"]
        assert project.status == ProjectStatus.PENDING
        assert project.ai_model == "gpt-4"
        assert project.model_types == ["staging", "intermediate", "marts"]
        assert project.include_tests is True
    
    def test_project_relationships(self, db_session, user, scan_result, sample_project_data):
        """Test project relationships."""
        project = Project(
            **sample_project_data,
            owner_id=user.id,
            scan_result_id=scan_result.id
        )
        db_session.add(project)
        db_session.commit()
        
        # Test relationships
        assert project.owner.email == user.email
        assert project.scan_result.name == scan_result.name
        assert project in user.projects
    
    def test_project_status_enum(self, db_session, user, scan_result, sample_project_data):
        """Test project status enumeration."""
        for status in ProjectStatus:
            project_data = sample_project_data.copy()
            project_data["name"] = f"Test {status.value}"
            
            project = Project(
                **project_data,
                status=status,
                owner_id=user.id,
                scan_result_id=scan_result.id
            )
            db_session.add(project)
            db_session.commit()
            
            assert project.status == status


class TestGeneratedModel:
    """Test GeneratedModel model."""
    
    def test_create_generated_model(self, db_session, project):
        """Test creating a generated model."""
        model = GeneratedModel(
            name="stg_customers",
            model_type=ModelType.STAGING,
            description="Staging table for customers",
            sql="SELECT * FROM {{ source('raw', 'customers') }}",
            tests=[
                {"test": "unique", "column": "customer_id"},
                {"test": "not_null", "column": "customer_id"}
            ],
            dependencies=[],
            project_id=project.id
        )
        db_session.add(model)
        db_session.commit()
        
        assert model.id is not None
        assert model.name == "stg_customers"
        assert model.model_type == ModelType.STAGING
        assert "SELECT" in model.sql
        assert len(model.tests) == 2
    
    def test_generated_model_project_relationship(self, db_session, project):
        """Test generated model project relationship."""
        model = GeneratedModel(
            name="dim_customers",
            model_type=ModelType.MARTS,
            sql="SELECT * FROM {{ ref('stg_customers') }}",
            project_id=project.id
        )
        db_session.add(model)
        db_session.commit()
        
        # Test relationship
        assert model.project.name == project.name
        assert model in project.models
    
    def test_model_type_enum(self, db_session, project):
        """Test model type enumeration."""
        for model_type in ModelType:
            model = GeneratedModel(
                name=f"test_{model_type.value}",
                model_type=model_type,
                sql="SELECT 1",
                project_id=project.id
            )
            db_session.add(model)
            db_session.commit()
            
            assert model.model_type == model_type