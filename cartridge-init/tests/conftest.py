"""Pytest configuration and fixtures."""

import asyncio
import os
import pytest
import pytest_asyncio
from typing import AsyncGenerator, Generator
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session
from fastapi.testclient import TestClient

# Set test environment
os.environ["CARTRIDGE_ENVIRONMENT"] = "test"
os.environ["CARTRIDGE_DB_URL"] = "postgresql://cartridge:cartridge@localhost:5432/cartridge_test"
os.environ["CARTRIDGE_REDIS_URL"] = "redis://localhost:6379/1"  # Different Redis DB for tests

from cartridge.core.config import settings
from cartridge.core.database import Base, get_db, get_async_db
from cartridge.api.main import app
from cartridge.models import User, Project, DataSource, ScanResult


# Test database URLs
TEST_DATABASE_URL = "postgresql://cartridge:cartridge@localhost:5432/cartridge_test"
TEST_ASYNC_DATABASE_URL = "postgresql+asyncpg://cartridge:cartridge@localhost:5432/cartridge_test"


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def engine():
    """Create test database engine."""
    engine = create_engine(TEST_DATABASE_URL, echo=False)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
async def async_engine():
    """Create async test database engine."""
    engine = create_async_engine(TEST_ASYNC_DATABASE_URL, echo=False)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def setup_database(engine):
    """Set up test database schema."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db_session(engine, setup_database) -> Generator[Session, None, None]:
    """Create a test database session."""
    connection = engine.connect()
    transaction = connection.begin()
    session = sessionmaker(bind=connection)(bind=connection)
    
    yield session
    
    session.close()
    transaction.rollback()
    connection.close()


@pytest_asyncio.fixture
async def async_db_session(async_engine, setup_database) -> AsyncGenerator[AsyncSession, None]:
    """Create an async test database session."""
    async with async_engine.begin() as connection:
        async_session = async_sessionmaker(
            bind=connection, class_=AsyncSession, expire_on_commit=False
        )
        async with async_session() as session:
            yield session
            await session.rollback()


@pytest.fixture
def client(db_session) -> Generator[TestClient, None, None]:
    """Create a test client with database session override."""
    
    def get_test_db():
        yield db_session
    
    app.dependency_overrides[get_db] = get_test_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


@pytest.fixture
async def async_client(async_db_session):
    """Create an async test client."""
    
    async def get_test_async_db():
        yield async_db_session
    
    app.dependency_overrides[get_async_db] = get_test_async_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


# Test data fixtures
@pytest.fixture
def sample_user_data():
    """Sample user data for testing."""
    return {
        "email": "test@example.com",
        "username": "testuser",
        "full_name": "Test User",
        "hashed_password": "hashed_password_123",
        "is_active": True,
        "is_superuser": False,
    }


@pytest.fixture
def sample_data_source_data():
    """Sample data source data for testing."""
    return {
        "name": "Test PostgreSQL",
        "description": "Test database connection",
        "database_type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "schema": "public",
        "username": "test_user",
        "password": "test_password",
        "connection_params": {},
        "is_active": True,
    }


@pytest.fixture
def sample_project_data():
    """Sample project data for testing."""
    return {
        "name": "Test Project",
        "description": "Test dbt project generation",
        "ai_model": "gpt-4",
        "model_types": ["staging", "intermediate", "marts"],
        "include_tests": True,
        "include_docs": True,
    }


@pytest.fixture
def sample_scan_result_data():
    """Sample scan result data for testing."""
    return {
        "name": "Test Scan",
        "status": "completed",
        "tables_scanned": ["customers", "orders"],
        "include_samples": True,
        "sample_size": 100,
        "total_tables": 2,
        "total_columns": 10,
        "scan_duration": 5.5,
        "scan_timestamp": "2024-01-01T00:00:00Z",
    }


@pytest.fixture
def user(db_session, sample_user_data) -> User:
    """Create a test user."""
    user = User(**sample_user_data)
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def data_source(db_session, user, sample_data_source_data) -> DataSource:
    """Create a test data source."""
    data_source = DataSource(**sample_data_source_data, owner_id=user.id)
    db_session.add(data_source)
    db_session.commit()
    db_session.refresh(data_source)
    return data_source


@pytest.fixture
def scan_result(db_session, data_source, sample_scan_result_data) -> ScanResult:
    """Create a test scan result."""
    scan_result = ScanResult(**sample_scan_result_data, data_source_id=data_source.id)
    db_session.add(scan_result)
    db_session.commit()
    db_session.refresh(scan_result)
    return scan_result


@pytest.fixture
def project(db_session, user, scan_result, sample_project_data) -> Project:
    """Create a test project."""
    project = Project(
        **sample_project_data, 
        owner_id=user.id, 
        scan_result_id=scan_result.id
    )
    db_session.add(project)
    db_session.commit()
    db_session.refresh(project)
    return project


# Mock fixtures for external services
@pytest.fixture
def mock_openai_response():
    """Mock OpenAI API response."""
    return {
        "choices": [
            {
                "message": {
                    "content": "SELECT * FROM {{ source('raw', 'customers') }}"
                }
            }
        ],
        "usage": {
            "total_tokens": 100
        }
    }


@pytest.fixture
def mock_database_connection():
    """Mock database connection for testing scanner."""
    class MockConnection:
        def execute(self, query):
            return [{"table_name": "customers", "column_name": "id", "data_type": "integer"}]
        
        def close(self):
            pass
    
    return MockConnection()


# Celery test configuration
@pytest.fixture
def celery_config():
    """Celery configuration for tests."""
    return {
        'broker_url': 'memory://',
        'result_backend': 'cache+memory://',
        'task_always_eager': True,
        'task_eager_propagates': True,
    }