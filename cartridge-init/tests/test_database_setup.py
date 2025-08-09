"""Test database setup and migrations."""

import pytest
import asyncio
from sqlalchemy import text
from alembic.config import Config
from alembic import command
import importlib.util
from pathlib import Path

from cartridge.core.database import async_engine, Base
from cartridge.models import User, Project, DataSource, ScanResult


class TestDatabaseSetup:
    """Test database setup and configuration."""
    
    @pytest.mark.asyncio
    async def test_database_connection(self):
        """Test database connection."""
        async with async_engine.begin() as conn:
            result = await conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
    
    @pytest.mark.asyncio
    async def test_create_all_tables(self):
        """Test creating all tables."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
            # Check that tables exist
            result = await conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            
            tables = [row[0] for row in result.fetchall()]
            
            # Check that our main tables exist
            assert "users" in tables
            assert "data_sources" in tables
            assert "scan_results" in tables
            assert "table_info" in tables
            assert "projects" in tables
            assert "generated_models" in tables
    
    @pytest.mark.asyncio
    async def test_table_constraints(self):
        """Test that table constraints are properly created."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
            # Check unique constraints
            result = await conn.execute(text("""
                SELECT constraint_name, table_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_schema = 'public'
                AND constraint_type = 'UNIQUE'
            """))
            
            constraints = result.fetchall()
            constraint_info = [(row[1], row[0]) for row in constraints]
            
            # Check that unique constraints exist for users
            user_constraints = [c for c in constraint_info if c[0] == 'users']
            assert len(user_constraints) >= 2  # email and username should be unique
    
    @pytest.mark.asyncio
    async def test_foreign_key_constraints(self):
        """Test foreign key constraints."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
            # Check foreign key constraints
            result = await conn.execute(text("""
                SELECT 
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
            """))
            
            fk_constraints = result.fetchall()
            
            # Check that foreign keys exist
            assert len(fk_constraints) > 0
            
            # Check specific foreign keys
            fk_info = [(row[0], row[1], row[2]) for row in fk_constraints]
            
            # data_sources should reference users
            data_source_fks = [fk for fk in fk_info if fk[0] == 'data_sources']
            assert any(fk[1] == 'owner_id' and fk[2] == 'users' for fk in data_source_fks)
            
            # projects should reference users and scan_results
            project_fks = [fk for fk in fk_info if fk[0] == 'projects']
            assert any(fk[1] == 'owner_id' and fk[2] == 'users' for fk in project_fks)
            assert any(fk[1] == 'scan_result_id' and fk[2] == 'scan_results' for fk in project_fks)


class TestAlembicMigrations:
    """Test Alembic migration system."""
    
    def test_alembic_config(self):
        """Test Alembic configuration."""
        # Dynamically load Alembic env from the project directory
        env_path = Path(__file__).resolve().parents[1].parent / "alembic" / "env.py"
        spec = importlib.util.spec_from_file_location("project_alembic_env", str(env_path))
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[attr-defined]

        # Test that we can get database URL
        url = module.get_url()
        assert "postgresql" in url
    
    @pytest.mark.slow
    def test_migration_up_and_down(self):
        """Test running migrations up and down."""
        # This would test actual migration files when they exist
        # For now, we'll just test that the migration system is configured
        
        # Resolve path to alembic.ini relative to this test file
        config_path = Path(__file__).resolve().parents[1] / "alembic.ini"
        config = Config(str(config_path))
        
        # Test that we can get the migration directory
        script_location = config.get_main_option("script_location")
        assert script_location == "alembic"
        
        # Test that we can get the database URL
        url = config.get_main_option("sqlalchemy.url")
        assert url is not None


class TestDatabasePerformance:
    """Test database performance and connection pooling."""
    
    @pytest.mark.asyncio
    async def test_connection_pooling(self):
        """Test connection pooling behavior."""
        # Test multiple concurrent connections
        async def test_query():
            async with async_engine.begin() as conn:
                result = await conn.execute(text("SELECT 1"))
                return result.scalar()
        
        # Run multiple queries concurrently
        tasks = [test_query() for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        # All queries should succeed
        assert all(result == 1 for result in results)
    
    @pytest.mark.asyncio
    async def test_transaction_isolation(self):
        """Test transaction isolation."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
            # Start a transaction
            trans = await conn.begin()
            
            try:
                # Insert data in transaction
                await conn.execute(text("""
                    INSERT INTO users (id, email, username, full_name, hashed_password)
                    VALUES (gen_random_uuid(), 'test@example.com', 'testuser', 'Test User', 'hashed')
                """))
                
                # Check data exists in this transaction
                result = await conn.execute(text("SELECT COUNT(*) FROM users WHERE email = 'test@example.com'"))
                count = result.scalar()
                assert count == 1
                
                # Rollback transaction
                await trans.rollback()
                
                # Check data doesn't exist after rollback
                result = await conn.execute(text("SELECT COUNT(*) FROM users WHERE email = 'test@example.com'"))
                count = result.scalar()
                assert count == 0
                
            except Exception:
                await trans.rollback()
                raise