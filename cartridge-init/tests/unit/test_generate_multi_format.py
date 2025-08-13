"""
Tests for the generate command with different scan result formats.
"""

import json
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock
from click.testing import CliRunner

from cartridge.cli import main
from cartridge.ai.base import ModelGenerationResult, GeneratedModel, ModelType


class TestGenerateMultiFormat:
    """Test the generate command with different scan result formats."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def create_test_scan_file(self, scan_data, suffix='.json'):
        """Create a temporary scan file with the given data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix=suffix, delete=False) as f:
            json.dump(scan_data, f)
            return f.name
    
    def create_mock_generation_result(self, table_count=2):
        """Create a mock generation result."""
        models = []
        for i in range(table_count):
            models.append(GeneratedModel(
                name=f"test_model_{i}",
                model_type=ModelType.STAGING,
                sql=f"SELECT * FROM table_{i}",
                description=f"Test model {i}",
                columns=[],
                dependencies=[],
                tests=[]
            ))
        
        return ModelGenerationResult(
            models=models,
            project_structure={"models": len(models)},
            generation_metadata={"ai_model": "mock", "table_count": table_count}
        )
    
    @patch('cartridge.cli.DBTProjectGenerator')
    @patch('cartridge.cli.AIProviderFactory')
    def test_generate_single_schema_format(self, mock_ai_factory, mock_dbt_generator):
        """Test generate command with single schema scan format."""
        # Create single schema scan data
        scan_data = {
            "database_type": "postgresql",
            "schema": "public",
            "tables": [
                {
                    "name": "users",
                    "schema": "public",
                    "columns": [
                        {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True},
                        {"name": "name", "data_type": "varchar", "is_nullable": False, "is_primary_key": False}
                    ]
                },
                {
                    "name": "orders",
                    "schema": "public", 
                    "columns": [
                        {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True},
                        {"name": "user_id", "data_type": "integer", "is_nullable": True, "is_primary_key": False}
                    ]
                }
            ]
        }
        
        scan_file = self.create_test_scan_file(scan_data)
        
        try:
            # Mock AI provider
            mock_ai_provider = AsyncMock()
            mock_ai_provider.generate_models.return_value = self.create_mock_generation_result(2)
            mock_ai_factory.create_provider.return_value = mock_ai_provider
            
            # Mock DBT generator
            mock_dbt_generator.return_value.generate_project.return_value = {"files_generated": {"models": 2}}
            
            # Run generate command
            result = self.runner.invoke(main, [
                'generate', scan_file,
                '--ai-model', 'mock',
                '--project-name', 'test_single'
            ], env={'OPENAI_API_KEY': 'test'})
            
            assert result.exit_code == 0
            assert "Detected single schema scan format" in result.output
            assert "Found 2 tables across all schemas/databases" in result.output
            assert "Processing 2 tables" in result.output
            
        finally:
            Path(scan_file).unlink()
    
    @patch('cartridge.cli.DBTProjectGenerator')
    @patch('cartridge.cli.AIProviderFactory')
    def test_generate_multi_schema_format(self, mock_ai_factory, mock_dbt_generator):
        """Test generate command with multi-schema scan format."""
        # Create multi-schema scan data
        scan_data = {
            "database_type": "postgresql",
            "schemas": ["public", "analytics"],
            "total_schemas": 2,
            "total_tables": 3,
            "schemas_data": [
                {
                    "schema": "public",
                    "tables": [
                        {
                            "name": "users",
                            "schema": "public",
                            "columns": [
                                {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True}
                            ]
                        }
                    ]
                },
                {
                    "schema": "analytics",
                    "tables": [
                        {
                            "name": "user_metrics",
                            "schema": "analytics",
                            "columns": [
                                {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True},
                                {"name": "metric_value", "data_type": "decimal", "is_nullable": True, "is_primary_key": False}
                            ]
                        },
                        {
                            "name": "events",
                            "schema": "analytics", 
                            "columns": [
                                {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True}
                            ]
                        }
                    ]
                }
            ]
        }
        
        scan_file = self.create_test_scan_file(scan_data)
        
        try:
            # Mock AI provider
            mock_ai_provider = AsyncMock()
            mock_ai_provider.generate_models.return_value = self.create_mock_generation_result(3)
            mock_ai_factory.create_provider.return_value = mock_ai_provider
            
            # Mock DBT generator
            mock_dbt_generator.return_value.generate_project.return_value = {"files_generated": {"models": 3}}
            
            # Run generate command
            result = self.runner.invoke(main, [
                'generate', scan_file,
                '--ai-model', 'mock',
                '--project-name', 'test_multi'
            ], env={'OPENAI_API_KEY': 'test'})
            
            assert result.exit_code == 0
            assert "Detected multi-schema scan format" in result.output
            assert "Schema 'public': 1 tables" in result.output
            assert "Schema 'analytics': 2 tables" in result.output
            assert "Found 3 tables across all schemas/databases" in result.output
            assert "Processing 3 tables" in result.output
            
        finally:
            Path(scan_file).unlink()
    
    @patch('cartridge.cli.DBTProjectGenerator')
    @patch('cartridge.cli.AIProviderFactory')
    def test_generate_multi_database_format(self, mock_ai_factory, mock_dbt_generator):
        """Test generate command with multi-database scan format."""
        # Create multi-database scan data
        scan_data = {
            "scan_type": "multi_database",
            "total_databases": 2,
            "total_schemas": 2,
            "total_tables": 3,
            "databases": [
                {
                    "name": "main_db",
                    "database_type": "postgresql",
                    "schemas_data": [
                        {
                            "schema": "public",
                            "tables": [
                                {
                                    "name": "users",
                                    "schema": "public",
                                    "columns": [
                                        {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True}
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    "name": "analytics_db",
                    "database_type": "postgresql",
                    "schemas_data": [
                        {
                            "schema": "analytics",
                            "tables": [
                                {
                                    "name": "events",
                                    "schema": "analytics",
                                    "columns": [
                                        {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True}
                                    ]
                                },
                                {
                                    "name": "metrics",
                                    "schema": "analytics",
                                    "columns": [
                                        {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        
        scan_file = self.create_test_scan_file(scan_data)
        
        try:
            # Mock AI provider
            mock_ai_provider = AsyncMock()
            mock_ai_provider.generate_models.return_value = self.create_mock_generation_result(3)
            mock_ai_factory.create_provider.return_value = mock_ai_provider
            
            # Mock DBT generator
            mock_dbt_generator.return_value.generate_project.return_value = {"files_generated": {"models": 3}}
            
            # Run generate command
            result = self.runner.invoke(main, [
                'generate', scan_file,
                '--ai-model', 'mock',
                '--project-name', 'test_multi_db'
            ], env={'OPENAI_API_KEY': 'test'})
            
            assert result.exit_code == 0
            assert "Detected multi-database scan format" in result.output
            assert "Processing database: main_db" in result.output
            assert "Processing database: analytics_db" in result.output
            assert "Schema 'public': 1 tables" in result.output
            assert "Schema 'analytics': 2 tables" in result.output
            assert "Found 3 tables across all schemas/databases" in result.output
            assert "Processing 3 tables" in result.output
            
        finally:
            Path(scan_file).unlink()
    
    def test_generate_unsupported_format(self):
        """Test generate command with unsupported scan format."""
        # Create unsupported scan data
        scan_data = {
            "database_type": "postgresql",
            "some_other_key": "value"
            # Missing 'tables', 'schemas_data', or 'databases' key
        }
        
        scan_file = self.create_test_scan_file(scan_data)
        
        try:
            # Run generate command
            result = self.runner.invoke(main, [
                'generate', scan_file,
                '--ai-model', 'mock',
                '--project-name', 'test_unsupported'
            ], env={'OPENAI_API_KEY': 'test'})
            
            assert result.exit_code != 0
            assert "Unsupported scan result format" in result.output
            
        finally:
            Path(scan_file).unlink()
    
    @patch('cartridge.cli.DBTProjectGenerator')
    @patch('cartridge.cli.AIProviderFactory')
    def test_generate_with_schema_context(self, mock_ai_factory, mock_dbt_generator):
        """Test that schema context is properly preserved in generated models."""
        # Create multi-schema scan data
        scan_data = {
            "database_type": "postgresql",
            "schemas_data": [
                {
                    "schema": "public",
                    "tables": [
                        {
                            "name": "users",
                            "schema": "public",
                            "columns": [
                                {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True}
                            ]
                        }
                    ]
                },
                {
                    "schema": "analytics",
                    "tables": [
                        {
                            "name": "users",  # Same table name, different schema
                            "schema": "analytics",
                            "columns": [
                                {"name": "id", "data_type": "integer", "is_nullable": False, "is_primary_key": True}
                            ]
                        }
                    ]
                }
            ]
        }
        
        scan_file = self.create_test_scan_file(scan_data)
        
        try:
            # Mock AI provider
            mock_ai_provider = AsyncMock()
            mock_ai_provider.generate_models.return_value = self.create_mock_generation_result(2)
            mock_ai_factory.create_provider.return_value = mock_ai_provider
            
            # Mock DBT generator
            mock_dbt_generator.return_value.generate_project.return_value = {"files_generated": {"models": 2}}
            
            # Run generate command
            result = self.runner.invoke(main, [
                'generate', scan_file,
                '--ai-model', 'mock',
                '--project-name', 'test_schema_context'
            ], env={'OPENAI_API_KEY': 'test'})
            
            assert result.exit_code == 0
            
            # Verify that the AI provider was called with properly structured table data
            mock_ai_provider.generate_models.assert_called_once()
            call_args = mock_ai_provider.generate_models.call_args[0][0]
            
            # Check that we have tables from both schemas
            assert len(call_args.tables) == 2
            
            # Check that schema information is preserved
            schemas = [table.schema for table in call_args.tables]
            assert "public" in schemas
            assert "analytics" in schemas
            
        finally:
            Path(scan_file).unlink()


class TestGenerateErrorHandling:
    """Test error handling in the generate command."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_generate_missing_scan_file(self):
        """Test generate command with missing scan file."""
        result = self.runner.invoke(main, [
            'generate', 'nonexistent_file.json',
            '--ai-model', 'mock',
            '--project-name', 'test'
        ], env={'OPENAI_API_KEY': 'test'})
        
        assert result.exit_code != 0
        assert "Scan file not found" in result.output
    
    def test_generate_invalid_json(self):
        """Test generate command with invalid JSON file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            scan_file = f.name
        
        try:
            result = self.runner.invoke(main, [
                'generate', scan_file,
                '--ai-model', 'mock',
                '--project-name', 'test'
            ], env={'OPENAI_API_KEY': 'test'})
            
            assert result.exit_code != 0
            
        finally:
            Path(scan_file).unlink()
