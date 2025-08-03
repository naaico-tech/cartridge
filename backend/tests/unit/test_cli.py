"""Unit tests for CLI commands."""

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from click.testing import CliRunner

from cartridge.cli import main
from cartridge.scanner.base import ScanResult, TableInfo, ColumnInfo
from cartridge.ai.base import ModelGenerationResult, GeneratedModel


class TestCLICommands:
    """Test CLI command functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_cli_help(self):
        """Test CLI help command."""
        result = self.runner.invoke(main, ['--help'])
        assert result.exit_code == 0
        assert 'Cartridge - AI-powered dbt model generator' in result.output

    def test_config_command(self):
        """Test config command shows configuration."""
        result = self.runner.invoke(main, ['config'])
        assert result.exit_code == 0
        assert 'Cartridge Configuration:' in result.output
        assert 'Version:' in result.output
        assert 'Environment:' in result.output

    @patch('cartridge.cli.init_db')
    def test_init_database_command(self, mock_init_db):
        """Test database initialization command."""
        mock_init_db.return_value = AsyncMock()
        
        result = self.runner.invoke(main, ['init-database'])
        assert result.exit_code == 0
        assert 'Initializing database...' in result.output
        assert 'Database initialized successfully!' in result.output
        mock_init_db.assert_called_once()

    @patch('cartridge.cli.drop_tables')
    @patch('cartridge.cli.init_db')
    def test_reset_database_command(self, mock_init_db, mock_drop_tables):
        """Test database reset command."""
        mock_drop_tables.return_value = AsyncMock()
        mock_init_db.return_value = AsyncMock()
        
        result = self.runner.invoke(main, ['reset-database'], input='y\n')
        assert result.exit_code == 0
        assert 'Dropping all database tables...' in result.output
        assert 'Database reset successfully!' in result.output
        mock_drop_tables.assert_called_once()
        mock_init_db.assert_called_once()

    @patch('cartridge.cli.ConnectorFactory')
    def test_scan_command_success(self, mock_connector_factory):
        """Test successful database scan command."""
        # Mock connector
        mock_connector = AsyncMock()
        mock_connector.test_connection.return_value = None
        
        # Mock scan result
        mock_column = ColumnInfo(
            name="id",
            data_type="integer",
            is_nullable=False,
            is_primary_key=True,
            default_value=None,
            comment="Primary key"
        )
        
        mock_table = TableInfo(
            name="users",
            schema="public",
            table_type="BASE TABLE",
            columns=[mock_column],
            constraints=[],
            indexes=[],
            row_count=100,
            sample_data=[{"id": 1, "name": "test"}]
        )
        
        mock_scan_result = ScanResult(
            database_type="postgresql",
            schema="public",
            tables=[mock_table],
            scan_timestamp=None
        )
        
        mock_connector.scan_schema.return_value = mock_scan_result
        mock_connector_factory.create_connector.return_value = mock_connector
        
        # Test scan command
        connection_string = "postgresql://user:pass@localhost:5432/testdb"
        result = self.runner.invoke(main, ['scan', connection_string])
        
        assert result.exit_code == 0
        assert 'Scanning database:' in result.output
        assert 'Connection successful!' in result.output
        assert 'Scan completed! Found 1 tables' in result.output
        assert 'users (BASE TABLE) - 1 columns' in result.output

    @patch('cartridge.cli.ConnectorFactory')
    def test_scan_command_with_output_file(self, mock_connector_factory):
        """Test scan command with output file."""
        # Mock connector and scan result (similar to above)
        mock_connector = AsyncMock()
        mock_connector.test_connection.return_value = None
        
        mock_column = ColumnInfo(
            name="id",
            data_type="integer",
            is_nullable=False,
            is_primary_key=True,
            default_value=None,
            comment=None
        )
        
        mock_table = TableInfo(
            name="users",
            schema="public",
            table_type="BASE TABLE",
            columns=[mock_column],
            constraints=[],
            indexes=[],
            row_count=100,
            sample_data=[]
        )
        
        mock_scan_result = ScanResult(
            database_type="postgresql",
            schema="public",
            tables=[mock_table],
            scan_timestamp=None
        )
        
        mock_connector.scan_schema.return_value = mock_scan_result
        mock_connector_factory.create_connector.return_value = mock_connector
        
        # Test with temporary output file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            output_file = f.name
        
        try:
            connection_string = "postgresql://user:pass@localhost:5432/testdb"
            result = self.runner.invoke(main, ['scan', connection_string, '--output', output_file])
            
            assert result.exit_code == 0
            assert f'Results saved to: {output_file}' in result.output
            
            # Verify output file content
            with open(output_file, 'r') as f:
                output_data = json.load(f)
            
            assert output_data['database_type'] == 'postgresql'
            assert output_data['schema'] == 'public'
            assert len(output_data['tables']) == 1
            assert output_data['tables'][0]['name'] == 'users'
            
        finally:
            Path(output_file).unlink(missing_ok=True)

    def test_scan_command_unsupported_database(self):
        """Test scan command with unsupported database type."""
        connection_string = "oracle://user:pass@localhost:1521/testdb"
        result = self.runner.invoke(main, ['scan', connection_string])
        
        assert result.exit_code == 1
        assert 'Unsupported database type: oracle' in result.output

    @patch('cartridge.cli.ConnectorFactory')
    def test_scan_command_connection_failure(self, mock_connector_factory):
        """Test scan command with connection failure."""
        mock_connector = AsyncMock()
        mock_connector.test_connection.side_effect = Exception("Connection failed")
        mock_connector_factory.create_connector.return_value = mock_connector
        
        connection_string = "postgresql://user:pass@localhost:5432/testdb"
        result = self.runner.invoke(main, ['scan', connection_string])
        
        assert result.exit_code == 1
        assert 'Scan failed: Connection failed' in result.output

    @patch('cartridge.cli.AIProviderFactory')
    @patch('cartridge.cli.DBTProjectGenerator')
    def test_generate_command_success(self, mock_dbt_generator, mock_ai_factory):
        """Test successful model generation command."""
        # Create temporary scan file
        scan_data = {
            "database_type": "postgresql",
            "schema": "public",
            "tables": [
                {
                    "name": "users",
                    "schema": "public",
                    "type": "BASE TABLE",
                    "columns": [
                        {
                            "name": "id",
                            "data_type": "integer",
                            "is_nullable": False,
                            "is_primary_key": True,
                            "comment": "Primary key"
                        },
                        {
                            "name": "name",
                            "data_type": "varchar",
                            "is_nullable": False,
                            "is_primary_key": False,
                            "comment": "User name"
                        }
                    ],
                    "sample_data": [{"id": 1, "name": "John"}]
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(scan_data, f)
            scan_file = f.name
        
        try:
            # Mock AI provider
            mock_ai_provider = AsyncMock()
            mock_model = GeneratedModel(
                name="dim_users",
                model_type="dimension",
                sql_content="SELECT * FROM users",
                description="User dimension table",
                dependencies=[],
                tests=[]
            )
            
            mock_generation_result = ModelGenerationResult(
                models=[mock_model],
                summary="Generated 1 model"
            )
            
            mock_ai_provider.generate_models.return_value = mock_generation_result
            mock_ai_factory.create_provider.return_value = mock_ai_provider
            
            # Mock DBT generator
            mock_dbt_instance = AsyncMock()
            mock_dbt_instance.generate_project.return_value = {
                "dbt_project.yml": "name: test_project",
                "models/dim_users.sql": "SELECT * FROM users"
            }
            mock_dbt_generator.return_value = mock_dbt_instance
            
            # Test with environment variable
            with patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'}):
                with tempfile.TemporaryDirectory() as temp_dir:
                    result = self.runner.invoke(main, [
                        'generate', 
                        scan_file,
                        '--output', temp_dir,
                        '--project-name', 'test_project'
                    ])
                    
                    assert result.exit_code == 0
                    assert 'Generating models from:' in result.output
                    assert 'Generated 1 models' in result.output
                    assert 'Created dbt project in:' in result.output
                    assert 'Generated 2 files' in result.output
                    
        finally:
            Path(scan_file).unlink(missing_ok=True)

    def test_generate_command_missing_scan_file(self):
        """Test generate command with missing scan file."""
        result = self.runner.invoke(main, ['generate', 'nonexistent.json'])
        
        assert result.exit_code == 1
        assert 'Scan file not found: nonexistent.json' in result.output

    def test_generate_command_missing_api_key(self):
        """Test generate command with missing API key."""
        # Create temporary scan file
        scan_data = {"database_type": "postgresql", "tables": []}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(scan_data, f)
            scan_file = f.name
        
        try:
            # Ensure API key is not set
            with patch.dict('os.environ', {}, clear=True):
                result = self.runner.invoke(main, ['generate', scan_file])
                
                assert result.exit_code == 1
                assert 'API key not found. Please set OPENAI_API_KEY environment variable' in result.output
                
        finally:
            Path(scan_file).unlink(missing_ok=True)

    def test_generate_command_unsupported_model(self):
        """Test generate command with unsupported AI model."""
        # Create temporary scan file
        scan_data = {"database_type": "postgresql", "tables": []}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(scan_data, f)
            scan_file = f.name
        
        try:
            result = self.runner.invoke(main, [
                'generate', 
                scan_file,
                '--ai-model', 'unsupported-model'
            ])
            
            assert result.exit_code == 1
            assert 'Cannot auto-detect provider for model: unsupported-model' in result.output
            
        finally:
            Path(scan_file).unlink(missing_ok=True)

    def test_generate_command_yaml_input(self):
        """Test generate command with YAML input file."""
        # Create temporary YAML scan file
        scan_data = {
            "database_type": "postgresql",
            "schema": "public",
            "tables": [
                {
                    "name": "products",
                    "columns": [
                        {"name": "id", "data_type": "integer", "is_primary_key": True}
                    ]
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(scan_data, f)
            scan_file = f.name
        
        try:
            # Mock AI provider and DBT generator
            with patch('cartridge.cli.AIProviderFactory') as mock_ai_factory, \
                 patch('cartridge.cli.DBTProjectGenerator') as mock_dbt_generator, \
                 patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'}):
                
                mock_ai_provider = AsyncMock()
                mock_generation_result = ModelGenerationResult(models=[], summary="Test")
                mock_ai_provider.generate_models.return_value = mock_generation_result
                mock_ai_factory.create_provider.return_value = mock_ai_provider
                
                mock_dbt_instance = AsyncMock()
                mock_dbt_instance.generate_project.return_value = {}
                mock_dbt_generator.return_value = mock_dbt_instance
                
                result = self.runner.invoke(main, ['generate', scan_file])
                
                assert result.exit_code == 0
                assert 'Loading scan results...' in result.output
                
        finally:
            Path(scan_file).unlink(missing_ok=True)


class TestCLIIntegration:
    """Test CLI integration scenarios."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_end_to_end_workflow_simulation(self):
        """Test simulated end-to-end CLI workflow."""
        # This test simulates the full workflow but with mocked components
        
        # Step 1: Mock scan command
        with patch('cartridge.cli.ConnectorFactory') as mock_connector_factory:
            mock_connector = AsyncMock()
            mock_connector.test_connection.return_value = None
            
            mock_scan_result = ScanResult(
                database_type="postgresql",
                schema="ecommerce",
                tables=[],
                scan_timestamp=None
            )
            
            mock_connector.scan_schema.return_value = mock_scan_result
            mock_connector_factory.create_connector.return_value = mock_connector
            
            # Create temporary output file for scan
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                scan_output = f.name
            
            try:
                # Run scan command
                connection_string = "postgresql://cartridge:cartridge@localhost:5432/cartridge"
                scan_result = self.runner.invoke(main, [
                    'scan', 
                    connection_string,
                    '--schema', 'ecommerce',
                    '--output', scan_output
                ])
                
                assert scan_result.exit_code == 0
                
                # Verify scan output file exists
                assert Path(scan_output).exists()
                
                # Step 2: Mock generate command using scan output
                with patch('cartridge.cli.AIProviderFactory') as mock_ai_factory, \
                     patch('cartridge.cli.DBTProjectGenerator') as mock_dbt_generator, \
                     patch.dict('os.environ', {'OPENAI_API_KEY': 'test-key'}):
                    
                    mock_ai_provider = AsyncMock()
                    mock_generation_result = ModelGenerationResult(models=[], summary="Test")
                    mock_ai_provider.generate_models.return_value = mock_generation_result
                    mock_ai_factory.create_provider.return_value = mock_ai_provider
                    
                    mock_dbt_instance = AsyncMock()
                    mock_dbt_instance.generate_project.return_value = {
                        "dbt_project.yml": "name: ecommerce_models"
                    }
                    mock_dbt_generator.return_value = mock_dbt_instance
                    
                    with tempfile.TemporaryDirectory() as temp_dir:
                        generate_result = self.runner.invoke(main, [
                            'generate',
                            scan_output,
                            '--output', temp_dir,
                            '--project-name', 'ecommerce_models',
                            '--business-context', 'E-commerce analytics models'
                        ])
                        
                        assert generate_result.exit_code == 0
                        assert 'ecommerce_models' in generate_result.output
                        
            finally:
                Path(scan_output).unlink(missing_ok=True)

    def test_cli_error_handling(self):
        """Test CLI error handling scenarios."""
        # Test invalid command
        result = self.runner.invoke(main, ['invalid-command'])
        assert result.exit_code != 0
        
        # Test scan with invalid connection string
        result = self.runner.invoke(main, ['scan', 'invalid://connection'])
        assert result.exit_code == 1
        
        # Test generate with invalid file format
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("invalid content")
            invalid_file = f.name
        
        try:
            result = self.runner.invoke(main, ['generate', invalid_file])
            assert result.exit_code == 1
            assert 'Scan file must be JSON or YAML format' in result.output
        finally:
            Path(invalid_file).unlink(missing_ok=True)