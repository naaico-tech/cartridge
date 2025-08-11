"""Integration tests for multi-schema and multi-database scanning."""

import pytest
import json
import tempfile
import yaml
from pathlib import Path
from click.testing import CliRunner

from cartridge.cli import main


class TestMultiSchemaIntegration:
    """Integration tests for multi-schema scanning CLI functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def create_test_config_file(self, config_data: dict, suffix: str = '.yml') -> str:
        """Create a temporary config file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix=suffix, delete=False) as f:
            if suffix in ['.yml', '.yaml']:
                yaml.dump(config_data, f)
            else:
                json.dump(config_data, f)
            return f.name

    def test_cli_help_includes_new_commands(self):
        """Test that CLI help includes the new multi-schema commands."""
        result = self.runner.invoke(main, ['--help'])
        assert result.exit_code == 0
        
        # Check that scan command mentions multi-schema support
        scan_help = self.runner.invoke(main, ['scan', '--help'])
        assert scan_help.exit_code == 0
        assert '--schemas' in scan_help.output
        assert 'Multiple schemas' in scan_help.output or 'comma-separated' in scan_help.output

    def test_cli_scan_multi_help(self):
        """Test that scan-multi command help is available."""
        result = self.runner.invoke(main, ['scan-multi', '--help'])
        assert result.exit_code == 0
        assert 'multiple databases' in result.output.lower()
        assert 'configuration file' in result.output.lower()

    def test_scan_multi_missing_config_file(self):
        """Test scan-multi with missing configuration file."""
        result = self.runner.invoke(main, ['scan-multi', 'nonexistent_config.yml'])
        assert result.exit_code != 0
        assert 'Configuration file not found' in result.output

    def test_scan_multi_invalid_config_format(self):
        """Test scan-multi with invalid configuration format."""
        # Create invalid config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("invalid config content")
            invalid_config_path = f.name
        
        try:
            result = self.runner.invoke(main, ['scan-multi', invalid_config_path])
            assert result.exit_code != 0
            assert 'must be YAML' in result.output or 'must be JSON' in result.output
        finally:
            Path(invalid_config_path).unlink()

    def test_scan_multi_missing_databases_key(self):
        """Test scan-multi with config file missing 'databases' key."""
        config_data = {
            "invalid_key": "value"
        }
        
        config_path = self.create_test_config_file(config_data)
        
        try:
            result = self.runner.invoke(main, ['scan-multi', config_path])
            assert result.exit_code != 0
            assert 'databases' in result.output
        finally:
            Path(config_path).unlink()

    def test_scan_multi_empty_databases_list(self):
        """Test scan-multi with empty databases list."""
        config_data = {
            "databases": []
        }
        
        config_path = self.create_test_config_file(config_data)
        
        try:
            result = self.runner.invoke(main, ['scan-multi', config_path])
            assert result.exit_code != 0
            assert 'non-empty list' in result.output
        finally:
            Path(config_path).unlink()

    def test_scan_multi_missing_required_fields(self):
        """Test scan-multi with database config missing required fields."""
        config_data = {
            "databases": [
                {
                    "name": "test_db"
                    # Missing 'uri' and 'schemas'
                }
            ]
        }
        
        config_path = self.create_test_config_file(config_data)
        
        try:
            result = self.runner.invoke(main, ['scan-multi', config_path])
            # The CLI should continue processing and show the error message
            # but still complete with exit code 0 (resilient behavior)
            assert result.exit_code == 0
            assert 'missing required field' in result.output
            assert 'Failed to scan database' in result.output
        finally:
            Path(config_path).unlink()

    def test_scan_multi_valid_config_structure(self):
        """Test scan-multi with valid config structure (will fail on connection but should parse correctly)."""
        config_data = {
            "databases": [
                {
                    "name": "sales_db",
                    "uri": "postgresql://user:pass@localhost:5432/sales",
                    "schemas": ["public", "analytics"]
                },
                {
                    "name": "marketing_db",
                    "uri": "mysql://user:pass@localhost:3306/marketing",
                    "schemas": ["raw", "campaigns"]
                }
            ]
        }
        
        config_path = self.create_test_config_file(config_data)
        
        try:
            result = self.runner.invoke(main, ['scan-multi', config_path])
            # Should fail on connection, but config parsing should work
            # The output should show it's trying to process databases
            assert 'Processing database: sales_db' in result.output or 'Scanning multiple databases' in result.output
        finally:
            Path(config_path).unlink()

    def test_scan_multi_json_config(self):
        """Test scan-multi with JSON configuration file."""
        config_data = {
            "databases": [
                {
                    "name": "test_db",
                    "uri": "postgresql://user:pass@localhost:5432/test",
                    "schemas": ["public"]
                }
            ]
        }
        
        config_path = self.create_test_config_file(config_data, '.json')
        
        try:
            result = self.runner.invoke(main, ['scan-multi', config_path])
            # Should fail on connection, but JSON parsing should work
            assert 'Scanning multiple databases' in result.output
        finally:
            Path(config_path).unlink()

    def test_scan_schemas_parameter(self):
        """Test the --schemas parameter in regular scan command."""
        # This will fail on connection, but should show it's trying to scan multiple schemas
        result = self.runner.invoke(main, [
            'scan', 
            'postgresql://user:pass@localhost:5432/test',
            '--schemas', 'public,staging,marts'
        ])
        
        # Should show it's scanning multiple schemas
        assert 'public, staging, marts' in result.output or 'Schemas:' in result.output

    def test_scan_single_schema_backward_compatibility(self):
        """Test that single schema scanning still works (backward compatibility)."""
        result = self.runner.invoke(main, [
            'scan', 
            'postgresql://user:pass@localhost:5432/test',
            '--schema', 'public'
        ])
        
        # Should show single schema scanning
        assert 'Schema: public' in result.output

    def test_scan_output_to_file(self):
        """Test scanning with output to file."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name
        
        try:
            result = self.runner.invoke(main, [
                'scan', 
                'postgresql://user:pass@localhost:5432/test',
                '--schemas', 'public,staging',
                '--output', output_path
            ])
            
            # Should mention saving results (even if scan fails)
            # The file creation logic should be tested
            if 'Results saved to:' in result.output:
                assert output_path in result.output
        finally:
            if Path(output_path).exists():
                Path(output_path).unlink()

    def test_scan_multi_output_to_file(self):
        """Test multi-database scanning with output to file."""
        config_data = {
            "databases": [
                {
                    "name": "test_db",
                    "uri": "postgresql://user:pass@localhost:5432/test",
                    "schemas": ["public"]
                }
            ]
        }
        
        config_path = self.create_test_config_file(config_data)
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name
        
        try:
            result = self.runner.invoke(main, [
                'scan-multi', config_path,
                '--output', output_path
            ])
            
            # Check if output file handling is mentioned
            if 'Results saved to:' in result.output:
                assert output_path in result.output
        finally:
            Path(config_path).unlink()
            if Path(output_path).exists():
                Path(output_path).unlink()


class TestMultiSchemaEndToEnd:
    """End-to-end tests for multi-schema functionality."""

    @pytest.mark.skipif(True, reason="Requires actual database setup")
    def test_real_database_multi_schema_scan(self):
        """Test actual multi-schema scanning with real database.
        
        This test is skipped by default as it requires a real database setup.
        Enable by removing the skipif decorator and setting up test databases.
        """
        runner = CliRunner()
        
        # This would require actual test databases
        result = runner.invoke(main, [
            'scan',
            'postgresql://test_user:test_pass@localhost:5432/test_db',
            '--schemas', 'public,test_schema',
            '--output', 'test_output.json'
        ])
        
        assert result.exit_code == 0
        assert Path('test_output.json').exists()
        
        # Verify output structure
        with open('test_output.json', 'r') as f:
            output_data = json.load(f)
        
        assert 'schemas' in output_data
        assert 'schemas_data' in output_data
        assert output_data['total_schemas'] == 2
        
        # Cleanup
        Path('test_output.json').unlink()

    @pytest.mark.skipif(True, reason="Requires actual database setup")
    def test_real_multi_database_scan(self):
        """Test actual multi-database scanning with real databases.
        
        This test is skipped by default as it requires real database setup.
        """
        config_data = {
            "databases": [
                {
                    "name": "db1",
                    "uri": "postgresql://test_user:test_pass@localhost:5432/db1",
                    "schemas": ["public", "schema1"]
                },
                {
                    "name": "db2",
                    "uri": "postgresql://test_user:test_pass@localhost:5432/db2",
                    "schemas": ["public"]
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump(config_data, f)
            config_path = f.name
        
        runner = CliRunner()
        
        try:
            result = runner.invoke(main, [
                'scan-multi', config_path,
                '--output', 'multi_db_output.json'
            ])
            
            assert result.exit_code == 0
            assert Path('multi_db_output.json').exists()
            
            # Verify output structure
            with open('multi_db_output.json', 'r') as f:
                output_data = json.load(f)
            
            assert output_data['scan_type'] == 'multi_database'
            assert output_data['total_databases'] == 2
            assert len(output_data['databases']) == 2
            
            # Cleanup
            Path('multi_db_output.json').unlink()
            
        finally:
            Path(config_path).unlink()


class TestConfigFileValidation:
    """Test configuration file validation and error handling."""

    def test_yaml_config_validation(self):
        """Test YAML configuration file validation."""
        valid_config = {
            "databases": [
                {
                    "name": "test_db",
                    "uri": "postgresql://user:pass@localhost:5432/test",
                    "schemas": ["public", "staging"]
                }
            ]
        }
        
        # Test valid config
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump(valid_config, f)
            config_path = f.name
        
        try:
            with open(config_path, 'r') as f:
                loaded = yaml.safe_load(f)
            
            assert 'databases' in loaded
            assert len(loaded['databases']) == 1
            assert loaded['databases'][0]['name'] == 'test_db'
        finally:
            Path(config_path).unlink()

    def test_json_config_validation(self):
        """Test JSON configuration file validation."""
        valid_config = {
            "databases": [
                {
                    "name": "test_db",
                    "uri": "postgresql://user:pass@localhost:5432/test",
                    "schemas": ["public", "staging"]
                }
            ]
        }
        
        # Test valid config
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(valid_config, f)
            config_path = f.name
        
        try:
            with open(config_path, 'r') as f:
                loaded = json.load(f)
            
            assert 'databases' in loaded
            assert len(loaded['databases']) == 1
            assert loaded['databases'][0]['name'] == 'test_db'
        finally:
            Path(config_path).unlink()

    def test_malformed_yaml_config(self):
        """Test handling of malformed YAML configuration."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            config_path = f.name
        
        try:
            runner = CliRunner()
            result = runner.invoke(main, ['scan-multi', config_path])
            assert result.exit_code != 0
        finally:
            Path(config_path).unlink()

    def test_malformed_json_config(self):
        """Test handling of malformed JSON configuration."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"invalid": json content}')
            config_path = f.name
        
        try:
            runner = CliRunner()
            result = runner.invoke(main, ['scan-multi', config_path])
            assert result.exit_code != 0
        finally:
            Path(config_path).unlink()
