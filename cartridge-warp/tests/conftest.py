"""Test configuration for cartridge-warp."""

import asyncio

import pytest


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_config_file(tmp_path):
    """Create a sample configuration file for testing."""
    config_content = """
mode: single

source:
  type: mongodb
  connection_string: "mongodb://localhost:27017"
  database: "test_db"

destination:
  type: postgresql
  connection_string: "postgresql://localhost:5432/test_warehouse"
  database: "test_warehouse"

schemas:
  - name: "test_schema"
    mode: "stream"
    default_batch_size: 100

single_schema_name: "test_schema"

monitoring:
  prometheus:
    enabled: false
  log_level: "DEBUG"

error_handling:
  max_retries: 1
"""

    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    return config_file
