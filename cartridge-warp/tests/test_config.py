"""Tests for cartridge-warp configuration."""

import pytest

from cartridge_warp.core.config import DestinationConfig, SourceConfig, WarpConfig


def test_load_config_from_file(sample_config_file):
    """Test loading configuration from YAML file."""
    config = WarpConfig.from_file(sample_config_file)

    assert config.mode == "single"
    assert config.source.type == "mongodb"
    assert config.destination.type == "postgresql"
    assert len(config.schemas) == 1
    assert config.schemas[0].name == "test_schema"
    assert config.single_schema_name == "test_schema"


def test_config_validation():
    """Test configuration validation."""
    # Test missing schemas
    with pytest.raises(ValueError, match="At least one schema must be configured"):
        WarpConfig(
            source=SourceConfig(
                type="mongodb", connection_string="mongodb://localhost:27017"
            ),
            destination=DestinationConfig(
                type="postgresql", connection_string="postgresql://localhost:5432/db"
            ),
            schemas=[],
        )


def test_get_schema_config(sample_config_file):
    """Test getting schema configuration by name."""
    config = WarpConfig.from_file(sample_config_file)

    schema_config = config.get_schema_config("test_schema")
    assert schema_config is not None
    assert schema_config.name == "test_schema"

    # Test non-existent schema
    schema_config = config.get_schema_config("non_existent")
    assert schema_config is None


def test_prometheus_config_defaults():
    """Test default Prometheus configuration."""
    from cartridge_warp.core.config import PrometheusConfig

    config = PrometheusConfig()
    assert config.enabled is True
    assert config.port == 8080
    assert config.path == "/metrics"
