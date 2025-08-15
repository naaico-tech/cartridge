"""Tests for parallelism and table filtering configuration."""

import os
import pytest
from cartridge_warp.core.config import WarpConfig, SchemaConfig, TableConfig, SourceConfig, DestinationConfig


class TestParallelismConfiguration:
    """Test parallelism configuration at global, schema, and table levels."""

    def test_global_parallelism_default(self):
        """Test that global parallelism defaults to 1."""
        config = WarpConfig(
            source=SourceConfig(type="test_source", connection_string="test://localhost"),
            destination=DestinationConfig(type="test_destination", connection_string="test://localhost"),
            schemas=[SchemaConfig(name="test_schema")],
            single_schema_name="test_schema"
        )
        assert config.global_max_parallel_streams == 1

    def test_schema_parallelism_default(self):
        """Test that schema parallelism defaults to 1."""
        schema_config = SchemaConfig(name="test_schema")
        assert schema_config.default_max_parallel_streams == 1

    def test_table_parallelism_default(self):
        """Test that table parallelism defaults to None (inherits from schema/global)."""
        table_config = TableConfig(name="test_table")
        assert table_config.max_parallel_streams is None

    def test_effective_parallelism_hierarchy(self):
        """Test the hierarchy of parallelism configuration: table > schema > global."""
        config = WarpConfig(
            global_max_parallel_streams=2,
            source=SourceConfig(type="test_source", connection_string="test://localhost"),
            destination=DestinationConfig(type="test_destination", connection_string="test://localhost"),
            schemas=[
                SchemaConfig(
                    name="test_schema",
                    default_max_parallel_streams=3,
                    tables=[
                        TableConfig(name="table_with_override", max_parallel_streams=4),
                        TableConfig(name="table_without_override"),
                    ]
                )
            ],
            single_schema_name="test_schema"
        )

        # Table with specific override should use table-level setting
        assert config.get_effective_max_parallel_streams("test_schema", "table_with_override") == 4
        
        # Table without override should use schema-level setting
        assert config.get_effective_max_parallel_streams("test_schema", "table_without_override") == 3
        
        # Non-existent table in existing schema should use schema default
        assert config.get_effective_max_parallel_streams("test_schema", "nonexistent_table") == 3
        
        # Non-existent table in non-existent schema should use global setting
        assert config.get_effective_max_parallel_streams("nonexistent_schema", "nonexistent_table") == 2


class TestTableFiltering:
    """Test table whitelist/blacklist functionality."""

    def test_no_filtering_allows_all_tables(self):
        """Test that when no filtering is configured, all tables are allowed."""
        schema_config = SchemaConfig(name="test_schema")
        
        assert schema_config.is_table_allowed("any_table") is True
        assert schema_config.is_table_allowed("another_table") is True

    def test_whitelist_only_allows_specified_tables(self):
        """Test that whitelist only allows specified tables."""
        schema_config = SchemaConfig(
            name="test_schema",
            table_whitelist=["users", "orders"]
        )
        
        assert schema_config.is_table_allowed("users") is True
        assert schema_config.is_table_allowed("orders") is True
        assert schema_config.is_table_allowed("products") is False

    def test_blacklist_excludes_specified_tables(self):
        """Test that blacklist excludes specified tables."""
        schema_config = SchemaConfig(
            name="test_schema",
            table_blacklist=["temp_tables", "logs"]
        )
        
        assert schema_config.is_table_allowed("users") is True
        assert schema_config.is_table_allowed("temp_tables") is False
        assert schema_config.is_table_allowed("logs") is False

    def test_whitelist_takes_precedence_over_blacklist(self):
        """Test that whitelist takes precedence when both are specified."""
        schema_config = SchemaConfig(
            name="test_schema",
            table_whitelist=["users", "orders"],
            table_blacklist=["users", "products"]  # users is in both lists
        )
        
        # Should follow whitelist only
        assert schema_config.is_table_allowed("users") is True  # In whitelist
        assert schema_config.is_table_allowed("orders") is True  # In whitelist
        assert schema_config.is_table_allowed("products") is False  # Not in whitelist

    def test_global_and_schema_filtering_combination(self):
        """Test combination of global and schema-level filtering."""
        config = WarpConfig(
            global_table_whitelist=["users", "orders", "products"],
            source=SourceConfig(type="test_source", connection_string="test://localhost"),
            destination=DestinationConfig(type="test_destination", connection_string="test://localhost"),
            schemas=[
                SchemaConfig(
                    name="test_schema",
                    table_blacklist=["products"]  # Exclude products at schema level
                )
            ],
            single_schema_name="test_schema"
        )

        # Should be allowed by global whitelist and not blocked by schema blacklist
        assert config.is_table_allowed("test_schema", "users") is True
        assert config.is_table_allowed("test_schema", "orders") is True
        
        # Should be blocked by schema blacklist even though in global whitelist
        assert config.is_table_allowed("test_schema", "products") is False
        
        # Should be blocked by global whitelist
        assert config.is_table_allowed("test_schema", "categories") is False

    def test_global_blacklist_filtering(self):
        """Test global blacklist filtering."""
        config = WarpConfig(
            global_table_blacklist=["temp_tables", "logs"],
            source=SourceConfig(type="test_source", connection_string="test://localhost"),
            destination=DestinationConfig(type="test_destination", connection_string="test://localhost"),
            schemas=[SchemaConfig(name="test_schema")],
            single_schema_name="test_schema"
        )

        assert config.is_table_allowed("test_schema", "users") is True
        assert config.is_table_allowed("test_schema", "temp_tables") is False
        assert config.is_table_allowed("test_schema", "logs") is False


class TestEnvironmentVariableSupport:
    """Test environment variable parsing for configuration."""

    def test_comma_separated_list_parsing(self):
        """Test parsing of comma-separated environment variables."""
        # Test with environment variables
        os.environ["CARTRIDGE_WARP_GLOBAL_TABLE_WHITELIST"] = "users,orders, products "
        os.environ["CARTRIDGE_WARP_GLOBAL_TABLE_BLACKLIST"] = " temp_tables, logs,cache "
        
        try:
            config = WarpConfig(
                source=SourceConfig(type="test_source", connection_string="test://localhost"),
                destination=DestinationConfig(type="test_destination", connection_string="test://localhost"),
                schemas=[SchemaConfig(name="test_schema")],
                single_schema_name="test_schema"
            )
            
            # Should parse and strip whitespace
            assert config.global_table_whitelist == ["users", "orders", "products"]
            assert config.global_table_blacklist == ["temp_tables", "logs", "cache"]
            
        finally:
            # Clean up environment variables
            os.environ.pop("CARTRIDGE_WARP_GLOBAL_TABLE_WHITELIST", None)
            os.environ.pop("CARTRIDGE_WARP_GLOBAL_TABLE_BLACKLIST", None)

    def test_empty_comma_separated_list(self):
        """Test parsing of empty comma-separated environment variables."""
        os.environ["CARTRIDGE_WARP_GLOBAL_TABLE_WHITELIST"] = ""
        os.environ["CARTRIDGE_WARP_GLOBAL_TABLE_BLACKLIST"] = "  ,  ,  "
        
        try:
            config = WarpConfig(
                source=SourceConfig(type="test_source", connection_string="test://localhost"),
                destination=DestinationConfig(type="test_destination", connection_string="test://localhost"),
                schemas=[SchemaConfig(name="test_schema")],
                single_schema_name="test_schema"
            )
            
            # Should result in empty lists or None
            assert config.global_table_whitelist in [None, []]
            assert config.global_table_blacklist in [None, []]
            
        finally:
            os.environ.pop("CARTRIDGE_WARP_GLOBAL_TABLE_WHITELIST", None)
            os.environ.pop("CARTRIDGE_WARP_GLOBAL_TABLE_BLACKLIST", None)

    def test_parallelism_environment_override(self):
        """Test that parallelism can be overridden via environment variables."""
        os.environ["CARTRIDGE_WARP_GLOBAL_MAX_PARALLEL_STREAMS"] = "5"
        
        try:
            config = WarpConfig(
                source=SourceConfig(type="test_source", connection_string="test://localhost"),
                destination=DestinationConfig(type="test_destination", connection_string="test://localhost"),
                schemas=[SchemaConfig(name="test_schema")],
                single_schema_name="test_schema"
            )
            
            assert config.global_max_parallel_streams == 5
            
        finally:
            os.environ.pop("CARTRIDGE_WARP_GLOBAL_MAX_PARALLEL_STREAMS", None)


class TestConfigurationIntegration:
    """Test integration of all configuration features."""

    def test_comprehensive_configuration(self):
        """Test a comprehensive configuration with all features."""
        config = WarpConfig(
            mode="single",
            single_schema_name="ecommerce",
            global_max_parallel_streams=3,
            global_table_whitelist=["users", "orders", "products", "inventory"],
            source=SourceConfig(type="test_source", connection_string="test://localhost"),
            destination=DestinationConfig(type="test_destination", connection_string="test://localhost"),
            schemas=[
                SchemaConfig(
                    name="ecommerce",
                    default_max_parallel_streams=4,
                    table_blacklist=["inventory"],  # Exclude inventory despite global whitelist
                    tables=[
                        TableConfig(
                            name="users",
                            max_parallel_streams=1  # Override for sensitive data
                        ),
                        TableConfig(
                            name="orders",
                            max_parallel_streams=6  # Override for high throughput
                        ),
                        TableConfig(name="products"),  # Uses schema default (4)
                    ]
                )
            ]
        )

        # Test parallelism hierarchy
        assert config.get_effective_max_parallel_streams("ecommerce", "users") == 1
        assert config.get_effective_max_parallel_streams("ecommerce", "orders") == 6
        assert config.get_effective_max_parallel_streams("ecommerce", "products") == 4
        assert config.get_effective_max_parallel_streams("ecommerce", "unknown_table") == 4  # Uses schema default

        # Test table filtering
        assert config.is_table_allowed("ecommerce", "users") is True
        assert config.is_table_allowed("ecommerce", "orders") is True
        assert config.is_table_allowed("ecommerce", "products") is True
        assert config.is_table_allowed("ecommerce", "inventory") is False  # Blocked by schema blacklist
        assert config.is_table_allowed("ecommerce", "categories") is False  # Not in global whitelist
