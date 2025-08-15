"""Configuration management for cartridge-warp."""

import os
import sys
from pathlib import Path
from typing import Any, Literal, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings


def _is_test_mode() -> bool:
    """Check if we're running in test mode."""
    return (
        os.getenv("PYTEST_CURRENT_TEST") is not None
        or "pytest" in sys.modules
        or any("_pytest" in m for m in sys.modules)
        or (len(sys.argv) > 0 and "pytest" in sys.argv[0])
    )


def _parse_comma_separated_list(value: Any) -> Optional[list[str]]:
    """Parse comma-separated string into list of strings."""
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        # Split by comma and strip whitespace, filter empty strings
        return [item.strip() for item in value.split(",") if item.strip()]
    return None


class SourceConfig(BaseModel):
    """Source database configuration."""

    type: str = Field(description="Type of source database")
    connection_string: str = Field(description="Database connection string")
    database: Optional[str] = Field(None, description="Database name")

    # Change detection settings
    change_detection_column: str = Field(
        "updated_at", description="Column used for change detection in batch mode"
    )
    change_detection_strategy: Literal["timestamp", "log", "trigger"] = Field(
        "timestamp", description="Strategy for detecting changes"
    )
    timezone: str = Field("UTC", description="Timezone for timestamp operations")

    @field_validator("type")
    @classmethod
    def validate_type(cls, v):
        """Validate source type."""
        allowed_types = ["mongodb", "mysql", "postgresql", "bigquery"]

        # Allow test types in test mode
        if _is_test_mode():
            allowed_types.extend(["test_source"])

        if v not in allowed_types:
            raise ValueError(f"Source type must be one of: {', '.join(allowed_types)}")
        return v


class DestinationConfig(BaseModel):
    """Destination database configuration."""

    type: str = Field(description="Type of destination database")
    connection_string: str = Field(description="Database connection string")
    database: Optional[str] = Field(None, description="Database name")

    # Schema settings
    metadata_schema: str = Field(
        "cartridge_warp_metadata", description="Schema for metadata tables"
    )

    @field_validator("type")
    @classmethod
    def validate_type(cls, v):
        """Validate destination type."""
        allowed_types = ["postgresql", "mysql", "bigquery"]

        # Allow test types in test mode
        if _is_test_mode():
            allowed_types.extend(["test_destination"])

        if v not in allowed_types:
            raise ValueError(
                f"Destination type must be one of: {', '.join(allowed_types)}"
            )
        return v


class TableConfig(BaseModel):
    """Table-specific configuration."""

    name: str = Field(description="Table name")
    mode: Literal["stream", "batch"] = Field("stream", description="Sync mode")

    # Batch sizes
    stream_batch_size: int = Field(1000, description="Records per stream batch")
    write_batch_size: int = Field(500, description="Records per write transaction")
    full_load_batch_size: int = Field(10000, description="Records per full load batch")

    # Timing
    polling_interval_seconds: int = Field(5, description="Polling interval for changes")

    # Parallelism configuration
    max_parallel_streams: Optional[int] = Field(None, description="Maximum parallel streams for this table")
    
    # Schema evolution
    enable_schema_evolution: bool = Field(True, description="Allow schema changes")

    # Deletion handling
    deletion_strategy: Literal["hard", "soft"] = Field(
        "hard", description="How to handle deleted records"
    )
    soft_delete_column: str = Field(
        "is_deleted", description="Column name for soft deletes"
    )


class SchemaConfig(BaseModel):
    """Schema-level configuration."""

    name: str = Field(description="Schema name")
    mode: Literal["stream", "batch"] = Field("stream", description="Default sync mode")

    # Default settings for tables in this schema
    default_batch_size: int = Field(1000, description="Default batch size")
    default_polling_interval: int = Field(5, description="Default polling interval")
    
    # Default parallelism settings
    default_max_parallel_streams: int = Field(1, description="Default maximum parallel streams per table")

    # Table filtering
    table_whitelist: Optional[list[str]] = Field(
        None, description="List of tables to include (whitelist takes precedence over blacklist)"
    )
    table_blacklist: Optional[list[str]] = Field(
        None, description="List of tables to exclude"
    )

    @field_validator("table_whitelist", mode="before")
    @classmethod
    def parse_table_whitelist(cls, v):
        """Parse comma-separated string for table whitelist."""
        return _parse_comma_separated_list(v)

    @field_validator("table_blacklist", mode="before")
    @classmethod
    def parse_table_blacklist(cls, v):
        """Parse comma-separated string for table blacklist."""
        return _parse_comma_separated_list(v)

    # Table-specific overrides
    tables: list[TableConfig] = Field(
        default_factory=list, description="Table configurations"
    )

    # Schedule for batch mode
    schedule: Optional[str] = Field(None, description="Cron schedule for batch mode")

    def is_table_allowed(self, table_name: str) -> bool:
        """Check if a table is allowed based on whitelist/blacklist configuration."""
        # Whitelist takes precedence - if whitelist is defined, only allow tables in it
        if self.table_whitelist is not None:
            return table_name in self.table_whitelist
            
        # If no whitelist but blacklist exists, exclude tables in blacklist
        if self.table_blacklist is not None:
            return table_name not in self.table_blacklist
            
        # If neither whitelist nor blacklist is defined, allow all tables
        return True


class PrometheusConfig(BaseModel):
    """Prometheus monitoring configuration."""

    enabled: bool = True
    port: int = 8080
    path: str = "/metrics"


class MonitoringConfig(BaseModel):
    """Monitoring and observability configuration."""

    prometheus: PrometheusConfig = PrometheusConfig()
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    structured_logging: bool = True


class ErrorHandlingConfig(BaseModel):
    """Error handling and retry configuration."""

    max_retries: int = 3
    backoff_factor: float = 2.0
    max_backoff_seconds: int = 300
    dead_letter_queue: bool = True
    ignore_type_conversion_errors: bool = True
    log_conversion_warnings: bool = True


class WarpConfig(BaseSettings):
    """Main configuration for cartridge-warp."""

    # Execution mode
    mode: Literal["single", "multi"] = "single"

    # Database configurations
    source: SourceConfig
    destination: DestinationConfig

    # Schema configurations
    schemas: list[SchemaConfig]

    # Single schema mode settings
    single_schema_name: Optional[str] = None

    # Global parallelism settings
    global_max_parallel_streams: int = Field(
        1, description="Global maximum parallel streams per table (can be overridden at schema/table level)"
    )
    
    # Global table filtering (applied to all schemas)
    global_table_whitelist: Optional[list[str]] = Field(
        None, description="Global list of tables to include (applies to all schemas)"
    )
    global_table_blacklist: Optional[list[str]] = Field(
        None, description="Global list of tables to exclude (applies to all schemas)"
    )

    @field_validator("global_table_whitelist", mode="before")
    @classmethod
    def parse_global_table_whitelist(cls, v):
        """Parse comma-separated string for global table whitelist."""
        return _parse_comma_separated_list(v)

    @field_validator("global_table_blacklist", mode="before")
    @classmethod
    def parse_global_table_blacklist(cls, v):
        """Parse comma-separated string for global table blacklist."""
        return _parse_comma_separated_list(v)

    # Global settings
    monitoring: MonitoringConfig = MonitoringConfig()
    error_handling: ErrorHandlingConfig = ErrorHandlingConfig()

    # Runtime settings
    dry_run: bool = False
    full_resync: bool = False

    model_config = {"env_prefix": "CARTRIDGE_WARP_", "case_sensitive": False}

    def is_table_globally_allowed(self, table_name: str) -> bool:
        """Check if a table is allowed based on global whitelist/blacklist configuration."""
        # Global whitelist takes precedence
        if self.global_table_whitelist is not None:
            return table_name in self.global_table_whitelist
            
        # If no global whitelist but global blacklist exists, exclude tables in blacklist
        if self.global_table_blacklist is not None:
            return table_name not in self.global_table_blacklist
            
        # If neither global whitelist nor blacklist is defined, allow all tables
        return True

    def is_table_allowed(self, schema_name: str, table_name: str) -> bool:
        """Check if a table is allowed considering both global and schema-level filters."""
        # First check global filters
        if not self.is_table_globally_allowed(table_name):
            return False
            
        # Then check schema-level filters
        schema_config = self.get_schema_config(schema_name)
        if schema_config:
            return schema_config.is_table_allowed(table_name)
            
        return True

    @field_validator("schemas")
    @classmethod
    def validate_schemas_not_empty(cls, v):
        """Ensure at least one schema is configured."""
        if not v:
            raise ValueError("At least one schema must be configured")
        return v

    @field_validator("single_schema_name")
    @classmethod
    def validate_single_schema_name(cls, v, info):
        """Validate single schema name is provided when in single mode."""
        if info.data.get("mode") == "single" and not v:
            raise ValueError("single_schema_name is required when mode is 'single'")
        return v

    @classmethod
    def from_file(cls, config_path: Union[str, Path]) -> "WarpConfig":
        """Load configuration from YAML file."""
        config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path) as f:
            config_data = yaml.safe_load(f)

        return cls(**config_data)

    def get_schema_config(self, schema_name: str) -> Optional[SchemaConfig]:
        """Get configuration for a specific schema."""
        for schema_config in self.schemas:
            if schema_config.name == schema_name:
                return schema_config
        return None

    def get_table_config(
        self, schema_name: str, table_name: str
    ) -> Optional[TableConfig]:
        """Get configuration for a specific table."""
        schema_config = self.get_schema_config(schema_name)
        if not schema_config:
            return None

        for table_config in schema_config.tables:
            if table_config.name == table_name:
                return table_config
        return None

    def get_effective_max_parallel_streams(
        self, schema_name: str, table_name: str
    ) -> int:
        """Get the effective max parallel streams for a table, considering hierarchy."""
        # Check table-level configuration first
        table_config = self.get_table_config(schema_name, table_name)
        if table_config and table_config.max_parallel_streams is not None:
            return table_config.max_parallel_streams
            
        # Check schema-level configuration
        schema_config = self.get_schema_config(schema_name)
        if schema_config:
            return schema_config.default_max_parallel_streams
            
        # Fall back to global configuration
        return self.global_max_parallel_streams
