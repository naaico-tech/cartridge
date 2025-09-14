"""Configuration for schema evolution engine."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from .types import EvolutionStrategy, ConversionRule, SchemaChangeType


class SchemaEvolutionConfig(BaseModel):
    """Configuration for schema evolution behavior."""
    
    # Global evolution settings
    enabled: bool = Field(default=True, description="Enable schema evolution")
    strategy: EvolutionStrategy = Field(default=EvolutionStrategy.CONSERVATIVE, description="Evolution strategy")
    
    # Change detection settings
    detection_interval_seconds: int = Field(default=30, description="How often to check for schema changes")
    detect_column_additions: bool = Field(default=True, description="Detect new columns")
    detect_column_removals: bool = Field(default=True, description="Detect removed columns")
    detect_type_changes: bool = Field(default=True, description="Detect column type changes") 
    detect_constraint_changes: bool = Field(default=False, description="Detect constraint changes")
    detect_index_changes: bool = Field(default=False, description="Detect index changes")
    
    # Type conversion settings
    enable_type_widening: bool = Field(default=True, description="Allow safe type widening (int -> bigint)")
    enable_type_narrowing: bool = Field(default=False, description="Allow risky type narrowing")
    enable_object_to_json: bool = Field(default=True, description="Convert complex objects to JSON")
    enable_fallback_to_varchar: bool = Field(default=True, description="Fall back to VARCHAR for unknown types")
    
    # Safety settings
    require_approval_for_risky_changes: bool = Field(default=True, description="Require manual approval for risky changes")
    require_approval_for_dangerous_changes: bool = Field(default=True, description="Require manual approval for dangerous changes") 
    max_data_loss_percentage: float = Field(default=0.01, description="Maximum acceptable data loss percentage")
    enable_rollback: bool = Field(default=True, description="Enable automatic rollback on failures")
    
    # Performance settings
    max_concurrent_migrations: int = Field(default=1, description="Maximum concurrent schema migrations")
    migration_timeout_seconds: int = Field(default=300, description="Timeout for individual migrations")
    batch_size: int = Field(default=1000, description="Batch size for data conversion operations")
    
    # Table-specific overrides
    table_configs: Dict[str, "TableEvolutionConfig"] = Field(default_factory=dict, description="Per-table evolution settings")
    
    # Excluded items
    excluded_tables: List[str] = Field(default_factory=list, description="Tables to exclude from evolution")
    excluded_columns: Dict[str, List[str]] = Field(default_factory=dict, description="Columns to exclude by table")
    
    # Notification settings
    notify_on_schema_changes: bool = Field(default=True, description="Send notifications on schema changes")
    notification_webhooks: List[str] = Field(default_factory=list, description="Webhook URLs for notifications")
    
    # Logging and monitoring
    log_all_changes: bool = Field(default=True, description="Log all schema evolution events")
    metrics_enabled: bool = Field(default=True, description="Enable schema evolution metrics")
    
    class Config:
        """Pydantic configuration."""
        env_prefix = "CARTRIDGE_WARP_SCHEMA_EVOLUTION_"
        

class TableEvolutionConfig(BaseModel):
    """Per-table schema evolution configuration."""
    
    # Override global settings for this table
    enabled: Optional[bool] = Field(default=None, description="Override global enabled setting")
    strategy: Optional[EvolutionStrategy] = Field(default=None, description="Override global strategy")
    
    # Table-specific settings
    allow_column_additions: bool = Field(default=True, description="Allow new columns for this table")
    allow_column_removals: bool = Field(default=False, description="Allow column removal for this table")
    allow_type_changes: bool = Field(default=True, description="Allow type changes for this table")
    
    # Custom conversion rules for this table
    custom_conversion_rules: List[ConversionRule] = Field(default_factory=list, description="Custom conversion rules")
    
    # Excluded columns for this table
    excluded_columns: List[str] = Field(default_factory=list, description="Columns to exclude from evolution")
    
    # Safety overrides
    max_data_loss_percentage: Optional[float] = Field(default=None, description="Override global data loss threshold")
    require_approval: Optional[bool] = Field(default=None, description="Override global approval requirement")


@dataclass
class EvolutionMetrics:
    """Metrics for schema evolution operations."""
    
    total_changes_detected: int = 0
    changes_applied_successfully: int = 0
    changes_failed: int = 0
    changes_requiring_approval: int = 0
    total_processing_time_seconds: float = 0.0
    last_check: Optional[datetime] = None
    
    # By change type
    column_additions: int = 0
    column_removals: int = 0
    type_changes: int = 0
    table_additions: int = 0
    table_removals: int = 0
    
    # Safety metrics
    safe_changes: int = 0
    risky_changes: int = 0
    dangerous_changes: int = 0
    rollbacks_performed: int = 0
