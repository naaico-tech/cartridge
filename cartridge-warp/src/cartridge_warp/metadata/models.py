"""Data models for metadata management.

This module defines Pydantic models for all metadata entities including:
- Sync markers and position tracking
- Schema registry and versioning
- Sync run tracking and monitoring
- Error logging and dead letter queue management
"""

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, computed_field


class MarkerType(str, Enum):
    """Types of sync position markers."""
    STREAM = "stream"      # For stream-based CDC (MongoDB change streams, PostgreSQL logical replication)
    BATCH = "batch"        # For batch processing with timestamps
    INITIAL = "initial"    # For initial data load tracking


class SyncMode(str, Enum):
    """Types of sync operations."""
    STREAM = "stream"      # Real-time streaming
    BATCH = "batch"        # Batch processing
    INITIAL = "initial"    # Initial data load


class SyncStatus(str, Enum):
    """Status of sync operations."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ErrorType(str, Enum):
    """Types of errors that can occur during sync."""
    CONNECTION = "connection"          # Database connection issues
    TRANSFORMATION = "transformation"  # Data transformation errors
    CONSTRAINT = "constraint"          # Database constraint violations
    SCHEMA = "schema"                  # Schema-related errors
    TIMEOUT = "timeout"                # Operation timeout errors
    PERMISSION = "permission"          # Permission/authorization errors
    VALIDATION = "validation"          # Data validation errors


class OperationType(str, Enum):
    """Types of database operations."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    SCHEMA_CHANGE = "schema_change"


class ErrorStatus(str, Enum):
    """Status of error records."""
    OPEN = "open"          # Error is active/unresolved
    RESOLVED = "resolved"  # Error has been fixed
    IGNORED = "ignored"    # Error has been marked as ignorable


class DLQStatus(str, Enum):
    """Status of dead letter queue records."""
    PENDING = "pending"      # Waiting for processing
    PROCESSING = "processing" # Currently being processed
    RESOLVED = "resolved"    # Successfully processed
    DISCARDED = "discarded"  # Permanently discarded


class EvolutionType(str, Enum):
    """Types of schema evolution."""
    CREATE = "create"              # New table creation
    ADD_COLUMN = "add_column"      # Column addition
    MODIFY_COLUMN = "modify_column" # Column modification
    DROP_COLUMN = "drop_column"    # Column removal
    ADD_INDEX = "add_index"        # Index addition
    DROP_INDEX = "drop_index"      # Index removal


class CompatibilityStatus(str, Enum):
    """Schema compatibility status."""
    COMPATIBLE = "compatible"      # Backward compatible change
    BREAKING = "breaking"          # Breaking change
    UNKNOWN = "unknown"           # Unknown compatibility


# Base model with common fields
class BaseMetadataModel(BaseModel):
    """Base model for all metadata entities."""
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }


class SyncMarker(BaseMetadataModel):
    """Model for sync position markers."""
    
    id: UUID = Field(default_factory=uuid4)
    schema_name: str = Field(..., max_length=255)
    table_name: Optional[str] = Field(None, max_length=255)
    marker_type: MarkerType = Field(...)
    position_data: Dict[str, Any] = Field(...)  # LSN, resume token, timestamp, etc.
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sync_run_id: Optional[UUID] = Field(None)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    @field_validator('position_data')
    @classmethod
    def validate_position_data(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Validate position data contains required fields."""
        if not isinstance(v, dict):
            raise ValueError("position_data must be a dictionary")
        if not v:
            raise ValueError("position_data cannot be empty")
        return v


class SchemaDefinition(BaseModel):
    """Schema definition for a table."""
    
    columns: List[Dict[str, Any]] = Field(...)  # Column definitions
    primary_keys: List[str] = Field(default_factory=list)
    indexes: List[Dict[str, Any]] = Field(default_factory=list)
    constraints: List[Dict[str, Any]] = Field(default_factory=list)
    _schema_hash: Optional[str] = None  # Cache for computed hash
    
    @property
    def schema_hash(self) -> str:
        """Compute SHA-256 hash of normalized schema (cached)."""
        if self._schema_hash is None:
            normalized = {
                "columns": sorted(self.columns, key=lambda x: x.get("name", "")),
                "primary_keys": sorted(self.primary_keys),
                "indexes": sorted(self.indexes, key=lambda x: x.get("name", "")),
                "constraints": sorted(self.constraints, key=lambda x: x.get("name", ""))
            }
            schema_str = json.dumps(normalized, sort_keys=True, separators=(',', ':'))
            object.__setattr__(self, '_schema_hash', hashlib.sha256(schema_str.encode()).hexdigest())
        return self._schema_hash  # type: ignore


class SchemaRegistry(BaseMetadataModel):
    """Model for schema registry entries."""
    
    id: UUID = Field(default_factory=uuid4)
    schema_name: str = Field(..., max_length=255)
    table_name: str = Field(..., max_length=255)
    version: int = Field(default=1, ge=1)
    schema_definition: SchemaDefinition = Field(...)
    evolution_type: Optional[EvolutionType] = Field(None)
    previous_version: Optional[int] = Field(None)
    compatibility_status: CompatibilityStatus = Field(default=CompatibilityStatus.COMPATIBLE)
    registered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    registered_by: str = Field(default="cartridge-warp", max_length=255)
    
    @property
    def schema_hash(self) -> str:
        """Get schema hash from definition."""
        return self.schema_definition.schema_hash


class SyncRunStatistics(BaseModel):
    """Statistics for a sync run."""
    
    records_processed: int = Field(default=0, ge=0)
    records_inserted: int = Field(default=0, ge=0)
    records_updated: int = Field(default=0, ge=0)
    records_deleted: int = Field(default=0, ge=0)
    records_failed: int = Field(default=0, ge=0)
    bytes_processed: int = Field(default=0, ge=0)


class SyncRun(BaseMetadataModel):
    """Model for sync run tracking."""
    
    id: UUID = Field(default_factory=uuid4)
    schema_name: str = Field(..., max_length=255)
    sync_mode: SyncMode = Field(...)
    status: SyncStatus = Field(...)
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = Field(None)
    duration_ms: Optional[int] = Field(None, ge=0)
    
    # Statistics
    statistics: SyncRunStatistics = Field(default_factory=SyncRunStatistics)
    
    # Configuration
    config_hash: Optional[str] = Field(None, max_length=64)
    source_info: Optional[Dict[str, Any]] = Field(None)  # Without credentials
    destination_info: Optional[Dict[str, Any]] = Field(None)  # Without credentials
    
    # Error information
    error_message: Optional[str] = Field(None)
    error_details: Optional[Dict[str, Any]] = Field(None)
    
    # Metadata
    instance_id: Optional[str] = Field(None, max_length=255)
    node_id: Optional[str] = Field(None, max_length=255)
    created_by: str = Field(default="cartridge-warp", max_length=255)
    
    @computed_field
    @property
    def is_running(self) -> bool:
        """Check if sync run is currently running."""
        return self.status == SyncStatus.RUNNING
    
    @computed_field
    @property
    def is_completed(self) -> bool:
        """Check if sync run completed successfully."""
        return self.status == SyncStatus.COMPLETED
    
    @computed_field
    @property
    def is_failed(self) -> bool:
        """Check if sync run failed."""
        return self.status == SyncStatus.FAILED


class ErrorLog(BaseMetadataModel):
    """Model for error logging."""
    
    id: UUID = Field(default_factory=uuid4)
    sync_run_id: Optional[UUID] = Field(None)
    schema_name: str = Field(..., max_length=255)
    table_name: Optional[str] = Field(None, max_length=255)
    error_type: ErrorType = Field(...)
    error_code: Optional[str] = Field(None, max_length=50)
    error_message: str = Field(...)
    error_details: Optional[Dict[str, Any]] = Field(None)
    stack_trace: Optional[str] = Field(None)
    
    # Context information
    record_data: Optional[Dict[str, Any]] = Field(None)  # Record that caused error
    operation_type: Optional[OperationType] = Field(None)
    retry_count: int = Field(default=0, ge=0)
    max_retries: int = Field(default=3, ge=0)
    retry_after: Optional[datetime] = Field(None)
    
    # Status
    status: ErrorStatus = Field(default=ErrorStatus.OPEN)
    resolved_at: Optional[datetime] = Field(None)
    resolved_by: Optional[str] = Field(None, max_length=255)
    resolution_notes: Optional[str] = Field(None)
    
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    @computed_field
    @property
    def can_retry(self) -> bool:
        """Check if error can be retried."""
        return self.retry_count < self.max_retries and self.status == ErrorStatus.OPEN
    
    @computed_field
    @property
    def is_resolved(self) -> bool:
        """Check if error is resolved."""
        return self.status == ErrorStatus.RESOLVED


class DeadLetterQueue(BaseMetadataModel):
    """Model for dead letter queue records."""
    
    id: UUID = Field(default_factory=uuid4)
    sync_run_id: Optional[UUID] = Field(None)
    error_log_id: Optional[UUID] = Field(None)
    schema_name: str = Field(..., max_length=255)
    table_name: str = Field(..., max_length=255)
    
    # Record information
    source_record_id: Optional[str] = Field(None, max_length=255)
    operation_type: OperationType = Field(...)
    record_data: Dict[str, Any] = Field(...)
    original_timestamp: Optional[datetime] = Field(None)
    
    # Error context
    error_count: int = Field(default=1, ge=1)
    first_error_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_error_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_error_message: Optional[str] = Field(None)
    
    # Processing status
    status: DLQStatus = Field(default=DLQStatus.PENDING)
    processed_at: Optional[datetime] = Field(None)
    processed_by: Optional[str] = Field(None, max_length=255)
    resolution_method: Optional[str] = Field(None, max_length=100)
    
    @field_validator('record_data')
    @classmethod
    def validate_record_data(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Validate record data is not empty."""
        if not isinstance(v, dict):
            raise ValueError("record_data must be a dictionary")
        if not v:
            raise ValueError("record_data cannot be empty")
        return v
    
    @computed_field
    @property
    def is_pending(self) -> bool:
        """Check if record is pending processing."""
        return self.status == DLQStatus.PENDING
    
    @computed_field
    @property
    def is_resolved(self) -> bool:
        """Check if record has been resolved."""
        return self.status == DLQStatus.RESOLVED


# Union types for easier handling
MetadataModel = Union[SyncMarker, SchemaRegistry, SyncRun, ErrorLog, DeadLetterQueue]

# Export all models
__all__ = [
    # Enums
    "MarkerType",
    "SyncMode", 
    "SyncStatus",
    "ErrorType",
    "OperationType",
    "ErrorStatus",
    "DLQStatus",
    "EvolutionType",
    "CompatibilityStatus",
    
    # Models
    "BaseMetadataModel",
    "SyncMarker",
    "SchemaDefinition",
    "SchemaRegistry", 
    "SyncRunStatistics",
    "SyncRun",
    "ErrorLog",
    "DeadLetterQueue",
    "MetadataModel"
]
