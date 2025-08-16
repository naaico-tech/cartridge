"""Comprehensive metadata management for cartridge-warp.

This module provides a complete metadata management system for CDC operations including:
- Position tracking for stream and batch processing
- Schema evolution and version management
- Sync run monitoring and statistics
- Error logging and dead letter queue management
- Recovery mechanisms and cleanup operations
"""

from .manager import MetadataManager
from .models import (
    # Enums
    MarkerType,
    SyncMode,
    SyncStatus,
    ErrorType,
    OperationType,
    ErrorStatus,
    DLQStatus,
    EvolutionType,
    CompatibilityStatus,
    
    # Models
    BaseMetadataModel,
    SyncMarker,
    SchemaDefinition,
    SchemaRegistry,
    SyncRunStatistics,
    SyncRun,
    ErrorLog,
    DeadLetterQueue,
    MetadataModel,
)
from .schema import (
    get_schema_creation_sql,
    get_schema_cleanup_sql,
    METADATA_TABLES_SQL,
    TABLE_CREATION_ORDER,
)

__all__ = [
    # Main manager
    "MetadataManager",
    
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
    "MetadataModel",
    
    # Schema utilities
    "get_schema_creation_sql",
    "get_schema_cleanup_sql", 
    "METADATA_TABLES_SQL",
    "TABLE_CREATION_ORDER",
]
