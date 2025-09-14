"""Types and enums for schema evolution engine."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional, TypedDict, Tuple, Union

from ..connectors.base import ColumnType


class EvolutionStrategy(Enum):
    """Strategy for schema evolution behavior."""
    
    STRICT = "strict"  # Manual intervention for all changes
    CONSERVATIVE = "conservative"  # Safe changes auto, risky require approval
    PERMISSIVE = "permissive"  # Most changes auto, warn on risky
    AGGRESSIVE = "aggressive"  # All changes auto with fallbacks


class ConversionSafety(Enum):
    """Safety level for type conversions."""
    
    SAFE = "safe"  # No data loss, always allowed
    RISKY = "risky"  # Potential data loss, requires validation
    DANGEROUS = "dangerous"  # High risk of data loss, requires approval
    INCOMPATIBLE = "incompatible"  # Cannot convert, blocked


class SchemaChangeType(Enum):
    """Types of schema changes that can be detected."""
    
    # Table changes
    ADD_TABLE = "add_table"
    DROP_TABLE = "drop_table"
    
    # Column changes
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    MODIFY_COLUMN_TYPE = "modify_column_type"
    MODIFY_COLUMN_CONSTRAINT = "modify_column_constraint"
    RENAME_COLUMN = "rename_column"
    
    # Index changes
    ADD_INDEX = "add_index"
    DROP_INDEX = "drop_index"
    
    # Constraint changes
    ADD_CONSTRAINT = "add_constraint"
    DROP_CONSTRAINT = "drop_constraint"


class SchemaDefinition(TypedDict):
    """Typed structure for schema definitions."""
    
    name: str
    type: str
    nullable: bool
    primary_key: bool
    constraints: List[str]


class TableDefinition(TypedDict):
    """Typed structure for table definitions."""
    
    name: str
    columns: Dict[str, SchemaDefinition]
    constraints: List[str]
    indexes: List[str]


class HealthStatus(TypedDict, total=False):
    """Typed structure for engine health status."""
    
    # Required fields
    running: bool
    enabled: bool
    strategy: str
    schemas_monitored: int
    last_check: Optional[str]
    metrics: Dict[str, Union[int, float]]
    
    # Optional fields
    detector_stats: Dict[str, Union[int, str]]


@dataclass
class ConversionRule:
    """Rule for converting between column types."""
    
    source_type: ColumnType
    target_type: ColumnType
    safety: ConversionSafety
    conversion_function: Optional[Callable[[Any], Any]] = None
    validation_function: Optional[Callable[[Any], bool]] = None
    fallback_value: Optional[Any] = None
    requires_approval: bool = False
    
    def can_convert(self, value: Any) -> bool:
        """Check if a value can be converted with this rule."""
        if self.validation_function:
            return self.validation_function(value)
        return True
        
    def convert(self, value: Any) -> Any:
        """Convert a value using this rule."""
        if value is None:
            return None
            
        if not self.can_convert(value):
            if self.fallback_value is not None:
                return self.fallback_value
            raise ValueError(f"Cannot convert value {value} from {self.source_type} to {self.target_type}")
            
        if self.conversion_function:
            return self.conversion_function(value)
        
        return value


@dataclass  
class SchemaEvolutionEvent:
    """Event representing a schema evolution change."""
    
    change_type: SchemaChangeType
    schema_name: str
    table_name: str
    column_name: Optional[str] = None
    old_definition: Optional[TableDefinition] = None
    new_definition: Optional[TableDefinition] = None
    conversion_rule: Optional[ConversionRule] = None
    requires_approval: bool = False
    safety_level: ConversionSafety = ConversionSafety.SAFE
    estimated_impact: str = ""
    rollback_sql: Optional[str] = None


@dataclass
class EvolutionResult:
    """Result of a schema evolution operation."""
    
    success: bool
    events: list[SchemaEvolutionEvent]
    applied_changes: list[str]  # SQL statements executed
    warnings: list[str]
    errors: list[str]
    rollback_commands: list[str]
    processing_time_seconds: float
