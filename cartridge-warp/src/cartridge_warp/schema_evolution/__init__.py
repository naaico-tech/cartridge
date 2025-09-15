"""Schema evolution engine for intelligent schema changes and type conversions."""

from .config import SchemaEvolutionConfig
from .engine import SchemaEvolutionEngine
from .types import EvolutionStrategy, ConversionRule, SchemaChangeType

__all__ = [
    "SchemaEvolutionConfig",
    "SchemaEvolutionEngine", 
    "EvolutionStrategy",
    "ConversionRule",
    "SchemaChangeType",
]
