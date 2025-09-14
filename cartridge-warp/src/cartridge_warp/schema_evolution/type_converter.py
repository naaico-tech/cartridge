"""Type conversion engine for schema evolution."""

import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import structlog

from ..connectors.base import ColumnType
from .types import ConversionRule, ConversionSafety

logger = structlog.get_logger(__name__)


class TypeConversionEngine:
    """Engine for converting between database column types."""
    
    def __init__(self):
        """Initialize the type conversion engine with default rules."""
        self.conversion_rules: Dict[tuple[ColumnType, ColumnType], ConversionRule] = {}
        self.fallback_functions: Dict[ColumnType, Any] = {}
        self._setup_default_rules()
        
    def _setup_default_rules(self) -> None:
        """Set up default type conversion rules."""
        
        # Safe widening conversions
        self.add_rule(ConversionRule(
            source_type=ColumnType.INTEGER,
            target_type=ColumnType.BIGINT,
            safety=ConversionSafety.SAFE,
            conversion_function=int
        ))
        
        self.add_rule(ConversionRule(
            source_type=ColumnType.FLOAT,
            target_type=ColumnType.DOUBLE,
            safety=ConversionSafety.SAFE,
            conversion_function=float
        ))
        
        self.add_rule(ConversionRule(
            source_type=ColumnType.INTEGER,
            target_type=ColumnType.FLOAT,
            safety=ConversionSafety.SAFE,
            conversion_function=float
        ))
        
        self.add_rule(ConversionRule(
            source_type=ColumnType.INTEGER,
            target_type=ColumnType.DOUBLE,
            safety=ConversionSafety.SAFE,
            conversion_function=float
        ))
        
        # String conversions (generally safe)
        for col_type in [ColumnType.INTEGER, ColumnType.BIGINT, ColumnType.FLOAT, 
                        ColumnType.DOUBLE, ColumnType.BOOLEAN, ColumnType.TIMESTAMP, ColumnType.DATE]:
            self.add_rule(ConversionRule(
                source_type=col_type,
                target_type=ColumnType.STRING,
                safety=ConversionSafety.SAFE,
                conversion_function=str
            ))
            
        # JSON conversions (for complex objects)
        self.add_rule(ConversionRule(
            source_type=ColumnType.JSON,
            target_type=ColumnType.STRING,
            safety=ConversionSafety.SAFE,
            conversion_function=lambda x: json.dumps(x) if x is not None else None
        ))
        
        # Risky narrowing conversions
        self.add_rule(ConversionRule(
            source_type=ColumnType.BIGINT,
            target_type=ColumnType.INTEGER,
            safety=ConversionSafety.RISKY,
            conversion_function=int,
            validation_function=lambda x: -2147483648 <= x <= 2147483647 if x is not None else True,
            requires_approval=True
        ))
        
        self.add_rule(ConversionRule(
            source_type=ColumnType.DOUBLE,
            target_type=ColumnType.FLOAT,
            safety=ConversionSafety.RISKY,
            conversion_function=float,
            requires_approval=True
        ))
        
        # String to numeric conversions (dangerous)
        self.add_rule(ConversionRule(
            source_type=ColumnType.STRING,
            target_type=ColumnType.INTEGER,
            safety=ConversionSafety.DANGEROUS,
            conversion_function=self._safe_int_conversion,
            validation_function=self._is_valid_integer,
            fallback_value=0,
            requires_approval=True
        ))
        
        self.add_rule(ConversionRule(
            source_type=ColumnType.STRING,
            target_type=ColumnType.FLOAT,
            safety=ConversionSafety.DANGEROUS,
            conversion_function=self._safe_float_conversion,
            validation_function=self._is_valid_float,
            fallback_value=0.0,
            requires_approval=True
        ))
        
        # Boolean conversions
        self.add_rule(ConversionRule(
            source_type=ColumnType.INTEGER,
            target_type=ColumnType.BOOLEAN,
            safety=ConversionSafety.RISKY,
            conversion_function=bool,
            requires_approval=True
        ))
        
        self.add_rule(ConversionRule(
            source_type=ColumnType.STRING,
            target_type=ColumnType.BOOLEAN,
            safety=ConversionSafety.DANGEROUS,
            conversion_function=self._string_to_bool,
            validation_function=self._is_valid_boolean_string,
            fallback_value=False,
            requires_approval=True
        ))
        
    def add_rule(self, rule: ConversionRule) -> None:
        """Add a conversion rule to the engine."""
        key = (rule.source_type, rule.target_type)
        self.conversion_rules[key] = rule
        logger.debug("Added conversion rule", 
                    source=rule.source_type.value, 
                    target=rule.target_type.value,
                    safety=rule.safety.value)
        
    def get_conversion_rule(self, source_type: ColumnType, target_type: ColumnType) -> Optional[ConversionRule]:
        """Get the conversion rule for two types."""
        return self.conversion_rules.get((source_type, target_type))
        
    def can_convert(self, source_type: ColumnType, target_type: ColumnType) -> bool:
        """Check if conversion between two types is possible."""
        if source_type == target_type:
            return True
        return (source_type, target_type) in self.conversion_rules
        
    def get_conversion_safety(self, source_type: ColumnType, target_type: ColumnType) -> ConversionSafety:
        """Get the safety level of a conversion."""
        if source_type == target_type:
            return ConversionSafety.SAFE
            
        rule = self.get_conversion_rule(source_type, target_type)
        if rule:
            return rule.safety
        return ConversionSafety.INCOMPATIBLE
        
    def convert_value(self, value: Any, source_type: ColumnType, target_type: ColumnType) -> Any:
        """Convert a value from one type to another."""
        if value is None:
            return None
            
        if source_type == target_type:
            return value
            
        rule = self.get_conversion_rule(source_type, target_type)
        if not rule:
            raise ValueError(f"No conversion rule from {source_type.value} to {target_type.value}")
            
        try:
            return rule.convert(value)
        except Exception as e:
            logger.error("Conversion failed", 
                        value=value, 
                        source_type=source_type.value,
                        target_type=target_type.value,
                        error=str(e))
            raise
            
    def estimate_data_loss(self, values: List[Any], source_type: ColumnType, target_type: ColumnType) -> float:
        """Estimate the percentage of data that would be lost in conversion."""
        if not values:
            return 0.0
            
        rule = self.get_conversion_rule(source_type, target_type)
        if not rule:
            return 100.0  # Total loss if no conversion possible
            
        if rule.safety == ConversionSafety.SAFE:
            return 0.0
            
        loss_count = 0
        for value in values:
            if value is None:
                continue
                
            try:
                if not rule.can_convert(value):
                    loss_count += 1
            except:
                loss_count += 1
                
        return (loss_count / len(values)) * 100.0
        
    def batch_convert(self, values: List[Any], source_type: ColumnType, target_type: ColumnType) -> List[Any]:
        """Convert a batch of values with error handling."""
        if source_type == target_type:
            return values
            
        rule = self.get_conversion_rule(source_type, target_type)
        if not rule:
            raise ValueError(f"No conversion rule from {source_type.value} to {target_type.value}")
            
        converted = []
        errors = 0
        
        for value in values:
            try:
                converted.append(rule.convert(value))
            except Exception as e:
                logger.warning("Value conversion failed, using fallback",
                             value=value,
                             error=str(e),
                             fallback=rule.fallback_value)
                converted.append(rule.fallback_value)
                errors += 1
                
        if errors > 0:
            logger.warning("Batch conversion completed with errors",
                          total_values=len(values),
                          errors=errors,
                          error_rate=errors/len(values))
                          
        return converted
        
    # Helper methods for specific conversions
    def _safe_int_conversion(self, value: Any) -> int:
        """Safely convert a value to integer."""
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            return int(float(value))  # Handle "123.0" strings
        if isinstance(value, Decimal):
            return int(value)
        raise ValueError(f"Cannot convert {type(value)} to int")
        
    def _safe_float_conversion(self, value: Any) -> float:
        """Safely convert a value to float."""
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value)
        if isinstance(value, Decimal):
            return float(value)
        raise ValueError(f"Cannot convert {type(value)} to float")
        
    def _string_to_bool(self, value: str) -> bool:
        """Convert string to boolean."""
        if not isinstance(value, str):
            return bool(value)
        
        lower_val = value.lower().strip()
        if lower_val in ('true', '1', 'yes', 'on', 't', 'y'):
            return True
        elif lower_val in ('false', '0', 'no', 'off', 'f', 'n'):
            return False
        else:
            raise ValueError(f"Cannot convert string '{value}' to boolean")
            
    def _is_valid_integer(self, value: Any) -> bool:
        """Check if a value can be converted to integer."""
        try:
            self._safe_int_conversion(value)
            return True
        except:
            return False
            
    def _is_valid_float(self, value: Any) -> bool:
        """Check if a value can be converted to float."""
        try:
            self._safe_float_conversion(value)
            return True
        except:
            return False
            
    def _is_valid_boolean_string(self, value: str) -> bool:
        """Check if a string can be converted to boolean."""
        try:
            self._string_to_bool(value)
            return True
        except:
            return False
