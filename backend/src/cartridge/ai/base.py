"""Base classes for AI model integration."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum

from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class ModelType(str, Enum):
    """Types of dbt models that can be generated."""
    
    STAGING = "staging"
    INTERMEDIATE = "intermediate"
    MARTS = "marts"
    SNAPSHOT = "snapshot"


@dataclass
class ColumnMapping:
    """Information about a column for model generation."""
    
    name: str
    data_type: str
    nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: Optional[str] = None
    foreign_key_column: Optional[str] = None
    description: Optional[str] = None
    sample_values: Optional[List[Any]] = None


@dataclass
class TableMapping:
    """Information about a table for model generation."""
    
    name: str
    schema: str
    table_type: str
    columns: List[ColumnMapping]
    row_count: Optional[int] = None
    description: Optional[str] = None
    primary_key_columns: Optional[List[str]] = None
    foreign_key_relationships: Optional[List[Dict[str, str]]] = None


@dataclass
class ModelGenerationRequest:
    """Request for generating dbt models."""
    
    tables: List[TableMapping]
    model_types: List[ModelType]
    business_context: Optional[str] = None
    naming_convention: Optional[str] = None
    include_tests: bool = True
    include_documentation: bool = True
    target_warehouse: str = "postgresql"
    
    # Analysis insights
    fact_tables: Optional[List[str]] = None
    dimension_tables: Optional[List[str]] = None
    bridge_tables: Optional[List[str]] = None
    relationships: Optional[List[Dict[str, Any]]] = None


@dataclass
class GeneratedTest:
    """Information about a generated dbt test."""
    
    test_type: str  # unique, not_null, relationships, accepted_values, etc.
    column: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


@dataclass
class GeneratedModel:
    """A generated dbt model."""
    
    name: str
    model_type: ModelType
    sql: str
    description: str
    columns: List[Dict[str, Any]]  # Column documentation
    tests: List[GeneratedTest]
    dependencies: List[str]  # Other models this depends on
    materialization: str = "table"  # table, view, incremental, snapshot
    tags: Optional[List[str]] = None
    meta: Optional[Dict[str, Any]] = None


@dataclass
class ModelGenerationResult:
    """Result of model generation."""
    
    models: List[GeneratedModel]
    project_structure: Dict[str, Any]
    generation_metadata: Dict[str, Any]
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None


class AIProvider(ABC):
    """Abstract base class for AI providers."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize AI provider with configuration."""
        self.config = config
        self.logger = get_logger(f"{self.__class__.__name__}")
    
    @abstractmethod
    async def generate_models(self, request: ModelGenerationRequest) -> ModelGenerationResult:
        """Generate dbt models based on schema analysis."""
        pass
    
    @abstractmethod
    async def generate_staging_model(self, table: TableMapping) -> GeneratedModel:
        """Generate a staging model for a specific table."""
        pass
    
    @abstractmethod
    async def generate_intermediate_model(
        self, 
        source_tables: List[TableMapping],
        business_logic: str
    ) -> GeneratedModel:
        """Generate an intermediate model with business logic."""
        pass
    
    @abstractmethod
    async def generate_mart_model(
        self, 
        source_models: List[str],
        model_purpose: str,
        table_info: Optional[TableMapping] = None
    ) -> GeneratedModel:
        """Generate a mart model for analytics."""
        pass
    
    @abstractmethod
    async def generate_tests(self, model: GeneratedModel, table: TableMapping) -> List[GeneratedTest]:
        """Generate appropriate tests for a model."""
        pass
    
    @abstractmethod
    async def generate_documentation(self, model: GeneratedModel) -> Dict[str, str]:
        """Generate documentation for a model."""
        pass
    
    def _create_base_prompt(self, context: str) -> str:
        """Create base prompt with common instructions."""
        return f"""
You are an expert dbt developer and data modeler. Your task is to generate high-quality dbt models based on database schema analysis.

Key principles to follow:
1. Follow dbt best practices and naming conventions
2. Create clean, readable SQL with proper formatting
3. Include appropriate tests and documentation
4. Consider data quality and performance
5. Use proper materialization strategies
6. Follow the medallion architecture (bronze/silver/gold or staging/intermediate/marts)

Context: {context}

Generate SQL that is:
- Well-formatted and readable
- Optimized for the target data warehouse
- Following dbt conventions (ref(), source(), etc.)
- Including appropriate column aliases and comments
- Handling null values appropriately
"""
    
    def _format_sql(self, sql: str) -> str:
        """Format SQL for better readability."""
        # Basic SQL formatting - could be enhanced with sqlparse
        lines = sql.strip().split('\n')
        formatted_lines = []
        
        for line in lines:
            stripped = line.strip()
            if stripped:
                formatted_lines.append(stripped)
        
        return '\n'.join(formatted_lines)
    
    def _generate_model_name(self, table_name: str, model_type: ModelType, prefix: Optional[str] = None) -> str:
        """Generate standardized model name."""
        if prefix:
            return f"{prefix}_{table_name}"
        
        type_prefixes = {
            ModelType.STAGING: "stg",
            ModelType.INTERMEDIATE: "int", 
            ModelType.MARTS: "dim" if "dim" in table_name.lower() else "fct",
            ModelType.SNAPSHOT: "snap"
        }
        
        prefix = type_prefixes.get(model_type, "mod")
        return f"{prefix}_{table_name}"
    
    def _suggest_materialization(self, model_type: ModelType, row_count: Optional[int] = None) -> str:
        """Suggest appropriate materialization strategy."""
        if model_type == ModelType.STAGING:
            return "view"  # Staging models are typically views
        elif model_type == ModelType.INTERMEDIATE:
            return "view" if not row_count or row_count < 100000 else "table"
        elif model_type == ModelType.MARTS:
            return "table"  # Marts should be materialized as tables
        elif model_type == ModelType.SNAPSHOT:
            return "snapshot"
        else:
            return "table"
    
    def _generate_column_tests(self, column: ColumnMapping) -> List[GeneratedTest]:
        """Generate appropriate tests for a column."""
        tests = []
        
        # Not null test for non-nullable columns
        if not column.nullable:
            tests.append(GeneratedTest(
                test_type="not_null",
                column=column.name,
                description=f"Ensure {column.name} is never null"
            ))
        
        # Unique test for primary keys
        if column.is_primary_key:
            tests.append(GeneratedTest(
                test_type="unique",
                column=column.name,
                description=f"Ensure {column.name} values are unique"
            ))
        
        # Relationship test for foreign keys
        if column.is_foreign_key and column.foreign_key_table:
            tests.append(GeneratedTest(
                test_type="relationships",
                column=column.name,
                config={
                    "to": f"ref('stg_{column.foreign_key_table}')",
                    "field": column.foreign_key_column or "id"
                },
                description=f"Ensure {column.name} references valid {column.foreign_key_table} records"
            ))
        
        # Accepted values test for columns with limited sample values
        if column.sample_values and len(column.sample_values) <= 10:
            # Check if values look like categories/enums
            if all(isinstance(v, str) and len(v) < 50 for v in column.sample_values):
                tests.append(GeneratedTest(
                    test_type="accepted_values",
                    column=column.name,
                    config={"values": column.sample_values},
                    description=f"Ensure {column.name} contains only expected values"
                ))
        
        return tests