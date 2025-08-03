"""OpenAI provider for dbt model generation."""

import json
from typing import Dict, List, Any, Optional
import openai
from openai import AsyncOpenAI

from cartridge.ai.base import (
    AIProvider, ModelGenerationRequest, ModelGenerationResult, 
    GeneratedModel, GeneratedTest, TableMapping, ModelType
)
from cartridge.core.config import settings
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class OpenAIProvider(AIProvider):
    """OpenAI provider for generating dbt models."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize OpenAI provider."""
        super().__init__(config)
        
        api_key = config.get("api_key") or settings.ai.openai_api_key
        if not api_key:
            raise ValueError("OpenAI API key is required")
        
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = config.get("model", "gpt-4")
        self.max_tokens = config.get("max_tokens", 4000)
        self.temperature = config.get("temperature", 0.1)
    
    async def generate_models(self, request: ModelGenerationRequest) -> ModelGenerationResult:
        """Generate dbt models based on schema analysis."""
        self.logger.info(f"Generating models with OpenAI {self.model}")
        
        models = []
        errors = []
        warnings = []
        
        try:
            # Generate staging models
            if ModelType.STAGING in request.model_types:
                for table in request.tables:
                    try:
                        staging_model = await self.generate_staging_model(table)
                        models.append(staging_model)
                    except Exception as e:
                        error_msg = f"Failed to generate staging model for {table.name}: {str(e)}"
                        self.logger.error(error_msg)
                        errors.append(error_msg)
            
            # Generate intermediate models
            if ModelType.INTERMEDIATE in request.model_types:
                # Group related tables for intermediate models
                intermediate_opportunities = self._identify_intermediate_opportunities(request)
                
                for opportunity in intermediate_opportunities:
                    try:
                        intermediate_model = await self.generate_intermediate_model(
                            opportunity["tables"],
                            opportunity["business_logic"]
                        )
                        models.append(intermediate_model)
                    except Exception as e:
                        error_msg = f"Failed to generate intermediate model: {str(e)}"
                        self.logger.error(error_msg)
                        errors.append(error_msg)
            
            # Generate mart models
            if ModelType.MARTS in request.model_types:
                # Generate fact and dimension models
                if request.fact_tables:
                    for fact_table in request.fact_tables:
                        table_info = next((t for t in request.tables if t.name == fact_table), None)
                        if table_info:
                            try:
                                fact_model = await self.generate_mart_model(
                                    [f"stg_{fact_table}"],
                                    f"Fact table for {fact_table} with business metrics",
                                    table_info
                                )
                                models.append(fact_model)
                            except Exception as e:
                                error_msg = f"Failed to generate fact model for {fact_table}: {str(e)}"
                                self.logger.error(error_msg)
                                errors.append(error_msg)
                
                if request.dimension_tables:
                    for dim_table in request.dimension_tables:
                        table_info = next((t for t in request.tables if t.name == dim_table), None)
                        if table_info:
                            try:
                                dim_model = await self.generate_mart_model(
                                    [f"stg_{dim_table}"],
                                    f"Dimension table for {dim_table} with descriptive attributes",
                                    table_info
                                )
                                models.append(dim_model)
                            except Exception as e:
                                error_msg = f"Failed to generate dimension model for {dim_table}: {str(e)}"
                                self.logger.error(error_msg)
                                errors.append(error_msg)
                
                # If no explicit fact/dimension tables, generate some mart models anyway
                if not request.fact_tables and not request.dimension_tables and request.tables:
                    # Generate at least one mart model from the first few tables
                    for table in request.tables[:2]:  # Limit to avoid too many models
                        try:
                            mart_model = await self.generate_mart_model(
                                [f"stg_{table.name}"],
                                f"Business mart model for {table.name}",
                                table
                            )
                            models.append(mart_model)
                        except Exception as e:
                            error_msg = f"Failed to generate mart model for {table.name}: {str(e)}"
                            self.logger.error(error_msg)
                            errors.append(error_msg)
            
            # Generate project structure
            project_structure = self._generate_project_structure(models)
            
            # Generate metadata
            generation_metadata = {
                "ai_provider": "openai",
                "model_used": self.model,
                "total_models_generated": len(models),
                "model_breakdown": {
                    model_type.value: len([m for m in models if m.model_type == model_type])
                    for model_type in ModelType
                },
                "generation_settings": {
                    "temperature": self.temperature,
                    "max_tokens": self.max_tokens,
                    "include_tests": request.include_tests,
                    "include_documentation": request.include_documentation
                }
            }
            
            return ModelGenerationResult(
                models=models,
                project_structure=project_structure,
                generation_metadata=generation_metadata,
                errors=errors if errors else None,
                warnings=warnings if warnings else None
            )
            
        except Exception as e:
            self.logger.error(f"Model generation failed: {e}")
            raise
    
    async def generate_staging_model(self, table: TableMapping) -> GeneratedModel:
        """Generate a staging model for a specific table."""
        model_name = self._generate_model_name(table.name, ModelType.STAGING)
        
        # Create column list for SQL
        columns_sql = []
        column_docs = []
        
        for col in table.columns:
            # Basic column selection with potential transformations
            col_sql = f'    "{col.name}"'
            
            # Add type casting or transformations if needed
            if col.data_type == "timestamp" and col.nullable:
                col_sql += "::timestamp"
            elif col.data_type == "boolean" and "varchar" in col.data_type.lower():
                col_sql += "::boolean"
            
            columns_sql.append(col_sql)
            
            # Column documentation
            column_docs.append({
                "name": col.name,
                "description": col.description or f"{col.name.replace('_', ' ').title()} from source table",
                "data_type": col.data_type,
                "tests": [test.test_type for test in self._generate_column_tests(col)]
            })
        
        # Generate SQL
        sql = f'''
select
{",".join(columns_sql)}
from {{{{ source('{table.schema}', '{table.name}') }}}}
'''.strip()
        
        # Generate tests
        tests = []
        if table.primary_key_columns:
            for pk_col in table.primary_key_columns:
                tests.extend([
                    GeneratedTest("unique", pk_col),
                    GeneratedTest("not_null", pk_col)
                ])
        
        # Add column-specific tests
        for col in table.columns:
            tests.extend(self._generate_column_tests(col))
        
        return GeneratedModel(
            name=model_name,
            model_type=ModelType.STAGING,
            sql=self._format_sql(sql),
            description=f"Staging model for {table.schema}.{table.name}. This model cleans and standardizes raw data.",
            columns=column_docs,
            tests=tests,
            dependencies=[],
            materialization=self._suggest_materialization(ModelType.STAGING, table.row_count),
            tags=["staging", table.schema],
            meta={"source_table": f"{table.schema}.{table.name}"}
        )
    
    async def generate_intermediate_model(
        self, 
        source_tables: List[TableMapping],
        business_logic: str
    ) -> GeneratedModel:
        """Generate an intermediate model with business logic."""
        # Use AI to generate the intermediate model
        prompt = self._create_base_prompt(f"""
Generate an intermediate dbt model that implements the following business logic:
{business_logic}

Source tables available:
{self._format_tables_for_prompt(source_tables)}

The model should:
1. Join relevant tables appropriately
2. Apply business transformations
3. Include proper column aliases and comments
4. Handle null values appropriately
5. Be optimized for performance
""")
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            
            # Parse the AI response
            content = response.choices[0].message.content
            
            # Extract SQL and metadata from response
            model_info = self._parse_ai_response(content, ModelType.INTERMEDIATE)
            
            return model_info
            
        except Exception as e:
            self.logger.error(f"Failed to generate intermediate model: {e}")
            raise
    
    async def generate_mart_model(
        self, 
        source_models: List[str],
        model_purpose: str,
        table_info: Optional[TableMapping] = None
    ) -> GeneratedModel:
        """Generate a mart model for analytics."""
        # Determine if this is a fact or dimension model
        is_fact = "fact" in model_purpose.lower() or "fct" in model_purpose.lower()
        model_prefix = "fct" if is_fact else "dim"
        
        table_name = table_info.name if table_info else "unknown"
        model_name = f"{model_prefix}_{table_name}"
        
        # Use AI to generate the mart model
        prompt = self._create_base_prompt(f"""
Generate a {'fact' if is_fact else 'dimension'} dbt model for: {model_purpose}

Source models available: {', '.join(source_models)}

{f"Original table information: {self._format_table_for_prompt(table_info)}" if table_info else ""}

The model should:
1. Select and transform appropriate columns
2. Include business-friendly column names
3. Add calculated fields if relevant
4. Include proper grain and uniqueness
5. Follow {'fact' if is_fact else 'dimension'} table best practices
""")
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            
            content = response.choices[0].message.content
            model_info = self._parse_ai_response(content, ModelType.MARTS)
            
            # Override name and add metadata
            model_info.name = model_name
            model_info.tags = ["marts", model_prefix]
            model_info.meta = {
                "model_type": "fact" if is_fact else "dimension",
                "source_models": source_models
            }
            
            return model_info
            
        except Exception as e:
            self.logger.error(f"Failed to generate mart model: {e}")
            raise
    
    async def generate_tests(self, model: GeneratedModel, table: TableMapping) -> List[GeneratedTest]:
        """Generate appropriate tests for a model."""
        tests = []
        
        # Use existing column-based test generation
        for col in table.columns:
            tests.extend(self._generate_column_tests(col))
        
        # Add model-level tests
        if table.primary_key_columns:
            # Combination uniqueness test
            if len(table.primary_key_columns) > 1:
                tests.append(GeneratedTest(
                    test_type="unique",
                    config={"combination_of_columns": table.primary_key_columns},
                    description=f"Ensure combination of {', '.join(table.primary_key_columns)} is unique"
                ))
        
        return tests
    
    async def generate_documentation(self, model: GeneratedModel) -> Dict[str, str]:
        """Generate documentation for a model."""
        prompt = f"""
Generate comprehensive documentation for this dbt model:

Model Name: {model.name}
Model Type: {model.model_type.value}
Description: {model.description}

SQL:
{model.sql}

Generate:
1. A detailed model description
2. Column descriptions
3. Usage examples
4. Business context
5. Data lineage notes
"""
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
                temperature=0.1
            )
            
            content = response.choices[0].message.content
            
            return {
                "model_description": content,
                "generated_by": "openai",
                "model_used": self.model
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate documentation: {e}")
            return {"model_description": model.description}
    
    def _identify_intermediate_opportunities(self, request: ModelGenerationRequest) -> List[Dict[str, Any]]:
        """Identify opportunities for intermediate models."""
        opportunities = []
        
        # Look for tables that could benefit from intermediate models
        if request.relationships:
            # Group tables by relationships
            related_groups = {}
            for rel in request.relationships:
                from_table = rel["from_table"].split(".")[-1]  # Get table name without schema
                to_table = rel["to_table"]
                
                if from_table not in related_groups:
                    related_groups[from_table] = set()
                related_groups[from_table].add(to_table)
        
        # Create intermediate model opportunities
        for table_name, related_tables in related_groups.items():
            if len(related_tables) >= 2:  # Tables with multiple relationships
                table_info = next((t for t in request.tables if t.name == table_name), None)
                if table_info:
                    opportunities.append({
                        "name": f"int_{table_name}_enriched",
                        "tables": [table_info],
                        "business_logic": f"Enrich {table_name} with related dimension data from {', '.join(related_tables)}"
                    })
        
        return opportunities
    
    def _format_tables_for_prompt(self, tables: List[TableMapping]) -> str:
        """Format table information for AI prompt."""
        table_descriptions = []
        
        for table in tables:
            columns = [f"  - {col.name} ({col.data_type})" for col in table.columns]
            table_desc = f"""
Table: {table.name}
Columns:
{chr(10).join(columns)}
Row Count: {table.row_count or 'Unknown'}
"""
            table_descriptions.append(table_desc)
        
        return "\n".join(table_descriptions)
    
    def _format_table_for_prompt(self, table: TableMapping) -> str:
        """Format single table information for AI prompt."""
        return self._format_tables_for_prompt([table])
    
    def _parse_ai_response(self, content: str, model_type: ModelType) -> GeneratedModel:
        """Parse AI response into a GeneratedModel."""
        # This is a simplified parser - in production, you'd want more robust parsing
        lines = content.split('\n')
        
        # Extract SQL (look for SELECT statements)
        sql_lines = []
        in_sql = False
        
        for line in lines:
            if 'select' in line.lower() or in_sql:
                in_sql = True
                sql_lines.append(line)
                if line.strip().endswith(';'):
                    break
        
        sql = '\n'.join(sql_lines) if sql_lines else "SELECT 1 -- Generated SQL not found"
        
        # Generate a basic model
        model_name = f"generated_{model_type.value}_model"
        
        return GeneratedModel(
            name=model_name,
            model_type=model_type,
            sql=self._format_sql(sql),
            description=f"AI-generated {model_type.value} model",
            columns=[],  # Would need more sophisticated parsing
            tests=[],
            dependencies=[],
            materialization=self._suggest_materialization(model_type),
            tags=[model_type.value, "ai-generated"]
        )
    
    def _generate_project_structure(self, models: List[GeneratedModel]) -> Dict[str, Any]:
        """Generate dbt project structure."""
        structure = {
            "models": {},
            "tests": {},
            "docs": {}
        }
        
        # Group models by type
        for model in models:
            model_type = model.model_type.value
            if model_type not in structure["models"]:
                structure["models"][model_type] = []
            
            structure["models"][model_type].append({
                "name": model.name,
                "file": f"{model.name}.sql",
                "materialization": model.materialization,
                "tags": model.tags
            })
        
        return structure