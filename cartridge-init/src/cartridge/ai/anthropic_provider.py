"""Anthropic Claude provider for dbt model generation."""

from typing import Dict, List, Any, Optional
import anthropic

from cartridge.ai.base import (
    AIProvider, ModelGenerationRequest, ModelGenerationResult, 
    GeneratedModel, GeneratedTest, TableMapping, ModelType, ProjectContext
)
from cartridge.core.config import settings
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class AnthropicProvider(AIProvider):
    """Anthropic Claude provider for generating dbt models."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Anthropic provider."""
        super().__init__(config)
        
        api_key = config.get("api_key") or settings.ai.anthropic_api_key
        if not api_key:
            raise ValueError("Anthropic API key is required")
        
        self.client = anthropic.AsyncAnthropic(api_key=api_key)
        self.model = config.get("model", "claude-3-sonnet-20240229")
        self.max_tokens = config.get("max_tokens", 4000)
        self.temperature = config.get("temperature", 0.1)
    
    async def generate_execution_plan(self, request: ModelGenerationRequest) -> "ExecutionPlan":
        """
        Generate an execution plan for integrating new schemas into a dbt project.
        
        Uses the Planner prompts to create a structured plan with Anthropic Claude.
        """
        import json
        from cartridge.ai.prompts import PLANNER_SYSTEM_PROMPT, PLANNER_USER_PROMPT
        from cartridge.ai.base import ExecutionPlan
        
        self.logger.info(f"Generating execution plan with Anthropic {self.model}")
        
        # Extract context
        context = request.context or ProjectContext()
        
        # Format existing sources and models
        existing_sources_str = ", ".join(context.existing_sources) if context.existing_sources else "None"
        existing_models_str = ", ".join(context.existing_models) if context.existing_models else "None"
        
        # Format new tables metadata
        new_tables_metadata = []
        for table in request.tables:
            columns_info = []
            for col in table.columns:
                col_info = f"  - {col.name} ({col.data_type})"
                if col.is_primary_key:
                    col_info += " [PK]"
                if col.is_foreign_key:
                    col_info += f" [FK -> {col.foreign_key_table}.{col.foreign_key_column}]"
                if not col.nullable:
                    col_info += " [NOT NULL]"
                columns_info.append(col_info)
            
            table_metadata = f"- {table.name} ({table.row_count or 'unknown'} rows)\n" + "\n".join(columns_info)
            new_tables_metadata.append(table_metadata)
        
        new_tables_str = "\n\n".join(new_tables_metadata)
        schema_name = request.tables[0].schema if request.tables else "unknown"
        naming_convention_str = request.naming_convention or "Standard dbt conventions (stg_, int_, fct_, dim_)"
        
        # Format user prompt
        user_prompt = PLANNER_USER_PROMPT.format(
            project_name=context.project_name,
            warehouse_type=context.warehouse_type,
            naming_convention=naming_convention_str,
            existing_sources=existing_sources_str,
            existing_models=existing_models_str,
            schema_name=schema_name,
            new_tables_metadata=new_tables_str
        )
        
        try:
            # Call Anthropic API
            message = await self.client.messages.create(
                model=self.model,
                max_tokens=4000,
                temperature=0.1,
                system=PLANNER_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}]
            )
            
            # Extract JSON from response
            content = message.content[0].text if message.content else ""
            if not content:
                raise ValueError("Empty response from Anthropic API")
            
            plan_data = json.loads(content)
            execution_plan = ExecutionPlan(**plan_data)
            
            self.logger.info(
                f"Generated execution plan: {execution_plan.strategy} strategy "
                f"with {len(execution_plan.actions)} actions"
            )
            
            return execution_plan
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            self.logger.error(f"Response content: {content}")
            raise ValueError(f"Invalid JSON response from AI: {e}")
        except Exception as e:
            self.logger.error(f"Execution plan generation failed: {e}")
            raise
    
    async def generate_models(self, request: ModelGenerationRequest) -> ModelGenerationResult:
        """Generate dbt models based on schema analysis."""
        self.logger.info(f"Generating models with Anthropic {self.model}")
        
        # For now, implement similar logic to OpenAI but with Claude-specific optimizations
        # This is a simplified implementation - in production you'd optimize prompts for Claude
        
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
            
            # Generate mart models (simplified for now)
            if ModelType.MARTS in request.model_types and request.fact_tables:
                for fact_table in request.fact_tables[:2]:  # Limit for demo
                    table_info = next((t for t in request.tables if t.name == fact_table), None)
                    if table_info:
                        try:
                            fact_model = await self.generate_mart_model(
                                [f"stg_{fact_table}"],
                                f"Fact table for {fact_table}",
                                table_info
                            )
                            models.append(fact_model)
                        except Exception as e:
                            error_msg = f"Failed to generate fact model for {fact_table}: {str(e)}"
                            self.logger.error(error_msg)
                            errors.append(error_msg)
            
            # Generate project structure
            project_structure = self._generate_project_structure(models)
            
            generation_metadata = {
                "ai_provider": "anthropic",
                "model_used": self.model,
                "total_models_generated": len(models),
                "model_breakdown": {
                    model_type.value: len([m for m in models if m.model_type == model_type])
                    for model_type in ModelType
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
        
        # Use Claude to generate staging model
        prompt = f"""
You are an expert dbt developer. Generate a staging model for the table '{table.name}' with the following schema:

Table: {table.schema}.{table.name}
Columns:
{self._format_columns_for_prompt(table.columns)}

Requirements:
1. Use proper dbt source() function
2. Apply basic data type casting where needed
3. Use consistent naming conventions
4. Include column comments
5. Handle potential data quality issues

Generate only the SQL SELECT statement for the staging model.
"""
        
        try:
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract SQL from Claude's response
            sql_content = response.content[0].text if response.content else ""
            
            # Basic SQL extraction and formatting
            sql = self._extract_sql_from_response(sql_content)
            
            if not sql:
                # Fallback to basic staging model
                columns_sql = [f'    "{col.name}"' for col in table.columns]
                sql = f'''
select
{",".join(columns_sql)}
from {{{{ source('{table.schema}', '{table.name}') }}}}
'''.strip()
            
            # Generate tests
            tests = []
            for col in table.columns:
                tests.extend(self._generate_column_tests(col))
            
            # Generate column documentation
            column_docs = [
                {
                    "name": col.name,
                    "description": col.description or f"{col.name.replace('_', ' ').title()}",
                    "data_type": col.data_type
                }
                for col in table.columns
            ]
            
            return GeneratedModel(
                name=model_name,
                model_type=ModelType.STAGING,
                sql=self._format_sql(sql),
                description=f"Staging model for {table.schema}.{table.name}",
                columns=column_docs,
                tests=tests,
                dependencies=[],
                materialization="view",
                tags=["staging", table.schema]
            )
            
        except Exception as e:
            self.logger.error(f"Failed to generate staging model with Claude: {e}")
            # Fallback to basic implementation
            return await self._generate_basic_staging_model(table)
    
    async def generate_intermediate_model(
        self, 
        source_tables: List[TableMapping],
        business_logic: str
    ) -> GeneratedModel:
        """Generate an intermediate model with business logic."""
        # Placeholder implementation
        model_name = f"int_{source_tables[0].name}_enriched"
        
        sql = f'''
select
    *
from {{{{ ref('stg_{source_tables[0].name}') }}}}
-- TODO: Add business logic for {business_logic}
'''
        
        return GeneratedModel(
            name=model_name,
            model_type=ModelType.INTERMEDIATE,
            sql=self._format_sql(sql),
            description=f"Intermediate model: {business_logic}",
            columns=[],
            tests=[],
            dependencies=[f"stg_{source_tables[0].name}"],
            materialization="view",
            tags=["intermediate"]
        )
    
    async def generate_mart_model(
        self, 
        source_models: List[str],
        model_purpose: str,
        table_info: Optional[TableMapping] = None
    ) -> GeneratedModel:
        """Generate a mart model for analytics."""
        is_fact = "fact" in model_purpose.lower()
        model_prefix = "fct" if is_fact else "dim"
        table_name = table_info.name if table_info else "unknown"
        model_name = f"{model_prefix}_{table_name}"
        
        # Use Claude to generate mart model
        prompt = f"""
Generate a {'fact' if is_fact else 'dimension'} dbt model for: {model_purpose}

Source model: {source_models[0]}
{f"Original table schema: {self._format_columns_for_prompt(table_info.columns)}" if table_info else ""}

Requirements:
1. Select appropriate columns for a {'fact' if is_fact else 'dimension'} table
2. Use business-friendly column names
3. Add calculated fields where relevant
4. Follow dbt best practices

Generate only the SQL SELECT statement.
"""
        
        try:
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                messages=[{"role": "user", "content": prompt}]
            )
            
            sql_content = response.content[0].text if response.content else ""
            sql = self._extract_sql_from_response(sql_content)
            
            if not sql:
                # Fallback SQL
                sql = f'''
select
    *
from {{{{ ref('{source_models[0]}') }}}}
'''
            
            return GeneratedModel(
                name=model_name,
                model_type=ModelType.MARTS,
                sql=self._format_sql(sql),
                description=model_purpose,
                columns=[],
                tests=[],
                dependencies=source_models,
                materialization="table",
                tags=["marts", model_prefix]
            )
            
        except Exception as e:
            self.logger.error(f"Failed to generate mart model with Claude: {e}")
            # Return basic model
            return await self._generate_basic_mart_model(source_models, model_purpose, table_info)
    
    async def generate_tests(self, model: GeneratedModel, table: TableMapping) -> List[GeneratedTest]:
        """Generate appropriate tests for a model."""
        tests = []
        for col in table.columns:
            tests.extend(self._generate_column_tests(col))
        return tests
    
    async def generate_documentation(self, model: GeneratedModel) -> Dict[str, str]:
        """Generate documentation for a model."""
        return {
            "model_description": model.description,
            "generated_by": "anthropic",
            "model_used": self.model
        }
    
    def _format_columns_for_prompt(self, columns: List[TableMapping]) -> str:
        """Format columns for Claude prompt."""
        return "\n".join([
            f"- {col.name}: {col.data_type} {'(nullable)' if col.nullable else '(not null)'}"
            for col in columns
        ])
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL from Claude's response."""
        lines = response.split('\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            # Look for SQL keywords to start capturing
            if any(keyword in line.lower() for keyword in ['select', 'with']) and not in_sql:
                in_sql = True
            
            if in_sql:
                sql_lines.append(line)
                
                # Stop at semicolon or end of obvious SQL block
                if line.strip().endswith(';') or (line.strip() == '' and sql_lines):
                    break
        
        return '\n'.join(sql_lines).strip()
    
    async def _generate_basic_staging_model(self, table: TableMapping) -> GeneratedModel:
        """Generate basic staging model as fallback."""
        model_name = self._generate_model_name(table.name, ModelType.STAGING)
        
        columns_sql = [f'    "{col.name}"' for col in table.columns]
        sql = f'''
select
{",".join(columns_sql)}
from {{{{ source('{table.schema}', '{table.name}') }}}}
'''.strip()
        
        return GeneratedModel(
            name=model_name,
            model_type=ModelType.STAGING,
            sql=self._format_sql(sql),
            description=f"Staging model for {table.schema}.{table.name}",
            columns=[],
            tests=[],
            dependencies=[],
            materialization="view",
            tags=["staging"]
        )
    
    async def _generate_basic_mart_model(
        self, 
        source_models: List[str], 
        model_purpose: str, 
        table_info: Optional[TableMapping]
    ) -> GeneratedModel:
        """Generate basic mart model as fallback."""
        is_fact = "fact" in model_purpose.lower()
        model_prefix = "fct" if is_fact else "dim"
        table_name = table_info.name if table_info else "unknown"
        model_name = f"{model_prefix}_{table_name}"
        
        sql = f'''
select
    *
from {{{{ ref('{source_models[0]}') }}}}
'''
        
        return GeneratedModel(
            name=model_name,
            model_type=ModelType.MARTS,
            sql=self._format_sql(sql),
            description=model_purpose,
            columns=[],
            tests=[],
            dependencies=source_models,
            materialization="table",
            tags=["marts", model_prefix]
        )
    
    def _generate_project_structure(self, models: List[GeneratedModel]) -> Dict[str, Any]:
        """Generate dbt project structure."""
        structure = {
            "models": {},
            "tests": {},
            "docs": {}
        }
        
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