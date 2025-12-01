"""Google Gemini provider for dbt model generation."""

from typing import Dict, List, Any, Optional
import google.generativeai as genai

from cartridge.ai.base import (
    AIProvider, ModelGenerationRequest, ModelGenerationResult, 
    GeneratedModel, GeneratedTest, TableMapping, ModelType, ProjectContext
)
from cartridge.core.config import settings
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class GeminiProvider(AIProvider):
    """Google Gemini provider for generating dbt models."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Gemini provider."""
        super().__init__(config)
        
        api_key = config.get("api_key") or settings.ai.gemini_api_key
        if not api_key:
            raise ValueError("Gemini API key is required")
        
        # Configure Gemini
        genai.configure(api_key=api_key)
        
        self.model_name = config.get("model", "gemini-2.5-flash")
        self.temperature = config.get("temperature", 0.1)
        self.max_tokens = config.get("max_tokens", 4000)
        
        # Initialize the model
        generation_config = genai.types.GenerationConfig(
            temperature=self.temperature,
            max_output_tokens=self.max_tokens,
            candidate_count=1
        )
        
        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config=generation_config
        )
    
    async def generate_execution_plan(self, request: ModelGenerationRequest) -> "ExecutionPlan":
        """
        Generate an execution plan for integrating new schemas into a dbt project.
        
        Uses the Planner prompts to create a structured plan with Google Gemini.
        """
        import json
        from cartridge.ai.prompts import PLANNER_SYSTEM_PROMPT, PLANNER_USER_PROMPT
        from cartridge.ai.base import ExecutionPlan
        
        self.logger.info(f"Generating execution plan with Gemini {self.model_name}")
        
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
        
        # Combine system and user prompts for Gemini
        combined_prompt = f"{PLANNER_SYSTEM_PROMPT}\n\n{PLANNER_USER_PROMPT}".format(
            project_name=context.project_name,
            warehouse_type=context.warehouse_type,
            naming_convention=naming_convention_str,
            existing_sources=existing_sources_str,
            existing_models=existing_models_str,
            schema_name=schema_name,
            new_tables_metadata=new_tables_str
        )
        
        try:
            # Call Gemini API
            response = self.model.generate_content(combined_prompt)
            
            # Extract JSON from response
            content = response.text if response.text else ""
            if not content:
                raise ValueError("Empty response from Gemini API")
            
            # Try to extract JSON if there's surrounding text
            if content.startswith('```json'):
                content = content.split('```json')[1].split('```')[0].strip()
            elif content.startswith('```'):
                content = content.split('```')[1].split('```')[0].strip()
            
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
        self.logger.info(f"Generating models with Gemini {self.model_name}")
        
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
                # Generate fact models
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
                
                # Generate dimension models
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
            
            # Generate project structure
            project_structure = self._generate_project_structure(models)
            
            # Generate metadata
            generation_metadata = {
                "ai_provider": "gemini",
                "model_used": self.model_name,
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
        
        # Create prompt for Gemini
        prompt = f"""
You are an expert dbt developer. Generate a staging model for the table '{table.name}' with the following schema:

Table: {table.schema}.{table.name}
Row Count: {table.row_count or 'Unknown'}
Description: {table.description or 'No description available'}

Columns:
{self._format_columns_for_prompt(table.columns)}

Requirements:
1. Use proper dbt source() function syntax
2. Apply appropriate data type casting where needed
3. Use consistent snake_case naming conventions
4. Add meaningful column aliases if needed
5. Handle potential data quality issues
6. Include basic data transformations (e.g., trimming strings, standardizing formats)

Generate ONLY the SQL SELECT statement for the staging model. Do not include any explanatory text.
"""
        
        try:
            # Generate content with Gemini
            response = self.model.generate_content(prompt)
            
            # Extract SQL from response
            sql_content = response.text if response and response.text else ""
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
                description=f"Staging model for {table.schema}.{table.name}. Cleans and standardizes raw data.",
                columns=column_docs,
                tests=tests,
                dependencies=[],
                materialization="view",
                tags=["staging", table.schema],
                meta={"source_table": f"{table.schema}.{table.name}"}
            )
            
        except Exception as e:
            self.logger.error(f"Failed to generate staging model with Gemini: {e}")
            # Fallback to basic implementation
            return await self._generate_basic_staging_model(table)
    
    async def generate_intermediate_model(
        self, 
        source_tables: List[TableMapping],
        business_logic: str
    ) -> GeneratedModel:
        """Generate an intermediate model with business logic."""
        model_name = f"int_{source_tables[0].name}_enriched"
        
        # Create prompt for Gemini
        prompt = f"""
You are an expert dbt developer. Generate an intermediate dbt model that implements the following business logic:

Business Logic: {business_logic}

Source tables available:
{self._format_tables_for_prompt(source_tables)}

Requirements:
1. Use proper dbt ref() functions for dependencies
2. Join tables appropriately based on relationships
3. Apply the specified business transformations
4. Include proper column aliases and comments
5. Handle null values appropriately
6. Optimize for performance

Generate ONLY the SQL SELECT statement. Do not include explanatory text.
"""
        
        try:
            response = self.model.generate_content(prompt)
            sql_content = response.text if response and response.text else ""
            sql = self._extract_sql_from_response(sql_content)
            
            if not sql:
                # Fallback SQL
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
            
        except Exception as e:
            self.logger.error(f"Failed to generate intermediate model with Gemini: {e}")
            return await self._generate_basic_intermediate_model(source_tables, business_logic)
    
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
        
        # Create prompt for Gemini
        prompt = f"""
You are an expert dbt developer. Generate a {'fact' if is_fact else 'dimension'} dbt model for: {model_purpose}

Source model: {source_models[0]}

{f"Original table schema: {self._format_columns_for_prompt(table_info.columns)}" if table_info else ""}

Requirements for {'fact' if is_fact else 'dimension'} table:
{'1. Include measures and metrics (counts, sums, averages)' if is_fact else '1. Include descriptive attributes'}
2. Use business-friendly column names
3. Add calculated fields where relevant
4. Ensure proper grain and uniqueness
5. Follow dbt best practices
6. Include appropriate joins if needed

Generate ONLY the SQL SELECT statement. Do not include explanatory text.
"""
        
        try:
            response = self.model.generate_content(prompt)
            sql_content = response.text if response and response.text else ""
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
                tags=["marts", model_prefix],
                meta={
                    "model_type": "fact" if is_fact else "dimension",
                    "source_models": source_models
                }
            )
            
        except Exception as e:
            self.logger.error(f"Failed to generate mart model with Gemini: {e}")
            return await self._generate_basic_mart_model(source_models, model_purpose, table_info)
    
    async def generate_tests(self, model: GeneratedModel, table: TableMapping) -> List[GeneratedTest]:
        """Generate appropriate tests for a model."""
        tests = []
        for col in table.columns:
            tests.extend(self._generate_column_tests(col))
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
2. Business context and usage
3. Key transformations applied
4. Data lineage information

Keep the documentation concise but informative.
"""
        
        try:
            response = self.model.generate_content(prompt)
            content = response.text if response and response.text else model.description
            
            return {
                "model_description": content,
                "generated_by": "gemini",
                "model_used": self.model_name
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
    
    def _format_columns_for_prompt(self, columns: List[TableMapping]) -> str:
        """Format columns for Gemini prompt."""
        column_descriptions = []
        
        for col in columns:
            desc_parts = [f"- {col.name}: {col.data_type}"]
            
            if not col.nullable:
                desc_parts.append("(NOT NULL)")
            
            if col.is_primary_key:
                desc_parts.append("(PRIMARY KEY)")
            
            if col.is_foreign_key and col.foreign_key_table:
                desc_parts.append(f"(FK -> {col.foreign_key_table})")
            
            if col.description:
                desc_parts.append(f"- {col.description}")
            
            if col.sample_values:
                sample_str = ", ".join(str(v) for v in col.sample_values[:5])
                desc_parts.append(f"(Sample values: {sample_str})")
            
            column_descriptions.append(" ".join(desc_parts))
        
        return "\n".join(column_descriptions)
    
    def _format_tables_for_prompt(self, tables: List[TableMapping]) -> str:
        """Format table information for Gemini prompt."""
        table_descriptions = []
        
        for table in tables:
            columns = [f"  - {col.name} ({col.data_type})" for col in table.columns]
            table_desc = f"""
Table: {table.name}
Schema: {table.schema}
Row Count: {table.row_count or 'Unknown'}
Columns:
{chr(10).join(columns)}
"""
            table_descriptions.append(table_desc)
        
        return "\n".join(table_descriptions)
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL from Gemini's response."""
        lines = response.split('\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            # Look for SQL keywords to start capturing
            if any(keyword in line.lower() for keyword in ['select', 'with']) and not in_sql:
                in_sql = True
            
            if in_sql:
                # Skip markdown code block markers
                if line.strip().startswith('```'):
                    continue
                    
                sql_lines.append(line)
                
                # Stop at semicolon or end of obvious SQL block
                if line.strip().endswith(';'):
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
    
    async def _generate_basic_intermediate_model(
        self, 
        source_tables: List[TableMapping], 
        business_logic: str
    ) -> GeneratedModel:
        """Generate basic intermediate model as fallback."""
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