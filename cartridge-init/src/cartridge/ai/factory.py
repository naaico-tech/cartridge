"""Factory for creating AI providers."""

from typing import Dict, Any, List

from cartridge.ai.base import AIProvider
from cartridge.ai.openai_provider import OpenAIProvider
from cartridge.ai.anthropic_provider import AnthropicProvider
from cartridge.ai.gemini_provider import GeminiProvider
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class AIProviderFactory:
    """Factory for creating AI providers."""
    
    _providers = {
        "openai": OpenAIProvider,
        "gpt-4": OpenAIProvider,
        "gpt-3.5-turbo": OpenAIProvider,
        "gpt-4-turbo": OpenAIProvider,
        "anthropic": AnthropicProvider,
        "claude-3-sonnet": AnthropicProvider,
        "claude-3-opus": AnthropicProvider,
        "claude-3-haiku": AnthropicProvider,
        "claude-3.5-sonnet": AnthropicProvider,
        "gemini": GeminiProvider,
        "gemini-pro": GeminiProvider,
        "gemini-1.5-pro": GeminiProvider,
        "gemini-1.5-flash": GeminiProvider,
        "gemini-2.5-flash": GeminiProvider,
        "gemini-2.5-pro": GeminiProvider,
        "gemini-2.5-pro-exp": GeminiProvider,
        "gemini-2.5-pro-exp-08-01": GeminiProvider,
        "gemini-2.5-pro-exp-08-01": GeminiProvider,
    }
    
    @classmethod
    def create_provider(cls, model_name: str, config: Dict[str, Any]) -> AIProvider:
        """Create an AI provider based on model name."""
        model_name = model_name.lower()
        
        # Determine provider type from model name
        provider_type = None
        for key, provider_class in cls._providers.items():
            if key in model_name:
                provider_type = provider_class
                break
        
        if not provider_type:
            # Default to OpenAI for unknown models
            logger.warning(f"Unknown model {model_name}, defaulting to OpenAI")
            provider_type = OpenAIProvider
        
        # Add model to config
        config = config.copy()
        config["model"] = model_name
        
        logger.info(f"Creating AI provider for model: {model_name}")
        return provider_type(config)
    
    @classmethod
    def get_supported_models(cls) -> List[str]:
        """Get list of supported AI models."""
        return list(cls._providers.keys())
    
    @classmethod
    def register_provider(cls, model_names: List[str], provider_class: type) -> None:
        """Register a new AI provider for specific models."""
        for model_name in model_names:
            cls._providers[model_name.lower()] = provider_class
            # Only log registration if not in CLI mode or if verbose is enabled
            import os
            if not os.environ.get('CARTRIDGE_CLI_MODE') or os.environ.get('CARTRIDGE_VERBOSE'):
                logger.info(f"Registered provider {provider_class.__name__} for model {model_name}")


# Mock provider for testing
class MockAIProvider(AIProvider):
    """Mock AI provider for testing and development."""
    
    async def generate_models(self, request) -> Any:
        """Generate mock models."""
        from cartridge.ai.base import ModelGenerationResult, GeneratedModel, ModelType
        
        models = []
        
        # Generate mock staging models
        if ModelType.STAGING in request.model_types:
            for table in request.tables[:3]:  # Limit for testing
                model = GeneratedModel(
                    name=f"stg_{table.name}",
                    model_type=ModelType.STAGING,
                    sql=f"select * from {{{{ source('{table.schema}', '{table.name}') }}}}",
                    description=f"Mock staging model for {table.name}",
                    columns=[],
                    tests=[],
                    dependencies=[],
                    materialization="view",
                    tags=["staging", "mock"]
                )
                models.append(model)
        
        # Generate mock mart models
        if ModelType.MARTS in request.model_types:
            # Create some example mart models based on the first few tables
            table_names = [table.name for table in request.tables[:3]]
            
            # Generate a fact model (assuming orders or similar)
            if any('order' in name.lower() for name in table_names):
                order_table = next((name for name in table_names if 'order' in name.lower()), table_names[0])
                model = GeneratedModel(
                    name=f"fct_{order_table}",
                    model_type=ModelType.MARTS,
                    sql=f"select * from {{{{ ref('stg_{order_table}') }}}}",
                    description=f"Mock fact model for {order_table}",
                    columns=[],
                    tests=[],
                    dependencies=[f"stg_{order_table}"],
                    materialization="table",
                    tags=["marts", "fact", "mock"]
                )
                models.append(model)
            
            # Generate a dimension model
            if len(table_names) > 1:
                dim_table = table_names[1]
                model = GeneratedModel(
                    name=f"dim_{dim_table}",
                    model_type=ModelType.MARTS,
                    sql=f"select * from {{{{ ref('stg_{dim_table}') }}}}",
                    description=f"Mock dimension model for {dim_table}",
                    columns=[],
                    tests=[],
                    dependencies=[f"stg_{dim_table}"],
                    materialization="table",
                    tags=["marts", "dimension", "mock"]
                )
                models.append(model)
        
        return ModelGenerationResult(
            models=models,
            project_structure={"models": {"staging": [], "marts": []}},
            generation_metadata={
                "ai_provider": "mock",
                "model_used": "mock-model",
                "total_models_generated": len(models)
            }
        )
    
    async def generate_staging_model(self, table):
        """Generate mock staging model."""
        from cartridge.ai.base import GeneratedModel, ModelType
        
        return GeneratedModel(
            name=f"stg_{table.name}",
            model_type=ModelType.STAGING,
            sql=f"select * from {{{{ source('{table.schema}', '{table.name}') }}}}",
            description=f"Mock staging model for {table.name}",
            columns=[],
            tests=[],
            dependencies=[],
            materialization="view"
        )
    
    async def generate_intermediate_model(self, source_tables, business_logic):
        """Generate mock intermediate model."""
        from cartridge.ai.base import GeneratedModel, ModelType
        
        return GeneratedModel(
            name="int_mock_model",
            model_type=ModelType.INTERMEDIATE,
            sql="select * from {{ ref('stg_mock') }}",
            description="Mock intermediate model",
            columns=[],
            tests=[],
            dependencies=["stg_mock"],
            materialization="view"
        )
    
    async def generate_mart_model(self, source_models, model_purpose, table_info=None):
        """Generate mock mart model."""
        from cartridge.ai.base import GeneratedModel, ModelType
        
        return GeneratedModel(
            name="fct_mock_model",
            model_type=ModelType.MARTS,
            sql=f"select * from {{{{ ref('{source_models[0]}') }}}}",
            description="Mock mart model",
            columns=[],
            tests=[],
            dependencies=source_models,
            materialization="table"
        )
    
    async def generate_tests(self, model, table):
        """Generate mock tests."""
        return []
    
    async def generate_documentation(self, model):
        """Generate mock documentation."""
        return {"model_description": "Mock documentation"}


# Register mock provider for testing
AIProviderFactory.register_provider(["mock", "test"], MockAIProvider)

# Register Gemini provider for additional models
AIProviderFactory.register_provider(["gemini-flash", "gemini-ultra"], GeminiProvider)