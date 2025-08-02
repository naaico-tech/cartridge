"""Tests for AI providers."""

import pytest
from unittest.mock import Mock, AsyncMock

from cartridge.ai.factory import AIProviderFactory
from cartridge.ai.base import ModelGenerationRequest, ModelType, TableMapping, ColumnMapping


class TestAIProviderFactory:
    """Test AI provider factory."""
    
    def test_get_supported_models(self):
        """Test getting supported models."""
        models = AIProviderFactory.get_supported_models()
        
        # Check that all major providers are supported
        assert "openai" in models
        assert "gpt-4" in models
        assert "anthropic" in models
        assert "claude-3-sonnet" in models
        assert "gemini" in models
        assert "gemini-1.5-pro" in models
        assert "mock" in models
    
    def test_create_openai_provider(self):
        """Test creating OpenAI provider."""
        from cartridge.ai.openai_provider import OpenAIProvider
        
        config = {"api_key": "test-key"}
        provider = AIProviderFactory.create_provider("gpt-4", config)
        
        assert isinstance(provider, OpenAIProvider)
        assert provider.config["model"] == "gpt-4"
    
    def test_create_anthropic_provider(self):
        """Test creating Anthropic provider."""
        from cartridge.ai.anthropic_provider import AnthropicProvider
        
        config = {"api_key": "test-key"}
        provider = AIProviderFactory.create_provider("claude-3-sonnet", config)
        
        assert isinstance(provider, AnthropicProvider)
        assert provider.config["model"] == "claude-3-sonnet"
    
    def test_create_gemini_provider(self):
        """Test creating Gemini provider."""
        from cartridge.ai.gemini_provider import GeminiProvider
        
        config = {"api_key": "test-key"}
        provider = AIProviderFactory.create_provider("gemini-1.5-pro", config)
        
        assert isinstance(provider, GeminiProvider)
        assert provider.config["model"] == "gemini-1.5-pro"
    
    def test_create_mock_provider(self):
        """Test creating mock provider."""
        from cartridge.ai.factory import MockAIProvider
        
        config = {}
        provider = AIProviderFactory.create_provider("mock", config)
        
        assert isinstance(provider, MockAIProvider)
    
    def test_unknown_model_defaults_to_openai(self):
        """Test that unknown models default to OpenAI."""
        from cartridge.ai.openai_provider import OpenAIProvider
        
        config = {"api_key": "test-key"}
        provider = AIProviderFactory.create_provider("unknown-model", config)
        
        assert isinstance(provider, OpenAIProvider)


class TestMockAIProvider:
    """Test mock AI provider."""
    
    @pytest.fixture
    def mock_provider(self):
        """Create mock AI provider."""
        from cartridge.ai.factory import MockAIProvider
        return MockAIProvider({})
    
    @pytest.fixture
    def sample_request(self):
        """Create sample generation request."""
        columns = [
            ColumnMapping(
                name="id",
                data_type="integer",
                nullable=False,
                is_primary_key=True
            ),
            ColumnMapping(
                name="name",
                data_type="varchar",
                nullable=False
            )
        ]
        
        table = TableMapping(
            name="customers",
            schema="public",
            table_type="table",
            columns=columns
        )
        
        return ModelGenerationRequest(
            tables=[table],
            model_types=[ModelType.STAGING, ModelType.MARTS],
            fact_tables=["customers"]
        )
    
    @pytest.mark.asyncio
    async def test_generate_models(self, mock_provider, sample_request):
        """Test generating models with mock provider."""
        result = await mock_provider.generate_models(sample_request)
        
        assert result is not None
        assert len(result.models) > 0
        assert result.generation_metadata["ai_provider"] == "mock"
        
        # Check that we have staging and mart models
        model_types = [model.model_type for model in result.models]
        assert ModelType.STAGING in model_types
        assert ModelType.MARTS in model_types
    
    @pytest.mark.asyncio
    async def test_generate_staging_model(self, mock_provider):
        """Test generating staging model."""
        columns = [
            ColumnMapping(name="id", data_type="integer", nullable=False),
            ColumnMapping(name="name", data_type="varchar", nullable=False)
        ]
        
        table = TableMapping(
            name="test_table",
            schema="public", 
            table_type="table",
            columns=columns
        )
        
        model = await mock_provider.generate_staging_model(table)
        
        assert model.name == "stg_test_table"
        assert model.model_type == ModelType.STAGING
        assert "source" in model.sql
        assert model.materialization == "view"
    
    @pytest.mark.asyncio
    async def test_generate_mart_model(self, mock_provider):
        """Test generating mart model."""
        model = await mock_provider.generate_mart_model(
            ["stg_customers"],
            "Customer dimension table"
        )
        
        assert model.name == "fct_mock_model"
        assert model.model_type == ModelType.MARTS
        assert "ref(" in model.sql
        assert model.materialization == "table"


class TestModelGenerationRequest:
    """Test model generation request."""
    
    def test_create_request(self):
        """Test creating generation request."""
        columns = [
            ColumnMapping(name="id", data_type="integer", nullable=False)
        ]
        
        table = TableMapping(
            name="test_table",
            schema="public",
            table_type="table", 
            columns=columns
        )
        
        request = ModelGenerationRequest(
            tables=[table],
            model_types=[ModelType.STAGING],
            business_context="Test context",
            include_tests=True,
            include_documentation=True
        )
        
        assert len(request.tables) == 1
        assert request.tables[0].name == "test_table"
        assert ModelType.STAGING in request.model_types
        assert request.business_context == "Test context"
        assert request.include_tests is True
        assert request.include_documentation is True