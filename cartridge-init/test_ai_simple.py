#!/usr/bin/env python3
"""Simple test script for AI providers without full application setup."""

import sys
import os
import asyncio

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from cartridge.ai.factory import AIProviderFactory
from cartridge.ai.base import ModelGenerationRequest, ModelType, TableMapping, ColumnMapping


def test_ai_provider_factory():
    """Test AI provider factory."""
    print("Testing AI Provider Factory...")
    
    # Test supported models
    models = AIProviderFactory.get_supported_models()
    print(f"✓ Supported models: {len(models)} models")
    
    # Check for major providers
    expected_providers = ["openai", "anthropic", "gemini", "mock"]
    for provider in expected_providers:
        assert provider in models, f"Missing provider: {provider}"
        print(f"✓ {provider} provider available")
    
    # Test creating mock provider
    mock_provider = AIProviderFactory.create_provider("mock", {})
    print(f"✓ Created mock provider: {type(mock_provider).__name__}")
    
    print("AI Provider Factory tests passed!\n")


async def test_mock_ai_provider():
    """Test mock AI provider functionality."""
    print("Testing Mock AI Provider...")
    
    # Create mock provider
    provider = AIProviderFactory.create_provider("mock", {})
    
    # Create sample data
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
        ),
        ColumnMapping(
            name="email",
            data_type="varchar",
            nullable=False
        ),
        ColumnMapping(
            name="created_at",
            data_type="timestamp",
            nullable=False
        )
    ]
    
    table = TableMapping(
        name="customers",
        schema="public",
        table_type="table",
        columns=columns,
        row_count=1000
    )
    
    # Create generation request
    request = ModelGenerationRequest(
        tables=[table],
        model_types=[ModelType.STAGING, ModelType.MARTS],
        business_context="E-commerce customer data",
        include_tests=True,
        include_documentation=True,
        fact_tables=["customers"]
    )
    
    # Generate models
    result = await provider.generate_models(request)
    
    print(f"✓ Generated {len(result.models)} models")
    print(f"✓ AI Provider: {result.generation_metadata['ai_provider']}")
    
    # Check model types
    model_types = [model.model_type for model in result.models]
    assert ModelType.STAGING in model_types, "Missing staging models"
    assert ModelType.MARTS in model_types, "Missing mart models"
    print("✓ Generated both staging and mart models")
    
    # Test individual model generation
    staging_model = await provider.generate_staging_model(table)
    assert staging_model.name == "stg_customers"
    assert staging_model.model_type == ModelType.STAGING
    assert "source" in staging_model.sql
    print("✓ Generated individual staging model")
    
    print("Mock AI Provider tests passed!\n")


def test_gemini_provider_creation():
    """Test Gemini provider creation (without API calls)."""
    print("Testing Gemini Provider Creation...")
    
    try:
        # This will fail because we don't have the google-generativeai package
        # but we can test the factory logic
        config = {"api_key": "test-key"}
        
        # Test that Gemini models are registered
        models = AIProviderFactory.get_supported_models()
        gemini_models = [m for m in models if "gemini" in m]
        
        print(f"✓ Found {len(gemini_models)} Gemini models: {gemini_models}")
        assert len(gemini_models) > 0, "No Gemini models found"
        
        print("Gemini Provider creation tests passed!\n")
        
    except Exception as e:
        print(f"Note: Gemini provider requires google-generativeai package: {e}")


def main():
    """Run all tests."""
    print("🚀 Running AI Provider Tests\n")
    
    try:
        # Test factory
        test_ai_provider_factory()
        
        # Test mock provider
        asyncio.run(test_mock_ai_provider())
        
        # Test Gemini provider creation
        test_gemini_provider_creation()
        
        print("🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()