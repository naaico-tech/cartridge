"""Unit tests for execution plan generation and planner workflow."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import json

from cartridge.ai.base import (
    ProjectContext, PlanAction, ExecutionPlan, 
    ModelGenerationRequest, TableMapping, ColumnMapping, ModelType
)
from cartridge.ai.openai_provider import OpenAIProvider
from cartridge.ai.anthropic_provider import AnthropicProvider
from cartridge.ai.gemini_provider import GeminiProvider


# Test Data Fixtures
@pytest.fixture
def sample_columns():
    """Sample columns for testing."""
    return [
        ColumnMapping(
            name="id",
            data_type="bigint",
            nullable=False,
            is_primary_key=True
        ),
        ColumnMapping(
            name="customer_id",
            data_type="bigint",
            nullable=False,
            is_foreign_key=True,
            foreign_key_table="customers",
            foreign_key_column="id"
        ),
        ColumnMapping(
            name="order_date",
            data_type="timestamp",
            nullable=False
        ),
        ColumnMapping(
            name="total_amount",
            data_type="decimal",
            nullable=False
        )
    ]


@pytest.fixture
def sample_table(sample_columns):
    """Sample table for testing."""
    return TableMapping(
        name="orders",
        schema="ecommerce",
        table_type="table",
        columns=sample_columns,
        row_count=10000,
        primary_key_columns=["id"]
    )


@pytest.fixture
def sample_tables(sample_columns):
    """Multiple sample tables for testing."""
    customers_table = TableMapping(
        name="customers",
        schema="ecommerce",
        table_type="table",
        columns=[
            ColumnMapping(name="id", data_type="bigint", nullable=False, is_primary_key=True),
            ColumnMapping(name="email", data_type="varchar", nullable=False),
            ColumnMapping(name="created_at", data_type="timestamp", nullable=False)
        ],
        row_count=5000,
        primary_key_columns=["id"]
    )
    
    orders_table = TableMapping(
        name="orders",
        schema="ecommerce",
        table_type="table",
        columns=sample_columns,
        row_count=10000,
        primary_key_columns=["id"]
    )
    
    return [customers_table, orders_table]


@pytest.fixture
def greenfield_context():
    """Greenfield project context (no existing models)."""
    return ProjectContext(
        is_incremental=False,
        existing_models=[],
        existing_sources=[],
        naming_conventions={"staging": "stg_", "intermediate": "int_", "mart": "fct_"},
        warehouse_type="postgresql",
        project_name="test_dbt_project"
    )


@pytest.fixture
def brownfield_context():
    """Brownfield project context (existing models)."""
    return ProjectContext(
        is_incremental=True,
        existing_models=["stg_customers", "stg_products", "fct_sales"],
        existing_sources=["ecommerce", "marketing"],
        naming_conventions={"staging": "stg_", "intermediate": "int_", "mart": "fct_"},
        warehouse_type="postgresql",
        project_name="existing_dbt_project"
    )


@pytest.fixture
def sample_execution_plan():
    """Sample execution plan for testing."""
    return ExecutionPlan(
        strategy="greenfield",
        actions=[
            PlanAction(
                step_id=1,
                type="create_source",
                name="ecommerce",
                file_path="models/staging/ecommerce/sources.yml",
                description="Create source definition for ecommerce schema",
                dependencies=[]
            ),
            PlanAction(
                step_id=2,
                type="create_staging",
                name="stg_ecommerce_customers",
                file_path="models/staging/ecommerce/stg_ecommerce_customers.sql",
                description="Create staging model for customers table",
                dependencies=["ecommerce"]
            ),
            PlanAction(
                step_id=3,
                type="create_staging",
                name="stg_ecommerce_orders",
                file_path="models/staging/ecommerce/stg_ecommerce_orders.sql",
                description="Create staging model for orders table",
                dependencies=["ecommerce"]
            ),
            PlanAction(
                step_id=4,
                type="create_mart",
                name="fct_orders",
                file_path="models/marts/fct_orders.sql",
                description="Create fact table for orders with customer info",
                dependencies=["stg_ecommerce_customers", "stg_ecommerce_orders"]
            )
        ]
    )


# Test Pydantic Models
class TestPydanticModels:
    """Test the Pydantic models for execution planning."""
    
    def test_project_context_defaults(self):
        """Test ProjectContext with default values."""
        context = ProjectContext()
        assert context.is_incremental is False
        assert context.existing_models == []
        assert context.existing_sources == []
        assert context.warehouse_type == "postgresql"
        assert context.project_name == "my_dbt_project"
    
    def test_project_context_custom_values(self, brownfield_context):
        """Test ProjectContext with custom values."""
        assert brownfield_context.is_incremental is True
        assert len(brownfield_context.existing_models) == 3
        assert "stg_customers" in brownfield_context.existing_models
        assert brownfield_context.warehouse_type == "postgresql"
    
    def test_plan_action_creation(self):
        """Test PlanAction creation."""
        action = PlanAction(
            step_id=1,
            type="create_source",
            name="test_source",
            file_path="models/staging/test/sources.yml",
            description="Test source",
            dependencies=[]
        )
        assert action.step_id == 1
        assert action.type == "create_source"
        assert action.name == "test_source"
        assert action.dependencies == []
    
    def test_plan_action_with_dependencies(self):
        """Test PlanAction with dependencies."""
        action = PlanAction(
            step_id=2,
            type="create_staging",
            name="stg_test",
            file_path="models/staging/test/stg_test.sql",
            description="Staging model",
            dependencies=["test_source"]
        )
        assert len(action.dependencies) == 1
        assert "test_source" in action.dependencies
    
    def test_execution_plan_creation(self, sample_execution_plan):
        """Test ExecutionPlan creation."""
        assert sample_execution_plan.strategy == "greenfield"
        assert len(sample_execution_plan.actions) == 4
        assert sample_execution_plan.actions[0].type == "create_source"
        assert sample_execution_plan.actions[-1].type == "create_mart"
    
    def test_execution_plan_json_serialization(self, sample_execution_plan):
        """Test ExecutionPlan can be serialized to JSON."""
        json_str = sample_execution_plan.model_dump_json()
        parsed = json.loads(json_str)
        assert parsed["strategy"] == "greenfield"
        assert len(parsed["actions"]) == 4
    
    def test_execution_plan_json_deserialization(self, sample_execution_plan):
        """Test ExecutionPlan can be deserialized from JSON."""
        json_str = sample_execution_plan.model_dump_json()
        reconstructed = ExecutionPlan.model_validate_json(json_str)
        assert reconstructed.strategy == sample_execution_plan.strategy
        assert len(reconstructed.actions) == len(sample_execution_plan.actions)
    
    def test_execution_plan_invalid_strategy(self):
        """Test ExecutionPlan rejects invalid strategy."""
        with pytest.raises(ValueError):
            ExecutionPlan(
                strategy="invalid_strategy",  # Should only be 'greenfield' or 'brownfield'
                actions=[]
            )
    
    def test_plan_action_invalid_type(self):
        """Test PlanAction rejects invalid action type."""
        with pytest.raises(ValueError):
            PlanAction(
                step_id=1,
                type="invalid_action",  # Should only be specific types
                name="test",
                file_path="test.sql",
                description="Test"
            )


class TestModelGenerationRequest:
    """Test ModelGenerationRequest with ProjectContext."""
    
    def test_request_without_context(self, sample_tables):
        """Test request without project context."""
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING]
        )
        assert request.context is None
    
    def test_request_with_greenfield_context(self, sample_tables, greenfield_context):
        """Test request with greenfield context."""
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING],
            context=greenfield_context
        )
        assert request.context is not None
        assert request.context.is_incremental is False
        assert len(request.context.existing_models) == 0
    
    def test_request_with_brownfield_context(self, sample_tables, brownfield_context):
        """Test request with brownfield context."""
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING, ModelType.MARTS],
            context=brownfield_context
        )
        assert request.context is not None
        assert request.context.is_incremental is True
        assert len(request.context.existing_models) > 0


# Test OpenAI Provider
class TestOpenAIExecutionPlan:
    """Test execution plan generation with OpenAI provider."""
    
    @pytest.mark.asyncio
    async def test_generate_execution_plan_greenfield(self, sample_tables, greenfield_context):
        """Test generating execution plan for greenfield project."""
        # Mock OpenAI API response
        mock_response = {
            "strategy": "greenfield",
            "actions": [
                {
                    "step_id": 1,
                    "type": "create_source",
                    "name": "ecommerce",
                    "file_path": "models/staging/ecommerce/sources.yml",
                    "description": "Create source definition",
                    "dependencies": []
                },
                {
                    "step_id": 2,
                    "type": "create_staging",
                    "name": "stg_ecommerce_customers",
                    "file_path": "models/staging/ecommerce/stg_ecommerce_customers.sql",
                    "description": "Staging model for customers",
                    "dependencies": ["ecommerce"]
                }
            ]
        }
        
        # Create provider with mocked client
        config = {"api_key": "test-key", "model": "gpt-4"}
        provider = OpenAIProvider(config)
        
        # Mock the API call
        mock_completion = MagicMock()
        mock_completion.choices = [MagicMock()]
        mock_completion.choices[0].message.content = json.dumps(mock_response)
        provider.client.chat.completions.create = AsyncMock(return_value=mock_completion)
        
        # Create request
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING],
            context=greenfield_context
        )
        
        # Generate plan
        plan = await provider.generate_execution_plan(request)
        
        # Assertions
        assert plan.strategy == "greenfield"
        assert len(plan.actions) == 2
        assert plan.actions[0].type == "create_source"
        assert plan.actions[1].type == "create_staging"
        assert plan.actions[1].dependencies == ["ecommerce"]
    
    @pytest.mark.asyncio
    async def test_generate_execution_plan_brownfield(self, sample_tables, brownfield_context):
        """Test generating execution plan for brownfield project."""
        mock_response = {
            "strategy": "brownfield",
            "actions": [
                {
                    "step_id": 1,
                    "type": "create_staging",
                    "name": "stg_ecommerce_orders",
                    "file_path": "models/staging/ecommerce/stg_ecommerce_orders.sql",
                    "description": "Add staging for new orders table",
                    "dependencies": ["ecommerce"]
                },
                {
                    "step_id": 2,
                    "type": "create_intermediate",
                    "name": "int_customer_orders",
                    "file_path": "models/intermediate/int_customer_orders.sql",
                    "description": "Join customers with new orders",
                    "dependencies": ["stg_customers", "stg_ecommerce_orders"]
                }
            ]
        }
        
        config = {"api_key": "test-key", "model": "gpt-4"}
        provider = OpenAIProvider(config)
        
        mock_completion = MagicMock()
        mock_completion.choices = [MagicMock()]
        mock_completion.choices[0].message.content = json.dumps(mock_response)
        provider.client.chat.completions.create = AsyncMock(return_value=mock_completion)
        
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING, ModelType.INTERMEDIATE],
            context=brownfield_context
        )
        
        plan = await provider.generate_execution_plan(request)
        
        assert plan.strategy == "brownfield"
        assert len(plan.actions) == 2
        assert plan.actions[1].type == "create_intermediate"
    
    @pytest.mark.asyncio
    async def test_generate_execution_plan_invalid_json(self, sample_tables, greenfield_context):
        """Test handling of invalid JSON response."""
        config = {"api_key": "test-key", "model": "gpt-4"}
        provider = OpenAIProvider(config)
        
        # Mock invalid JSON response
        mock_completion = MagicMock()
        mock_completion.choices = [MagicMock()]
        mock_completion.choices[0].message.content = "This is not JSON"
        provider.client.chat.completions.create = AsyncMock(return_value=mock_completion)
        
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING],
            context=greenfield_context
        )
        
        with pytest.raises(ValueError, match="Invalid JSON response"):
            await provider.generate_execution_plan(request)
    
    @pytest.mark.asyncio
    async def test_generate_execution_plan_empty_response(self, sample_tables, greenfield_context):
        """Test handling of empty response."""
        config = {"api_key": "test-key", "model": "gpt-4"}
        provider = OpenAIProvider(config)
        
        # Mock empty response
        mock_completion = MagicMock()
        mock_completion.choices = [MagicMock()]
        mock_completion.choices[0].message.content = None
        provider.client.chat.completions.create = AsyncMock(return_value=mock_completion)
        
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING],
            context=greenfield_context
        )
        
        with pytest.raises(ValueError, match="Empty response"):
            await provider.generate_execution_plan(request)


# Test Anthropic Provider
class TestAnthropicExecutionPlan:
    """Test execution plan generation with Anthropic provider."""
    
    @pytest.mark.asyncio
    async def test_generate_execution_plan(self, sample_tables, greenfield_context):
        """Test generating execution plan with Anthropic."""
        mock_response = {
            "strategy": "greenfield",
            "actions": [
                {
                    "step_id": 1,
                    "type": "create_source",
                    "name": "ecommerce",
                    "file_path": "models/staging/ecommerce/sources.yml",
                    "description": "Create source definition",
                    "dependencies": []
                }
            ]
        }
        
        config = {"api_key": "test-key", "model": "claude-3-sonnet-20240229"}
        provider = AnthropicProvider(config)
        
        # Mock Anthropic API response
        mock_message = MagicMock()
        mock_content = MagicMock()
        mock_content.text = json.dumps(mock_response)
        mock_message.content = [mock_content]
        provider.client.messages.create = AsyncMock(return_value=mock_message)
        
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING],
            context=greenfield_context
        )
        
        plan = await provider.generate_execution_plan(request)
        
        assert plan.strategy == "greenfield"
        assert len(plan.actions) >= 1


# Test Gemini Provider
class TestGeminiExecutionPlan:
    """Test execution plan generation with Gemini provider."""
    
    @pytest.mark.asyncio
    async def test_generate_execution_plan(self, sample_tables, greenfield_context):
        """Test generating execution plan with Gemini."""
        mock_response = {
            "strategy": "greenfield",
            "actions": [
                {
                    "step_id": 1,
                    "type": "create_source",
                    "name": "ecommerce",
                    "file_path": "models/staging/ecommerce/sources.yml",
                    "description": "Create source definition",
                    "dependencies": []
                }
            ]
        }
        
        config = {"api_key": "test-key", "model": "gemini-2.5-flash"}
        provider = GeminiProvider(config)
        
        # Mock Gemini API response
        mock_response_obj = MagicMock()
        mock_response_obj.text = json.dumps(mock_response)
        provider.model.generate_content = MagicMock(return_value=mock_response_obj)
        
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING],
            context=greenfield_context
        )
        
        plan = await provider.generate_execution_plan(request)
        
        assert plan.strategy == "greenfield"
        assert len(plan.actions) >= 1
    
    @pytest.mark.asyncio
    async def test_generate_execution_plan_with_markdown_json(self, sample_tables, greenfield_context):
        """Test handling of JSON wrapped in markdown code blocks."""
        mock_response_data = {
            "strategy": "greenfield",
            "actions": [
                {
                    "step_id": 1,
                    "type": "create_source",
                    "name": "ecommerce",
                    "file_path": "models/staging/ecommerce/sources.yml",
                    "description": "Create source definition",
                    "dependencies": []
                }
            ]
        }
        
        config = {"api_key": "test-key", "model": "gemini-2.5-flash"}
        provider = GeminiProvider(config)
        
        # Mock response with markdown wrapper
        mock_response_obj = MagicMock()
        mock_response_obj.text = f"```json\n{json.dumps(mock_response_data)}\n```"
        provider.model.generate_content = MagicMock(return_value=mock_response_obj)
        
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING],
            context=greenfield_context
        )
        
        plan = await provider.generate_execution_plan(request)
        
        assert plan.strategy == "greenfield"
        assert len(plan.actions) >= 1


# Integration-like tests (can be skipped if no API keys available)
class TestExecutionPlanIntegration:
    """Integration tests for execution plan generation (requires API keys)."""
    
    @pytest.mark.skip(reason="Requires valid API key")
    @pytest.mark.asyncio
    async def test_real_openai_execution_plan(self, sample_tables, greenfield_context):
        """Test with real OpenAI API (skipped by default)."""
        import os
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            pytest.skip("OPENAI_API_KEY not set")
        
        config = {"api_key": api_key, "model": "gpt-4"}
        provider = OpenAIProvider(config)
        
        request = ModelGenerationRequest(
            tables=sample_tables,
            model_types=[ModelType.STAGING],
            context=greenfield_context,
            naming_convention="Standard dbt conventions"
        )
        
        plan = await provider.generate_execution_plan(request)
        
        # Verify structure
        assert plan.strategy in ["greenfield", "brownfield"]
        assert len(plan.actions) > 0
        assert all(action.step_id > 0 for action in plan.actions)
        assert all(action.type in ["create_source", "create_staging", "create_intermediate", "create_mart"] 
                   for action in plan.actions)
