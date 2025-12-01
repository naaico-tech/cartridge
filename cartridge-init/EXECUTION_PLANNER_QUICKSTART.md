# Quick Start: Using the Execution Planner

## Overview

The execution planner generates a structured plan for integrating new database schemas into dbt projects. It supports both greenfield (new project) and brownfield (existing project) scenarios.

## Basic Usage

### 1. Import Required Classes

```python
from cartridge.ai.base import (
    ModelGenerationRequest,
    ProjectContext,
    TableMapping,
    ColumnMapping,
    ModelType
)
from cartridge.ai.factory import create_ai_provider
```

### 2. Prepare Your Data

```python
# Define columns
columns = [
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
    # ... more columns
]

# Define table
table = TableMapping(
    name="orders",
    schema="ecommerce",
    table_type="table",
    columns=columns,
    row_count=10000
)
```

### 3. Create Project Context (Optional - for Brownfield)

```python
# For existing projects
context = ProjectContext(
    is_incremental=True,  # Adding to existing project
    existing_models=["stg_customers", "stg_products", "fct_sales"],
    existing_sources=["ecommerce", "marketing"],
    naming_conventions={
        "staging": "stg_",
        "intermediate": "int_",
        "mart": "fct_"
    },
    warehouse_type="postgresql",
    project_name="my_dbt_project"
)

# For new projects
context = ProjectContext()  # All defaults = greenfield
```

### 4. Create Request

```python
request = ModelGenerationRequest(
    tables=[table],  # List of tables to integrate
    model_types=[ModelType.STAGING, ModelType.MARTS],
    context=context,  # Optional
    naming_convention="Standard dbt conventions",  # Optional
    target_warehouse="postgresql"  # Default
)
```

### 5. Generate Execution Plan

```python
# Create AI provider
provider = create_ai_provider(
    provider_type="openai",
    config={"api_key": "your-api-key"}
)

# Generate plan
plan = await provider.generate_execution_plan(request)

# Access plan details
print(f"Strategy: {plan.strategy}")  # "greenfield" or "brownfield"
print(f"Actions: {len(plan.actions)}")

for action in plan.actions:
    print(f"Step {action.step_id}: {action.type}")
    print(f"  Name: {action.name}")
    print(f"  File: {action.file_path}")
    print(f"  Dependencies: {action.dependencies}")
```

## Example Output

```json
{
  "strategy": "brownfield",
  "actions": [
    {
      "step_id": 1,
      "type": "create_staging",
      "name": "stg_ecommerce_orders",
      "file_path": "models/staging/ecommerce/stg_ecommerce_orders.sql",
      "description": "Staging model for orders table",
      "dependencies": ["ecommerce"]
    },
    {
      "step_id": 2,
      "type": "create_intermediate",
      "name": "int_customer_orders",
      "file_path": "models/intermediate/int_customer_orders.sql",
      "description": "Join customers with orders",
      "dependencies": ["stg_customers", "stg_ecommerce_orders"]
    },
    {
      "step_id": 3,
      "type": "create_mart",
      "name": "fct_orders",
      "file_path": "models/marts/fct_orders.sql",
      "description": "Fact table for orders",
      "dependencies": ["int_customer_orders"]
    }
  ]
}
```

## Greenfield vs Brownfield

### Greenfield (New Project)
- `context.is_incremental = False`
- No existing models or sources
- Creates everything from scratch
- Generates source definitions, staging, and marts

```python
context = ProjectContext()  # Defaults to greenfield
```

### Brownfield (Existing Project)
- `context.is_incremental = True`
- Has existing models and sources
- Checks for duplicates
- Integrates with existing lineage

```python
context = ProjectContext(
    is_incremental=True,
    existing_models=["stg_customers"],
    existing_sources=["ecommerce"]
)
```

## Action Types

| Type | Description | Example File Path |
|------|-------------|-------------------|
| `create_source` | Create source YAML definition | `models/staging/ecommerce/sources.yml` |
| `create_staging` | Create staging model | `models/staging/ecommerce/stg_orders.sql` |
| `create_intermediate` | Create intermediate model | `models/intermediate/int_customer_orders.sql` |
| `create_mart` | Create fact or dimension | `models/marts/fct_orders.sql` |

## Supported AI Providers

### OpenAI (GPT-4)
```python
provider = create_ai_provider("openai", {
    "api_key": "sk-...",
    "model": "gpt-4",  # or "gpt-3.5-turbo"
    "temperature": 0.1
})
```

### Anthropic (Claude)
```python
provider = create_ai_provider("anthropic", {
    "api_key": "sk-ant-...",
    "model": "claude-3-sonnet-20240229",
    "temperature": 0.1
})
```

### Google (Gemini)
```python
provider = create_ai_provider("gemini", {
    "api_key": "...",
    "model": "gemini-2.5-flash",
    "temperature": 0.1
})
```

## Error Handling

```python
try:
    plan = await provider.generate_execution_plan(request)
except ValueError as e:
    # Invalid JSON response from AI
    print(f"JSON parsing error: {e}")
except Exception as e:
    # General error
    print(f"Error generating plan: {e}")
```

## Testing

```python
# Mock the AI response for testing
from unittest.mock import AsyncMock, MagicMock
import json

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

# Mock OpenAI
provider.client.chat.completions.create = AsyncMock(
    return_value=MagicMock(
        choices=[MagicMock(
            message=MagicMock(
                content=json.dumps(mock_response)
            )
        )]
    )
)

plan = await provider.generate_execution_plan(request)
assert plan.strategy == "greenfield"
```

## Advanced: Custom Prompts

You can customize prompts by modifying `src/cartridge/ai/prompts.py`:

```python
# Example: Add custom instructions to planner
PLANNER_SYSTEM_PROMPT = """
You are the Principal Data Architect...

CUSTOM INSTRUCTIONS:
- Always use snake_case for model names
- Prefix all intermediate models with "int_v2_"
- Add metadata tags for data governance

...
"""
```

## Tips

1. **Always specify context** for brownfield projects to avoid duplicates
2. **Use low temperature** (0.1) for consistent planning
3. **Validate dependencies** before execution
4. **Check file paths** match your dbt project structure
5. **Review generated plans** before executing
6. **Log all API calls** for debugging
7. **Handle retries** for transient API failures

## Common Issues

### Issue: AI returns non-JSON response
**Solution**: Use `response_format={"type": "json_object"}` for OpenAI, or parse markdown-wrapped JSON for Gemini.

### Issue: Duplicate source definitions
**Solution**: Ensure `existing_sources` in `ProjectContext` includes all current sources.

### Issue: Invalid dependencies
**Solution**: Validate that all dependencies in `action.dependencies` exist in previous steps or `existing_models`.

### Issue: Naming conflicts
**Solution**: Provide clear `naming_convention` string in request.

## Next Steps

After generating a plan:
1. Review the `ExecutionPlan` manually
2. Validate file paths against your dbt project structure
3. Check dependencies are correct
4. Execute actions sequentially (step 1, then 2, etc.)
5. Validate generated SQL before committing

## Need Help?

- Check `EXECUTION_PLAN_IMPLEMENTATION.md` for architecture details
- Review tests in `tests/unit/test_execution_plan.py` for examples
- See `src/cartridge/ai/prompts.py` for prompt templates
- Refer to `src/cartridge/ai/base.py` for model definitions
