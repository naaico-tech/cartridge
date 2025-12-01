"""Unit tests for AI prompts module."""

import pytest
from cartridge.ai.prompts import (
    PLANNER_SYSTEM_PROMPT, PLANNER_USER_PROMPT,
    STAGING_SYSTEM_PROMPT, STAGING_USER_PROMPT,
    INTERMEDIATE_SYSTEM_PROMPT, INTERMEDIATE_USER_PROMPT,
    MART_SYSTEM_PROMPT, MART_USER_PROMPT,
    TEST_GENERATION_PROMPT, DOCUMENTATION_GENERATION_PROMPT
)


class TestPromptContent:
    """Test that prompts contain expected content."""
    
    def test_planner_system_prompt_contains_key_sections(self):
        """Test that planner system prompt has required sections."""
        assert "Principal Data Architect" in PLANNER_SYSTEM_PROMPT
        assert "CORE RESPONSIBILITIES" in PLANNER_SYSTEM_PROMPT
        assert "Duplicate Detection" in PLANNER_SYSTEM_PROMPT
        assert "Lineage Design" in PLANNER_SYSTEM_PROMPT
        assert "Naming Standards" in PLANNER_SYSTEM_PROMPT
        assert "Atomic Actions" in PLANNER_SYSTEM_PROMPT
        assert "OUTPUT FORMAT" in PLANNER_SYSTEM_PROMPT
        assert "ExecutionPlan" in PLANNER_SYSTEM_PROMPT
    
    def test_planner_system_prompt_specifies_action_types(self):
        """Test that planner prompt specifies all action types."""
        assert "create_source" in PLANNER_SYSTEM_PROMPT
        assert "create_staging" in PLANNER_SYSTEM_PROMPT
        assert "create_intermediate" in PLANNER_SYSTEM_PROMPT
        assert "create_mart" in PLANNER_SYSTEM_PROMPT
    
    def test_planner_system_prompt_specifies_strategies(self):
        """Test that planner prompt specifies greenfield/brownfield."""
        assert "greenfield" in PLANNER_SYSTEM_PROMPT
        assert "brownfield" in PLANNER_SYSTEM_PROMPT
    
    def test_planner_user_prompt_has_placeholders(self):
        """Test that planner user prompt has all required placeholders."""
        assert "{project_name}" in PLANNER_USER_PROMPT
        assert "{warehouse_type}" in PLANNER_USER_PROMPT
        assert "{naming_convention}" in PLANNER_USER_PROMPT
        assert "{existing_sources}" in PLANNER_USER_PROMPT
        assert "{existing_models}" in PLANNER_USER_PROMPT
        assert "{schema_name}" in PLANNER_USER_PROMPT
        assert "{new_tables_metadata}" in PLANNER_USER_PROMPT
    
    def test_staging_prompts_contain_key_concepts(self):
        """Test staging prompts contain dbt best practices."""
        assert "staging models" in STAGING_SYSTEM_PROMPT.lower()
        assert "source()" in STAGING_SYSTEM_PROMPT
        assert "naming conventions" in STAGING_SYSTEM_PROMPT.lower()
        
        assert "{table_name}" in STAGING_USER_PROMPT
        assert "{schema_name}" in STAGING_USER_PROMPT
        assert "{columns_list}" in STAGING_USER_PROMPT
        assert "{model_name}" in STAGING_USER_PROMPT
    
    def test_intermediate_prompts_contain_key_concepts(self):
        """Test intermediate prompts mention joins and business logic."""
        assert "intermediate models" in INTERMEDIATE_SYSTEM_PROMPT.lower()
        assert "business logic" in INTERMEDIATE_SYSTEM_PROMPT.lower()
        assert "ref()" in INTERMEDIATE_SYSTEM_PROMPT
        assert "join" in INTERMEDIATE_SYSTEM_PROMPT.lower()
        
        assert "{business_logic}" in INTERMEDIATE_USER_PROMPT
        assert "{source_models}" in INTERMEDIATE_USER_PROMPT
        assert "{relationships}" in INTERMEDIATE_USER_PROMPT
    
    def test_mart_prompts_contain_key_concepts(self):
        """Test mart prompts mention facts, dimensions, and analytics."""
        assert "mart models" in MART_SYSTEM_PROMPT.lower()
        assert "facts and dimensions" in MART_SYSTEM_PROMPT.lower() or "fact" in MART_SYSTEM_PROMPT.lower()
        assert "ref()" in MART_SYSTEM_PROMPT
        
        assert "{model_type}" in MART_USER_PROMPT
        assert "{model_purpose}" in MART_USER_PROMPT
        assert "{source_models}" in MART_USER_PROMPT
    
    def test_test_generation_prompt_has_placeholders(self):
        """Test test generation prompt has required fields."""
        assert "{model_name}" in TEST_GENERATION_PROMPT
        assert "{model_type}" in TEST_GENERATION_PROMPT
        assert "{columns_info}" in TEST_GENERATION_PROMPT
        assert "not_null" in TEST_GENERATION_PROMPT
        assert "unique" in TEST_GENERATION_PROMPT
        assert "relationship" in TEST_GENERATION_PROMPT
    
    def test_documentation_prompt_has_placeholders(self):
        """Test documentation prompt has required fields."""
        assert "{model_name}" in DOCUMENTATION_GENERATION_PROMPT
        assert "{model_type}" in DOCUMENTATION_GENERATION_PROMPT
        assert "{model_purpose}" in DOCUMENTATION_GENERATION_PROMPT
        assert "{columns_info}" in DOCUMENTATION_GENERATION_PROMPT


class TestPromptFormatting:
    """Test that prompts can be formatted correctly."""
    
    def test_planner_user_prompt_formats_correctly(self):
        """Test that planner user prompt can be formatted."""
        formatted = PLANNER_USER_PROMPT.format(
            project_name="test_project",
            warehouse_type="postgresql",
            naming_convention="Standard dbt",
            existing_sources="source1, source2",
            existing_models="model1, model2",
            schema_name="public",
            new_tables_metadata="table1\ntable2"
        )
        
        assert "test_project" in formatted
        assert "postgresql" in formatted
        assert "source1, source2" in formatted
        assert "model1, model2" in formatted
        assert "public" in formatted
        assert "{" not in formatted  # No unformatted placeholders
    
    def test_staging_user_prompt_formats_correctly(self):
        """Test that staging user prompt can be formatted."""
        formatted = STAGING_USER_PROMPT.format(
            table_name="customers",
            schema_name="public",
            warehouse_type="postgresql",
            columns_list="id, name, email",
            model_name="stg_customers",
            naming_convention="stg_"
        )
        
        assert "customers" in formatted
        assert "stg_customers" in formatted
        assert "{" not in formatted
    
    def test_intermediate_user_prompt_formats_correctly(self):
        """Test that intermediate user prompt can be formatted."""
        formatted = INTERMEDIATE_USER_PROMPT.format(
            business_logic="Join customers with orders",
            source_models="stg_customers, stg_orders",
            warehouse_type="postgresql",
            relationships="customer_id -> customers.id",
            model_name="int_customer_orders",
            naming_convention="int_"
        )
        
        assert "Join customers with orders" in formatted
        assert "int_customer_orders" in formatted
        assert "{" not in formatted
    
    def test_mart_user_prompt_formats_correctly(self):
        """Test that mart user prompt can be formatted."""
        formatted = MART_USER_PROMPT.format(
            model_type="fact",
            model_purpose="Track all customer orders",
            source_models="int_customer_orders",
            warehouse_type="postgresql",
            business_requirements="Daily order metrics",
            model_name="fct_orders",
            naming_convention="fct_"
        )
        
        assert "fact" in formatted
        assert "fct_orders" in formatted
        assert "{" not in formatted


class TestPromptLength:
    """Test that prompts are reasonable length."""
    
    def test_prompts_not_empty(self):
        """Test that all prompts have content."""
        prompts = [
            PLANNER_SYSTEM_PROMPT, PLANNER_USER_PROMPT,
            STAGING_SYSTEM_PROMPT, STAGING_USER_PROMPT,
            INTERMEDIATE_SYSTEM_PROMPT, INTERMEDIATE_USER_PROMPT,
            MART_SYSTEM_PROMPT, MART_USER_PROMPT,
            TEST_GENERATION_PROMPT, DOCUMENTATION_GENERATION_PROMPT
        ]
        
        for prompt in prompts:
            assert len(prompt) > 100, "Prompt should have substantial content"
            assert len(prompt.strip()) == len(prompt), "Prompt should not have leading/trailing whitespace"
    
    def test_prompts_not_too_long(self):
        """Test that prompts are not excessively long."""
        prompts = [
            PLANNER_SYSTEM_PROMPT, PLANNER_USER_PROMPT,
            STAGING_SYSTEM_PROMPT, STAGING_USER_PROMPT,
            INTERMEDIATE_SYSTEM_PROMPT, INTERMEDIATE_USER_PROMPT,
            MART_SYSTEM_PROMPT, MART_USER_PROMPT
        ]
        
        for prompt in prompts:
            # Reasonable upper limit for a single prompt
            assert len(prompt) < 5000, "Prompt may be too long and costly"
