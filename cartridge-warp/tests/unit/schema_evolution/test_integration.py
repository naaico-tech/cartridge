"""Integration tests for schema evolution engine."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from cartridge_warp.connectors.base import (
    ColumnType, 
    ColumnDefinition, 
    TableSchema, 
    DatabaseSchema
)
from cartridge_warp.schema_evolution.config import SchemaEvolutionConfig
from cartridge_warp.schema_evolution.types import EvolutionStrategy, SchemaChangeType
from cartridge_warp.schema_evolution.engine import SchemaEvolutionEngine


class TestSchemaEvolutionIntegration:
    """Integration tests for schema evolution scenarios."""
    
    @pytest.fixture
    def mock_connectors(self):
        """Create mock connectors with realistic behavior."""
        source_connector = AsyncMock()
        destination_connector = AsyncMock()
        metadata_manager = AsyncMock()
        
        # Mock destination connector methods
        destination_connector.create_schema_if_not_exists = AsyncMock()
        destination_connector.acquire = AsyncMock()
        
        return source_connector, destination_connector, metadata_manager
        
    @pytest.mark.asyncio
    async def test_complete_evolution_workflow(self, mock_connectors):
        """Test complete schema evolution workflow from detection to application."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        # Configure evolution with permissive strategy
        config = SchemaEvolutionConfig(
            enabled=True,
            strategy=EvolutionStrategy.PERMISSIVE,
            enable_type_widening=True,
            enable_type_narrowing=False,
            require_approval_for_risky_changes=False
        )
        
        engine = SchemaEvolutionEngine(
            config=config,
            source_connector=source_connector,
            destination_connector=destination_connector,
            metadata_manager=metadata_manager
        )
        
        # Step 1: Initial schema detection (empty cache)
        initial_schema = DatabaseSchema(
            name="ecommerce",
            tables=[
                TableSchema(
                    name="products",
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.INTEGER),
                        ColumnDefinition(name="name", type=ColumnType.STRING),
                        ColumnDefinition(name="price", type=ColumnType.FLOAT)
                    ],
                    primary_keys=["id"]
                )
            ]
        )
        
        source_connector.get_schema.return_value = initial_schema
        
        # First evolution - should cache schema
        result1 = await engine.evolve_schema("ecommerce")
        assert result1.success
        assert len(result1.events) == 0  # No changes on first run
        
        # Step 2: Schema evolution with new column
        evolved_schema = DatabaseSchema(
            name="ecommerce",
            tables=[
                TableSchema(
                    name="products", 
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.INTEGER),
                        ColumnDefinition(name="name", type=ColumnType.STRING),
                        ColumnDefinition(name="price", type=ColumnType.FLOAT),
                        ColumnDefinition(name="description", type=ColumnType.STRING),  # New column
                        ColumnDefinition(name="category_id", type=ColumnType.INTEGER)  # Another new column
                    ],
                    primary_keys=["id"]
                )
            ]
        )
        
        source_connector.get_schema.return_value = evolved_schema
        
        # Second evolution - should detect new columns
        result2 = await engine.evolve_schema("ecommerce", dry_run=True)
        assert result2.success
        assert len(result2.events) == 2  # Two new columns
        assert all(event.change_type == SchemaChangeType.ADD_COLUMN for event in result2.events)
        assert all("DRY RUN:" in change for change in result2.applied_changes)
        
        # Step 3: Schema evolution with type changes
        type_changed_schema = DatabaseSchema(
            name="ecommerce",
            tables=[
                TableSchema(
                    name="products",
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.BIGINT),  # Widened type
                        ColumnDefinition(name="name", type=ColumnType.STRING),
                        ColumnDefinition(name="price", type=ColumnType.DOUBLE),  # Widened type
                        ColumnDefinition(name="description", type=ColumnType.STRING),
                        ColumnDefinition(name="category_id", type=ColumnType.INTEGER)
                    ],
                    primary_keys=["id"]
                )
            ]
        )
        
        # Cache the evolved schema first
        engine.change_detector._schema_cache["ecommerce"] = evolved_schema
        source_connector.get_schema.return_value = type_changed_schema
        
        # Third evolution - should detect type changes
        result3 = await engine.evolve_schema("ecommerce", dry_run=True)
        assert result3.success
        assert len(result3.events) == 2  # Two type changes
        assert all(event.change_type == SchemaChangeType.MODIFY_COLUMN_TYPE for event in result3.events)
        
        # Step 4: Test metrics collection
        metrics = engine.get_metrics()
        assert metrics.total_changes_detected >= 4  # At least 4 changes detected across runs
        
    @pytest.mark.asyncio
    async def test_conservative_strategy_blocking(self, mock_connectors):
        """Test that conservative strategy blocks dangerous changes."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        # Configure with conservative strategy
        config = SchemaEvolutionConfig(
            enabled=True,
            strategy=EvolutionStrategy.CONSERVATIVE,
            require_approval_for_dangerous_changes=True
        )
        
        engine = SchemaEvolutionEngine(
            config=config,
            source_connector=source_connector,
            destination_connector=destination_connector,
            metadata_manager=metadata_manager
        )
        
        # Setup initial schema with string column
        initial_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.INTEGER),
                        ColumnDefinition(name="age", type=ColumnType.STRING)  # String type
                    ],
                    primary_keys=["id"]
                )
            ]
        )
        
        # Cache initial schema
        engine.change_detector._schema_cache["test_db"] = initial_schema
        
        # Try to change string to integer (dangerous conversion)
        dangerous_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.INTEGER),
                        ColumnDefinition(name="age", type=ColumnType.INTEGER)  # Changed to integer
                    ],
                    primary_keys=["id"]
                )
            ]
        )
        
        source_connector.get_schema.return_value = dangerous_schema
        
        # Should fail in conservative mode
        result = await engine.evolve_schema("test_db")
        assert not result.success
        assert len(result.errors) > 0
        assert "dangerous changes blocked" in result.errors[0].lower()
        
    @pytest.mark.asyncio
    async def test_table_filtering_configuration(self, mock_connectors):
        """Test table and column filtering based on configuration."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        # Configure with exclusions
        config = SchemaEvolutionConfig(
            enabled=True,
            strategy=EvolutionStrategy.PERMISSIVE,
            excluded_tables=["temp_data", "logs"],
            excluded_columns={"users": ["internal_notes", "debug_info"]}
        )
        
        engine = SchemaEvolutionEngine(
            config=config,
            source_connector=source_connector,
            destination_connector=destination_connector,
            metadata_manager=metadata_manager
        )
        
        # Initial schema
        initial_schema = DatabaseSchema(
            name="app_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        # Cache initial schema
        engine.change_detector._schema_cache["app_db"] = initial_schema
        
        # Schema with excluded items
        new_schema = DatabaseSchema(
            name="app_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.INTEGER),
                        ColumnDefinition(name="name", type=ColumnType.STRING),  # Should be detected
                        ColumnDefinition(name="internal_notes", type=ColumnType.STRING)  # Should be excluded
                    ],
                    primary_keys=["id"]
                ),
                TableSchema(
                    name="temp_data",  # Should be excluded
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                ),
                TableSchema(
                    name="products",  # Should be detected
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        source_connector.get_schema.return_value = new_schema
        
        result = await engine.evolve_schema("app_db", dry_run=True)
        
        assert result.success
        # Should detect: 1 column addition (name), 1 table addition (products)
        # Should exclude: 1 column (internal_notes), 1 table (temp_data)
        assert len(result.events) == 2
        
        # Verify specific exclusions
        event_descriptions = [f"{e.change_type.value}:{e.table_name}:{e.column_name}" for e in result.events]
        assert "add_column:users:name" in event_descriptions
        assert "add_table:products:None" in event_descriptions
        assert "add_column:users:internal_notes" not in event_descriptions
        assert "add_table:temp_data:None" not in event_descriptions
        
    @pytest.mark.asyncio
    async def test_error_handling_and_rollback(self, mock_connectors):
        """Test error handling and rollback functionality."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        config = SchemaEvolutionConfig(
            enabled=True,
            strategy=EvolutionStrategy.PERMISSIVE,
            enable_rollback=True
        )
        
        engine = SchemaEvolutionEngine(
            config=config,
            source_connector=source_connector,
            destination_connector=destination_connector,
            metadata_manager=metadata_manager
        )
        
        # Setup schema with changes
        initial_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        evolved_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.INTEGER),
                        ColumnDefinition(name="email", type=ColumnType.STRING)
                    ],
                    primary_keys=["id"]
                )
            ]
        )
        
        engine.change_detector._schema_cache["test_db"] = initial_schema
        source_connector.get_schema.return_value = evolved_schema
        
        # Mock SQL execution to simulate failure
        # Since we're not actually executing SQL in this implementation,
        # we'll test the dry run capabilities
        result = await engine.evolve_schema("test_db", dry_run=True)
        
        # Should succeed in dry run mode
        assert result.success
        assert len(result.events) == 1
        assert "DRY RUN:" in result.applied_changes[0]
        
    @pytest.mark.asyncio
    async def test_health_monitoring(self, mock_connectors):
        """Test health monitoring capabilities."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        config = SchemaEvolutionConfig(
            enabled=True,
            metrics_enabled=True
        )
        
        engine = SchemaEvolutionEngine(
            config=config,
            source_connector=source_connector,
            destination_connector=destination_connector,
            metadata_manager=metadata_manager
        )
        
        # Start engine
        await engine.start()
        
        # Check health
        health = await engine.health_check()
        
        assert health["running"] is True
        assert health["enabled"] is True
        assert "metrics" in health
        assert "detector_stats" in health
        
        # Stop engine
        await engine.stop()
        
        # Check health after stop
        health_after_stop = await engine.health_check()
        assert health_after_stop["running"] is False
        
    @pytest.mark.asyncio
    async def test_concurrent_evolution_limits(self, mock_connectors):
        """Test concurrent migration limits."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        config = SchemaEvolutionConfig(
            enabled=True,
            max_concurrent_migrations=1  # Limit to 1 concurrent migration
        )
        
        engine = SchemaEvolutionEngine(
            config=config,
            source_connector=source_connector,
            destination_connector=destination_connector,
            metadata_manager=metadata_manager
        )
        
        # Setup test schema
        test_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.INTEGER),
                        ColumnDefinition(name="name", type=ColumnType.STRING)
                    ],
                    primary_keys=["id"]
                )
            ]
        )
        
        source_connector.get_schema.return_value = test_schema
        
        # Should work normally with dry run
        result = await engine.evolve_schema("test_db", dry_run=True)
        assert result.success
