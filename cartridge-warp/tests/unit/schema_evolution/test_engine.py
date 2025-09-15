"""Unit tests for schema evolution engine."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from cartridge_warp.connectors.base import ColumnType, ColumnDefinition, TableSchema, DatabaseSchema
from cartridge_warp.schema_evolution.config import SchemaEvolutionConfig
from cartridge_warp.schema_evolution.types import (
    ConversionRule, 
    ConversionSafety, 
    SchemaChangeType,
    EvolutionStrategy
)
from cartridge_warp.schema_evolution.type_converter import TypeConversionEngine
from cartridge_warp.schema_evolution.detector import SchemaChangeDetector
from cartridge_warp.schema_evolution.engine import SchemaEvolutionEngine


class TestTypeConversionEngine:
    """Test type conversion engine functionality."""
    
    def test_initialization(self):
        """Test type converter initialization."""
        converter = TypeConversionEngine()
        assert len(converter.conversion_rules) > 0
        
    def test_safe_widening_conversions(self):
        """Test safe type widening conversions."""
        converter = TypeConversionEngine()
        
        # Integer to bigint should be safe
        assert converter.can_convert(ColumnType.INTEGER, ColumnType.BIGINT)
        assert converter.get_conversion_safety(ColumnType.INTEGER, ColumnType.BIGINT) == ConversionSafety.SAFE
        
        # Float to double should be safe
        assert converter.can_convert(ColumnType.FLOAT, ColumnType.DOUBLE)
        assert converter.get_conversion_safety(ColumnType.FLOAT, ColumnType.DOUBLE) == ConversionSafety.SAFE
        
    def test_risky_narrowing_conversions(self):
        """Test risky type narrowing conversions."""
        converter = TypeConversionEngine()
        
        # Bigint to integer should be risky
        assert converter.can_convert(ColumnType.BIGINT, ColumnType.INTEGER)
        assert converter.get_conversion_safety(ColumnType.BIGINT, ColumnType.INTEGER) == ConversionSafety.RISKY
        
    def test_dangerous_conversions(self):
        """Test dangerous type conversions."""
        converter = TypeConversionEngine()
        
        # String to numeric should be dangerous
        assert converter.can_convert(ColumnType.STRING, ColumnType.INTEGER)
        assert converter.get_conversion_safety(ColumnType.STRING, ColumnType.INTEGER) == ConversionSafety.DANGEROUS
        
    def test_value_conversions(self):
        """Test actual value conversions."""
        converter = TypeConversionEngine()
        
        # Safe conversions
        assert converter.convert_value(42, ColumnType.INTEGER, ColumnType.BIGINT) == 42
        assert converter.convert_value(3.14, ColumnType.FLOAT, ColumnType.DOUBLE) == 3.14
        assert converter.convert_value(123, ColumnType.INTEGER, ColumnType.STRING) == "123"
        
        # None handling
        assert converter.convert_value(None, ColumnType.INTEGER, ColumnType.STRING) is None
        
    def test_batch_conversions(self):
        """Test batch value conversions."""
        converter = TypeConversionEngine()
        
        values = [1, 2, 3, None, 5]
        result = converter.batch_convert(values, ColumnType.INTEGER, ColumnType.STRING)
        expected = ["1", "2", "3", None, "5"]
        assert result == expected
        
    def test_data_loss_estimation(self):
        """Test data loss estimation."""
        converter = TypeConversionEngine()
        
        # Safe conversion should have 0% loss
        values = [1, 2, 3, 4, 5]
        loss = converter.estimate_data_loss(values, ColumnType.INTEGER, ColumnType.BIGINT)
        assert loss == 0.0
        
        # String to integer with invalid values should have loss
        string_values = ["1", "2", "invalid", "4", "not_a_number"]
        loss = converter.estimate_data_loss(string_values, ColumnType.STRING, ColumnType.INTEGER)
        assert loss > 0.0
        
    def test_custom_conversion_rules(self):
        """Test adding custom conversion rules."""
        converter = TypeConversionEngine()
        
        # Add custom rule
        custom_rule = ConversionRule(
            source_type=ColumnType.STRING,
            target_type=ColumnType.BOOLEAN,
            safety=ConversionSafety.SAFE,
            conversion_function=lambda x: x.lower() == "true" if isinstance(x, str) else bool(x)
        )
        
        converter.add_rule(custom_rule)
        
        # Test custom conversion
        assert converter.convert_value("true", ColumnType.STRING, ColumnType.BOOLEAN) is True
        assert converter.convert_value("false", ColumnType.STRING, ColumnType.BOOLEAN) is False


class TestSchemaChangeDetector:
    """Test schema change detection functionality."""
    
    def test_initialization(self):
        """Test detector initialization."""
        config = SchemaEvolutionConfig()
        detector = SchemaChangeDetector(config)
        assert detector.config == config
        
    @pytest.mark.asyncio
    async def test_table_addition_detection(self):
        """Test detection of new tables."""
        config = SchemaEvolutionConfig()
        detector = SchemaChangeDetector(config)
        
        # Previous schema with one table
        prev_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        # Current schema with additional table
        curr_schema = DatabaseSchema(
            name="test_db", 
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                ),
                TableSchema(
                    name="products",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        events = await detector.detect_changes("test_db", curr_schema, prev_schema)
        
        assert len(events) == 1
        assert events[0].change_type == SchemaChangeType.ADD_TABLE
        assert events[0].table_name == "products"
        assert events[0].safety_level == ConversionSafety.SAFE
        
    @pytest.mark.asyncio
    async def test_column_addition_detection(self):
        """Test detection of new columns."""
        config = SchemaEvolutionConfig(detect_column_additions=True)
        detector = SchemaChangeDetector(config)
        
        # Previous schema
        prev_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        # Current schema with additional column
        curr_schema = DatabaseSchema(
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
        
        events = await detector.detect_changes("test_db", curr_schema, prev_schema)
        
        assert len(events) == 1
        assert events[0].change_type == SchemaChangeType.ADD_COLUMN
        assert events[0].table_name == "users"
        assert events[0].column_name == "name"
        assert events[0].safety_level == ConversionSafety.SAFE
        
    @pytest.mark.asyncio
    async def test_type_change_detection(self):
        """Test detection of column type changes."""
        config = SchemaEvolutionConfig(detect_type_changes=True)
        detector = SchemaChangeDetector(config)
        
        # Previous schema
        prev_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        # Current schema with type change
        curr_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.BIGINT)],
                    primary_keys=["id"]
                )
            ]
        )
        
        events = await detector.detect_changes("test_db", curr_schema, prev_schema)
        
        assert len(events) == 1
        assert events[0].change_type == SchemaChangeType.MODIFY_COLUMN_TYPE
        assert events[0].table_name == "users"
        assert events[0].column_name == "id"
        assert events[0].safety_level == ConversionSafety.SAFE  # Safe widening
        
    @pytest.mark.asyncio
    async def test_dangerous_type_change_detection(self):
        """Test detection of dangerous type changes."""
        config = SchemaEvolutionConfig(detect_type_changes=True)
        detector = SchemaChangeDetector(config)
        
        # Previous schema
        prev_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="age", type=ColumnType.STRING)],
                    primary_keys=["id"]
                )
            ]
        )
        
        # Current schema with dangerous type change
        curr_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="age", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        events = await detector.detect_changes("test_db", curr_schema, prev_schema)
        
        assert len(events) == 1
        assert events[0].change_type == SchemaChangeType.MODIFY_COLUMN_TYPE
        assert events[0].safety_level == ConversionSafety.DANGEROUS
        assert events[0].requires_approval is True
        
    @pytest.mark.asyncio
    async def test_excluded_tables_filtering(self):
        """Test that excluded tables are ignored."""
        config = SchemaEvolutionConfig(excluded_tables=["temp_table"])
        detector = SchemaChangeDetector(config)
        
        prev_schema = DatabaseSchema(name="test_db", tables=[])
        curr_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="temp_table",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        events = await detector.detect_changes("test_db", curr_schema, prev_schema)
        
        # Should be empty because temp_table is excluded
        assert len(events) == 0
        
    @pytest.mark.asyncio 
    async def test_cache_functionality(self):
        """Test schema caching functionality."""
        config = SchemaEvolutionConfig()
        detector = SchemaChangeDetector(config)
        
        schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        # First detection should cache the schema
        events = await detector.detect_changes("test_db", schema)
        assert len(events) == 0  # No previous schema to compare
        
        # Check cache
        cached = detector.get_schema_cache("test_db")
        assert cached is not None
        assert cached.name == "test_db"
        assert len(cached.tables) == 1
        
        # Clear cache
        detector.clear_cache("test_db")
        assert detector.get_schema_cache("test_db") is None


class TestSchemaEvolutionEngine:
    """Test the main schema evolution engine."""
    
    @pytest.fixture
    def mock_connectors(self):
        """Create mock connectors for testing."""
        source_connector = AsyncMock()
        destination_connector = AsyncMock()
        metadata_manager = AsyncMock()
        
        return source_connector, destination_connector, metadata_manager
        
    @pytest.fixture
    def evolution_config(self):
        """Create test configuration."""
        return SchemaEvolutionConfig(
            enabled=True,
            strategy=EvolutionStrategy.CONSERVATIVE,
            detection_interval_seconds=60
        )
        
    @pytest.fixture
    def evolution_engine(self, evolution_config, mock_connectors):
        """Create evolution engine for testing."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        return SchemaEvolutionEngine(
            config=evolution_config,
            source_connector=source_connector,
            destination_connector=destination_connector,
            metadata_manager=metadata_manager
        )
        
    @pytest.mark.asyncio
    async def test_engine_initialization(self, evolution_engine):
        """Test engine initialization."""
        assert not evolution_engine.running
        assert evolution_engine.config.enabled
        assert evolution_engine.type_converter is not None
        assert evolution_engine.change_detector is not None
        assert evolution_engine.migrator is not None
        
    @pytest.mark.asyncio
    async def test_start_stop_engine(self, evolution_engine):
        """Test starting and stopping the engine."""
        # Start engine
        await evolution_engine.start()
        assert evolution_engine.running
        assert evolution_engine.evolution_task is not None
        
        # Stop engine
        await evolution_engine.stop()
        assert not evolution_engine.running
        
    @pytest.mark.asyncio
    async def test_disabled_engine(self, mock_connectors):
        """Test engine behavior when disabled."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        config = SchemaEvolutionConfig(enabled=False)
        engine = SchemaEvolutionEngine(
            config=config,
            source_connector=source_connector,
            destination_connector=destination_connector,
            metadata_manager=metadata_manager
        )
        
        await engine.start()
        assert not engine.running
        
    @pytest.mark.asyncio
    async def test_schema_evolution_no_changes(self, evolution_engine, mock_connectors):
        """Test schema evolution when no changes are detected."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        # Mock source schema
        test_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        source_connector.get_schema.return_value = test_schema
        
        # First call - should cache schema and return no changes
        result = await evolution_engine.evolve_schema("test_db")
        
        assert result.success
        assert len(result.events) == 0
        assert len(result.applied_changes) == 0
        assert len(result.errors) == 0
        
    @pytest.mark.asyncio
    async def test_schema_evolution_with_changes(self, evolution_engine, mock_connectors):
        """Test schema evolution with detected changes."""
        source_connector, destination_connector, metadata_manager = mock_connectors
        
        # Setup initial cached schema
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
        
        # Cache initial schema
        evolution_engine.change_detector._schema_cache["test_db"] = initial_schema
        
        # Mock current schema with changes
        current_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[
                        ColumnDefinition(name="id", type=ColumnType.INTEGER),
                        ColumnDefinition(name="name", type=ColumnType.STRING)  # New column
                    ],
                    primary_keys=["id"]
                )
            ]
        )
        
        source_connector.get_schema.return_value = current_schema
        
        # Test dry run
        result = await evolution_engine.evolve_schema("test_db", dry_run=True)
        
        assert result.success
        assert len(result.events) == 1
        assert result.events[0].change_type == SchemaChangeType.ADD_COLUMN
        assert "DRY RUN:" in result.applied_changes[0]
        
    @pytest.mark.asyncio
    async def test_schema_validation(self, evolution_engine):
        """Test schema change validation."""
        # Create proposed schema
        proposed_schema = DatabaseSchema(
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
        
        # Mock source connector for current schema
        current_schema = DatabaseSchema(
            name="test_db",
            tables=[
                TableSchema(
                    name="users",
                    columns=[ColumnDefinition(name="id", type=ColumnType.INTEGER)],
                    primary_keys=["id"]
                )
            ]
        )
        
        evolution_engine.source_connector.get_schema.return_value = current_schema
        
        result = await evolution_engine.validate_schema_changes("test_db", proposed_schema)
        
        assert result.success
        assert len(result.events) == 1
        assert result.events[0].change_type == SchemaChangeType.ADD_COLUMN
        
    @pytest.mark.asyncio
    async def test_health_check(self, evolution_engine):
        """Test engine health check."""
        health = await evolution_engine.health_check()
        
        assert "running" in health
        assert "enabled" in health
        assert "strategy" in health
        assert "metrics" in health
        assert "detector_stats" in health
        
        assert health["enabled"] is True
        assert health["strategy"] == "conservative"
        
    def test_metrics_tracking(self, evolution_engine):
        """Test metrics tracking functionality."""
        # Get initial metrics
        metrics = evolution_engine.get_metrics()
        initial_changes = metrics.total_changes_detected
        
        # Reset metrics
        evolution_engine.reset_metrics()
        reset_metrics = evolution_engine.get_metrics()
        
        assert reset_metrics.total_changes_detected == 0
        assert reset_metrics.changes_applied_successfully == 0
