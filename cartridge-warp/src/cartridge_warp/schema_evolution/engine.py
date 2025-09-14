"""Main schema evolution engine coordinating all components."""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import structlog

from ..connectors.base import DestinationConnector, SourceConnector, DatabaseSchema
from ..metadata.manager import MetadataManager
from ..monitoring.metrics import MetricsCollector
from .config import SchemaEvolutionConfig, EvolutionMetrics
from .detector import SchemaChangeDetector
from .migrator import SchemaMigrationEngine
from .type_converter import TypeConversionEngine
from .types import EvolutionResult, SchemaEvolutionEvent, HealthStatus

logger = structlog.get_logger(__name__)


class SchemaEvolutionEngine:
    """Main engine for intelligent schema evolution."""
    
    def __init__(
        self,
        config: SchemaEvolutionConfig,
        source_connector: SourceConnector,
        destination_connector: DestinationConnector,
        metadata_manager: MetadataManager,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        """Initialize the schema evolution engine.
        
        Args:
            config: Schema evolution configuration
            source_connector: Source database connector
            destination_connector: Destination database connector  
            metadata_manager: Metadata management system
            metrics_collector: Optional metrics collection system
        """
        self.config = config
        self.source_connector = source_connector
        self.destination_connector = destination_connector
        self.metadata_manager = metadata_manager
        self.metrics_collector = metrics_collector
        
        # Initialize components
        self.type_converter = TypeConversionEngine()
        self.change_detector = SchemaChangeDetector(config)
        self.migrator = SchemaMigrationEngine(config, destination_connector, self.type_converter)
        
        # Evolution state
        self.running = False
        self.evolution_task: Optional[asyncio.Task] = None
        self.metrics = EvolutionMetrics()
        
        # Logger with context
        self.logger = logger.bind(component="schema_evolution")
        
    async def start(self) -> None:
        """Start the schema evolution engine."""
        if self.running:
            self.logger.warning("Schema evolution engine already running")
            return
            
        if not self.config.enabled:
            self.logger.info("Schema evolution disabled in configuration")
            return
            
        self.logger.info("Starting schema evolution engine", 
                        strategy=self.config.strategy.value,
                        detection_interval=self.config.detection_interval_seconds)
        
        self.running = True
        
        # Start background evolution monitoring
        self.evolution_task = asyncio.create_task(
            self._evolution_loop(),
            name="schema_evolution_monitor"
        )
        
    async def stop(self) -> None:
        """Stop the schema evolution engine."""
        if not self.running:
            return
            
        self.logger.info("Stopping schema evolution engine")
        self.running = False
        
        if self.evolution_task and not self.evolution_task.done():
            self.evolution_task.cancel()
            try:
                await self.evolution_task
            except asyncio.CancelledError:
                pass
                
        self.logger.info("Schema evolution engine stopped")
        
    async def evolve_schema(
        self, 
        schema_name: str,
        force_check: bool = False,
        dry_run: bool = False
    ) -> EvolutionResult:
        """Perform schema evolution for a specific schema.
        
        Args:
            schema_name: Name of the schema to evolve
            force_check: Force schema change detection even if recently checked
            dry_run: Only validate and plan changes without executing
            
        Returns:
            Result of the evolution operation
        """
        start_time = datetime.now()
        
        # Track when we last checked for changes
        self.metrics.last_check = start_time
        
        self.logger.info("Starting schema evolution", 
                        schema=schema_name, 
                        force_check=force_check,
                        dry_run=dry_run)
        
        try:
            # Get current source schema
            source_schema = await self.source_connector.get_schema(schema_name)
            
            # Detect changes
            events = await self.change_detector.detect_changes(schema_name, source_schema)
            
            if not events:
                self.logger.debug("No schema changes detected", schema=schema_name)
                return EvolutionResult(
                    success=True,
                    events=[],
                    applied_changes=[],
                    warnings=[],
                    errors=[],
                    rollback_commands=[],
                    processing_time_seconds=(datetime.now() - start_time).total_seconds()
                )
                
            # Filter events based on configuration
            filtered_events = self._filter_events(events, schema_name)
            
            if not filtered_events:
                self.logger.debug("All detected changes filtered out", 
                                schema=schema_name, 
                                original_events=len(events))
                return EvolutionResult(
                    success=True,
                    events=events,
                    applied_changes=[],
                    warnings=["All changes filtered by configuration"],
                    errors=[],
                    rollback_commands=[],
                    processing_time_seconds=(datetime.now() - start_time).total_seconds()
                )
                
            # Execute migrations
            result = await self.migrator.execute_migrations(filtered_events, schema_name, dry_run)
            
            # Update metrics
            self._update_metrics(result)
            
            # Record evolution event in metadata
            if not dry_run:
                await self._record_evolution_event(schema_name, result)
                
            self.logger.info("Schema evolution completed",
                           schema=schema_name,
                           success=result.success,
                           changes_applied=len(result.applied_changes),
                           processing_time=result.processing_time_seconds)
                           
            return result
            
        except Exception as e:
            self.logger.error("Schema evolution failed", schema=schema_name, error=str(e))
            return EvolutionResult(
                success=False,
                events=[],
                applied_changes=[],
                warnings=[],
                errors=[f"Schema evolution failed: {str(e)}"],
                rollback_commands=[],
                processing_time_seconds=(datetime.now() - start_time).total_seconds()
            )
            
    async def validate_schema_changes(
        self, 
        schema_name: str,
        proposed_schema: DatabaseSchema
    ) -> EvolutionResult:
        """Validate proposed schema changes without executing them.
        
        Args:
            schema_name: Name of the schema
            proposed_schema: Proposed new schema definition
            
        Returns:
            Validation result with warnings and errors
        """
        # Get current cached schema
        current_schema = self.change_detector.get_schema_cache(schema_name)
        if not current_schema:
            # Get from source if not cached
            current_schema = await self.source_connector.get_schema(schema_name)
            
        # Detect changes
        events = await self.change_detector.detect_changes(
            schema_name, 
            proposed_schema, 
            current_schema
        )
        
        # Validate with dry run
        return await self.migrator.execute_migrations(events, schema_name, dry_run=True)
        
    async def get_schema_diff(
        self, 
        schema_name: str,
        compare_with_cache: bool = True
    ) -> List[SchemaEvolutionEvent]:
        """Get a diff of schema changes without applying them.
        
        Args:
            schema_name: Name of the schema to check
            compare_with_cache: Whether to compare with cached schema
            
        Returns:
            List of detected changes
        """
        # Get current source schema
        source_schema = await self.source_connector.get_schema(schema_name)
        
        if compare_with_cache:
            return await self.change_detector.detect_changes(schema_name, source_schema)
        else:
            # For now, compare with source schema only since destination may not have get_schema
            # dest_schema = await self.destination_connector.get_schema(schema_name)
            return await self.change_detector.detect_changes(schema_name, source_schema)
            
    async def _evolution_loop(self) -> None:
        """Background loop for continuous schema evolution monitoring."""
        self.logger.info("Starting schema evolution monitoring loop")
        
        while self.running:
            try:
                # Get list of schemas to monitor
                schemas = await self._get_monitored_schemas()
                
                # Check each schema for changes
                for schema_name in schemas:
                    if not self.running:
                        break
                        
                    try:
                        await self.evolve_schema(schema_name)
                    except Exception as e:
                        self.logger.error("Schema evolution failed for schema",
                                        schema=schema_name, 
                                        error=str(e))
                        
                # Sleep until next check
                await asyncio.sleep(self.config.detection_interval_seconds)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Evolution loop error", error=str(e))
                await asyncio.sleep(10)  # Short sleep before retrying
                
        self.logger.info("Schema evolution monitoring loop stopped")
        
    def _filter_events(self, events: List[SchemaEvolutionEvent], schema_name: str) -> List[SchemaEvolutionEvent]:
        """Filter evolution events based on configuration."""
        filtered = []
        
        for event in events:
            # Check table exclusions
            if event.table_name in self.config.excluded_tables:
                continue
                
            # Check column exclusions
            if (event.column_name and 
                event.table_name in self.config.excluded_columns and
                event.column_name in self.config.excluded_columns[event.table_name]):
                continue
                
            # Check table-specific configuration
            table_config = self.config.table_configs.get(event.table_name)
            if table_config:
                if not table_config.enabled:
                    continue
                    
                # Apply table-specific filters
                if (event.change_type.value == "add_column" and 
                    not table_config.allow_column_additions):
                    continue
                    
                if (event.change_type.value == "drop_column" and 
                    not table_config.allow_column_removals):
                    continue
                    
                if (event.change_type.value == "modify_column_type" and 
                    not table_config.allow_type_changes):
                    continue
                    
            filtered.append(event)
            
        return filtered
        
    def _update_metrics(self, result: EvolutionResult) -> None:
        """Update evolution metrics."""
        self.metrics.total_changes_detected += len(result.events)
        
        if result.success:
            self.metrics.changes_applied_successfully += len(result.applied_changes)
        else:
            self.metrics.changes_failed += len(result.events)
            
        self.metrics.total_processing_time_seconds += result.processing_time_seconds
        
        # Count by change type and safety level
        for event in result.events:
            if event.change_type.value == "add_column":
                self.metrics.column_additions += 1
            elif event.change_type.value == "drop_column":
                self.metrics.column_removals += 1
            elif event.change_type.value == "modify_column_type":
                self.metrics.type_changes += 1
            elif event.change_type.value == "add_table":
                self.metrics.table_additions += 1
            elif event.change_type.value == "drop_table":
                self.metrics.table_removals += 1
                
            if event.requires_approval:
                self.metrics.changes_requiring_approval += 1
                
            if event.safety_level.value == "safe":
                self.metrics.safe_changes += 1
            elif event.safety_level.value == "risky":
                self.metrics.risky_changes += 1
            elif event.safety_level.value == "dangerous":
                self.metrics.dangerous_changes += 1
                
        # Report metrics if collector available  
        if self.metrics_collector:
            # Record evolution metrics through structured logging for now
            # In a real implementation, this would integrate with the actual metrics collector
            self.logger.info("Recording evolution metrics", 
                           total_changes_detected=self.metrics.total_changes_detected,
                           changes_applied_successfully=self.metrics.changes_applied_successfully,
                           changes_failed=self.metrics.changes_failed)
            
    async def _record_evolution_event(self, schema_name: str, result: EvolutionResult) -> None:
        """Record evolution event in metadata system."""
        try:
            evolution_record = {
                "schema_name": schema_name,
                "timestamp": datetime.now().isoformat(),
                "success": result.success,
                "events_count": len(result.events),
                "changes_applied": len(result.applied_changes),
                "processing_time_seconds": result.processing_time_seconds,
                "warnings": result.warnings,
                "errors": result.errors
            }
            
            # TODO: Add record_evolution_event method to MetadataManager
            # await self.metadata_manager.record_evolution_event(evolution_record)
            pass
            
        except Exception as e:
            self.logger.warning("Failed to record evolution event", error=str(e))
            
    async def _get_monitored_schemas(self) -> List[str]:
        """Get list of schemas to monitor for changes."""
        # This would typically come from configuration or discovery
        # For now, return a placeholder
        return ["public"]  # TODO: Implement schema discovery
        
    def get_metrics(self) -> EvolutionMetrics:
        """Get current evolution metrics."""
        return self.metrics
        
    def reset_metrics(self) -> None:
        """Reset evolution metrics."""
        self.metrics = EvolutionMetrics()
        
    async def health_check(self) -> HealthStatus:
        """Perform health check of schema evolution engine."""
        health_status: HealthStatus = {
            "running": self.running,
            "enabled": self.config.enabled,
            "strategy": self.config.strategy.value,
            "schemas_monitored": len(await self._get_monitored_schemas()),
            "last_check": self.metrics.last_check.isoformat() if self.metrics.last_check else None,
            "metrics": {
                "total_changes_detected": self.metrics.total_changes_detected,
                "changes_applied_successfully": self.metrics.changes_applied_successfully,
                "changes_failed": self.metrics.changes_failed,
                "total_processing_time_seconds": self.metrics.total_processing_time_seconds
            }
        }
        
        # Add component health with detector stats
        try:
            # Get detector stats if available, otherwise use defaults
            detector_stats = getattr(self.change_detector, 'get_detection_stats', lambda: {
                "cache_hits": 0, "cache_misses": 0, "total_comparisons": 0
            })()
            health_status["detector_stats"] = detector_stats
        except Exception as e:
            # Fallback detector stats in case of error
            health_status["detector_stats"] = {
                "cache_hits": 0, "cache_misses": 0, "total_comparisons": 0, "error": str(e)
            }
            
        return health_status
