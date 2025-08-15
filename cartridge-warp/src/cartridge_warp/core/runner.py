"""Main runner for cartridge-warp CDC streaming."""

import asyncio
import logging
from typing import Any, Optional

import structlog

from ..connectors.factory import ConnectorFactory
from ..metadata.manager import MetadataManager
from ..monitoring.metrics import MetricsCollector
from .config import WarpConfig
from .schema_processor import SchemaProcessor

logger = structlog.get_logger(__name__)


class WarpRunner:
    """Main runner for cartridge-warp CDC operations."""

    def __init__(self, config: WarpConfig):
        """Initialize the runner with configuration."""
        self.config = config
        self.connector_factory = ConnectorFactory()
        self.metrics = MetricsCollector(config.monitoring.prometheus)
        self.metadata_manager: Optional[MetadataManager] = None
        self._running = False
        self._schema_processors: dict[str, SchemaProcessor] = {}

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure structured logging."""
        log_format = (
            "%(message)s"
            if self.config.monitoring.structured_logging
            else "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        logging.basicConfig(
            level=getattr(logging, self.config.monitoring.log_level), format=log_format
        )

        if self.config.monitoring.structured_logging:
            structlog.configure(
                processors=[
                    structlog.stdlib.filter_by_level,
                    structlog.stdlib.add_logger_name,
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.PositionalArgumentsFormatter(),
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    structlog.processors.UnicodeDecoder(),
                    structlog.processors.JSONRenderer(),
                ],
                context_class=dict,
                logger_factory=structlog.stdlib.LoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )

    async def start(self):
        """Start the CDC streaming process."""
        logger.info("Starting cartridge-warp", mode=self.config.mode)

        try:
            # Initialize components
            await self._initialize()

            # Start metrics server
            if self.config.monitoring.prometheus.enabled:
                await self.metrics.start_server()

            # Run based on mode
            if self.config.mode == "single":
                await self._run_single_schema()
            else:
                await self._run_multi_schema()

        except Exception as e:
            logger.error("Failed to start cartridge-warp", error=str(e))
            raise

    async def stop(self):
        """Stop the CDC streaming process."""
        logger.info("Stopping cartridge-warp")
        self._running = False

        # Stop all schema processors
        stop_tasks = []
        for schema_name, processor in self._schema_processors.items():
            logger.debug("Stopping schema processor", schema=schema_name)
            stop_tasks.append(processor.stop())

        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        # Stop metrics server
        if self.config.monitoring.prometheus.enabled:
            await self.metrics.stop_server()

        logger.info("Cartridge-warp stopped successfully")

    async def _initialize(self):
        """Initialize connectors and metadata management."""
        logger.info("Initializing components")

        # Create destination connector for metadata
        dest_connector = await self.connector_factory.create_destination_connector(
            self.config.destination
        )

        # Initialize metadata manager
        self.metadata_manager = MetadataManager(
            dest_connector, self.config.destination.metadata_schema
        )
        await self.metadata_manager.initialize()

        logger.info("Components initialized successfully")

    async def _run_single_schema(self):
        """Run CDC for a single schema."""
        if not self.config.single_schema_name:
            raise ValueError("single_schema_name is required for single mode")

        schema_config = self.config.get_schema_config(self.config.single_schema_name)
        if not schema_config:
            raise ValueError(
                f"Schema config not found: {self.config.single_schema_name}"
            )

        logger.info("Running single schema mode", schema=schema_config.name)

        # Create connectors
        source_connector = await self.connector_factory.create_source_connector(
            self.config.source
        )
        dest_connector = await self.connector_factory.create_destination_connector(
            self.config.destination
        )

        # Create and run schema processor
        if not self.metadata_manager:
            raise RuntimeError("Metadata manager not initialized")

        processor = SchemaProcessor(
            schema_config,
            source_connector,
            dest_connector,
            self.metadata_manager,
            self.metrics,
        )

        self._schema_processors[schema_config.name] = processor
        self._running = True

        try:
            await processor.start(full_resync=self.config.full_resync)

            # Keep running until stopped
            while self._running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error("Error in single schema processing", error=str(e))
            raise
        finally:
            await processor.stop()

    async def _run_multi_schema(self):
        """Run CDC for multiple schemas concurrently."""
        logger.info("Running multi-schema mode", schema_count=len(self.config.schemas))

        if not self.metadata_manager:
            raise RuntimeError("Metadata manager not initialized")

        # Create connectors (shared for all schemas in multi mode)
        source_connector = await self.connector_factory.create_source_connector(
            self.config.source
        )
        dest_connector = await self.connector_factory.create_destination_connector(
            self.config.destination
        )

        # Create processors for each schema
        processors = []
        for schema_config in self.config.schemas:
            processor = SchemaProcessor(
                schema_config,
                source_connector,
                dest_connector,
                self.metadata_manager,
                self.metrics,
            )
            processors.append(processor)
            self._schema_processors[schema_config.name] = processor

        # Start all processors
        self._running = True
        start_tasks = []

        for processor in processors:
            start_tasks.append(processor.start(full_resync=self.config.full_resync))

        try:
            # Start all processors concurrently
            await asyncio.gather(*start_tasks)

            # Keep running until stopped
            while self._running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error("Error in multi-schema processing", error=str(e))
            raise
        finally:
            # Stop all processors
            stop_tasks = []
            for processor in processors:
                stop_tasks.append(processor.stop())

            if stop_tasks:
                await asyncio.gather(*stop_tasks, return_exceptions=True)

    def get_status(self) -> dict[str, Any]:
        """Get current status of the runner.

        Returns:
            Dictionary with status information
        """
        processor_status = {}
        for schema_name, processor in self._schema_processors.items():
            processor_status[schema_name] = processor.get_status()

        return {
            "running": self._running,
            "mode": self.config.mode,
            "schemas": list(self._schema_processors.keys()),
            "processor_status": processor_status,
            "metrics_enabled": self.config.monitoring.prometheus.enabled,
        }


__all__ = ["WarpRunner"]
