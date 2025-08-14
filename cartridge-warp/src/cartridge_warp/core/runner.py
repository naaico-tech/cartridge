"""Main runner for cartridge-warp CDC streaming."""

import asyncio
import logging
from typing import Dict, List, Optional
import structlog

from .config import WarpConfig, SchemaConfig
from ..connectors.factory import ConnectorFactory
from ..monitoring.metrics import MetricsCollector
from ..metadata.manager import MetadataManager

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
        
        # Setup logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Configure structured logging."""
        log_format = "%(message)s" if self.config.monitoring.structured_logging else "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        
        logging.basicConfig(
            level=getattr(logging, self.config.monitoring.log_level),
            format=log_format
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
                    structlog.processors.JSONRenderer()
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
        
        if self.config.monitoring.prometheus.enabled:
            await self.metrics.stop_server()
    
    async def _initialize(self):
        """Initialize connectors and metadata management."""
        logger.info("Initializing components")
        
        # Create destination connector for metadata
        dest_connector = await self.connector_factory.create_destination_connector(
            self.config.destination
        )
        
        # Initialize metadata manager
        self.metadata_manager = MetadataManager(
            dest_connector, 
            self.config.destination.metadata_schema
        )
        await self.metadata_manager.initialize()
        
        logger.info("Components initialized successfully")
    
    async def _run_single_schema(self):
        """Run CDC for a single schema."""
        if not self.config.single_schema_name:
            raise ValueError("single_schema_name is required for single mode")
        
        schema_config = self.config.get_schema_config(self.config.single_schema_name)
        if not schema_config:
            raise ValueError(f"Schema config not found: {self.config.single_schema_name}")
        
        logger.info("Running single schema mode", schema=schema_config.name)
        
        # Create and run schema processor
        if not self.metadata_manager:
            raise RuntimeError("Metadata manager not initialized")
        
        processor = SchemaProcessor(
            schema_config,
            self.config,
            self.connector_factory,
            self.metadata_manager,
            self.metrics
        )
        
        await processor.run()
    
    async def _run_multi_schema(self):
        """Run CDC for multiple schemas concurrently."""
        logger.info("Running multi-schema mode", schema_count=len(self.config.schemas))
        
        # Create processors for each schema
        if not self.metadata_manager:
            raise RuntimeError("Metadata manager not initialized")
        
        processors = []
        for schema_config in self.config.schemas:
            processor = SchemaProcessor(
                schema_config,
                self.config,
                self.connector_factory,
                self.metadata_manager,
                self.metrics
            )
            processors.append(processor)
        
        # Run all processors concurrently
        self._running = True
        tasks = [asyncio.create_task(processor.run()) for processor in processors]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error("Error in multi-schema processing", error=str(e))
            # Cancel remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise


class SchemaProcessor:
    """Processes CDC events for a single schema."""
    
    def __init__(
        self,
        schema_config: SchemaConfig,
        warp_config: WarpConfig,
        connector_factory: ConnectorFactory,
        metadata_manager: MetadataManager,
        metrics: MetricsCollector
    ):
        self.schema_config = schema_config
        self.warp_config = warp_config
        self.connector_factory = connector_factory
        self.metadata_manager = metadata_manager
        self.metrics = metrics
        self.logger = logger.bind(schema=schema_config.name)
        
    async def run(self):
        """Run CDC processing for this schema."""
        self.logger.info("Starting schema processor", mode=self.schema_config.mode)
        
        try:
            # Create connectors
            source_connector = await self.connector_factory.create_source_connector(
                self.warp_config.source
            )
            dest_connector = await self.connector_factory.create_destination_connector(
                self.warp_config.destination
            )
            
            # Initialize schema in destination if needed
            await self._ensure_schema_exists(dest_connector)
            
            # Process tables based on mode
            if self.schema_config.mode == "stream":
                await self._process_stream_mode(source_connector, dest_connector)
            else:
                await self._process_batch_mode(source_connector, dest_connector)
                
        except Exception as e:
            self.logger.error("Schema processor failed", error=str(e))
            
            # Update metrics
            self.metrics.record_schema_status(
                self.schema_config.name, 
                "error"
            )
            raise
    
    async def _ensure_schema_exists(self, dest_connector):
        """Ensure the destination schema exists."""
        self.logger.debug("Ensuring schema exists in destination")
        # Implementation depends on destination connector
        pass
    
    async def _process_stream_mode(self, source_connector, dest_connector):
        """Process in streaming mode."""
        self.logger.info("Processing in stream mode")
        
        # Get last processed position from metadata
        last_position = await self.metadata_manager.get_stream_position(
            self.schema_config.name
        )
        
        # Start streaming changes
        async for change_batch in source_connector.stream_changes(
            schema_name=self.schema_config.name,
            last_position=last_position,
            batch_size=self.schema_config.default_batch_size
        ):
            
            # Process the batch
            await self._process_change_batch(change_batch, dest_connector)
            
            # Update position marker
            await self.metadata_manager.update_stream_position(
                self.schema_config.name,
                change_batch.last_position
            )
            
            # Update metrics
            self.metrics.record_records_processed(
                self.schema_config.name,
                len(change_batch.changes)
            )
    
    async def _process_batch_mode(self, source_connector, dest_connector):
        """Process in batch mode."""
        self.logger.info("Processing in batch mode")
        
        # Get last processed timestamp from metadata
        last_timestamp = await self.metadata_manager.get_batch_timestamp(
            self.schema_config.name
        )
        
        # Get changed records since last timestamp
        async for record_batch in source_connector.get_batch_changes(
            schema_name=self.schema_config.name,
            since_timestamp=last_timestamp,
            batch_size=self.schema_config.default_batch_size
        ):
            
            # Process the batch
            await self._process_record_batch(record_batch, dest_connector)
            
            # Update timestamp marker
            await self.metadata_manager.update_batch_timestamp(
                self.schema_config.name,
                record_batch.max_timestamp
            )
            
            # Update metrics
            self.metrics.record_records_processed(
                self.schema_config.name,
                len(record_batch.records)
            )
    
    async def _process_change_batch(self, change_batch, dest_connector):
        """Process a batch of changes."""
        self.logger.debug("Processing change batch", count=len(change_batch.changes))
        
        # Group changes by table
        changes_by_table = {}
        for change in change_batch.changes:
            table_name = change.table_name
            if table_name not in changes_by_table:
                changes_by_table[table_name] = []
            changes_by_table[table_name].append(change)
        
        # Process each table's changes
        for table_name, table_changes in changes_by_table.items():
            await self._process_table_changes(table_name, table_changes, dest_connector)
    
    async def _process_record_batch(self, record_batch, dest_connector):
        """Process a batch of records."""
        self.logger.debug("Processing record batch", count=len(record_batch.records))
        
        # Group records by table
        records_by_table = {}
        for record in record_batch.records:
            table_name = record.table_name
            if table_name not in records_by_table:
                records_by_table[table_name] = []
            records_by_table[table_name].append(record)
        
        # Process each table's records
        for table_name, table_records in records_by_table.items():
            await self._process_table_records(table_name, table_records, dest_connector)
    
    async def _process_table_changes(self, table_name: str, changes: List, dest_connector):
        """Process changes for a specific table."""
        table_logger = self.logger.bind(table=table_name)
        table_logger.debug("Processing table changes", count=len(changes))
        
        try:
            # Apply changes to destination
            await dest_connector.apply_changes(
                schema_name=self.schema_config.name,
                table_name=table_name,
                changes=changes
            )
            
            # Update metrics
            self.metrics.record_table_sync(
                self.schema_config.name,
                table_name,
                "success",
                len(changes)
            )
            
        except Exception as e:
            table_logger.error("Failed to process table changes", error=str(e))
            
            # Update metrics
            self.metrics.record_table_sync(
                self.schema_config.name,
                table_name,
                "error",
                0
            )
            
            # Re-raise based on error handling config
            if not self.warp_config.error_handling.ignore_type_conversion_errors:
                raise
    
    async def _process_table_records(self, table_name: str, records: List, dest_connector):
        """Process records for a specific table."""
        table_logger = self.logger.bind(table=table_name)
        table_logger.debug("Processing table records", count=len(records))
        
        try:
            # Write records to destination
            await dest_connector.write_records(
                schema_name=self.schema_config.name,
                table_name=table_name,
                records=records
            )
            
            # Update metrics
            self.metrics.record_table_sync(
                self.schema_config.name,
                table_name,
                "success",
                len(records)
            )
            
        except Exception as e:
            table_logger.error("Failed to process table records", error=str(e))
            
            # Update metrics
            self.metrics.record_table_sync(
                self.schema_config.name,
                table_name,
                "error",
                0
            )
            
            # Re-raise based on error handling config
            if not self.warp_config.error_handling.ignore_type_conversion_errors:
                raise
