"""Schema processor for handling individual schema synchronization."""

import asyncio
from typing import Any

import structlog

from ..connectors.base import (
    ChangeEvent,
    DestinationConnector,
    OperationType,
    SchemaChange,
    SourceConnector,
)
from ..core.config import SchemaConfig, TableConfig
from ..metadata.manager import MetadataManager
from ..monitoring.metrics import MetricsCollector

logger = structlog.get_logger(__name__)


class SchemaProcessor:
    """Processes CDC changes for a single schema independently."""

    def __init__(
        self,
        schema_config: SchemaConfig,
        source_connector: SourceConnector,
        destination_connector: DestinationConnector,
        metadata_manager: MetadataManager,
        metrics_collector: MetricsCollector,
    ):
        """Initialize the schema processor.

        Args:
            schema_config: Configuration for this schema
            source_connector: Source database connector
            destination_connector: Destination database connector
            metadata_manager: Manager for sync metadata
            metrics_collector: Metrics collection instance
        """
        self.schema_config = schema_config
        self.source_connector = source_connector
        self.destination_connector = destination_connector
        self.metadata_manager = metadata_manager
        self.metrics = metrics_collector

        self.schema_name = schema_config.name
        self.running = False
        self.tasks: dict[str, asyncio.Task] = {}

        # Logger with context
        self.logger = logger.bind(schema=self.schema_name)

    async def start(self, full_resync: bool = False) -> None:
        """Start processing for this schema.

        Args:
            full_resync: Whether to perform a full resync of all tables
        """
        if self.running:
            self.logger.warning("Schema processor already running")
            return

        self.logger.info("Starting schema processor", mode=self.schema_config.mode)
        self.running = True

        try:
            # Ensure destination schema exists
            await self.destination_connector.create_schema_if_not_exists(
                self.schema_name
            )

            # Get current schema from source
            source_schema = await self.source_connector.get_schema(self.schema_name)

            # Process each table
            for table in source_schema.tables:
                table_config = self._get_table_config(table.name)

                if full_resync:
                    # Perform full table resync
                    await self._full_table_sync(table, table_config)

                # Start change processing for this table
                if self.schema_config.mode == "stream":
                    task = asyncio.create_task(
                        self._process_table_changes(table.name, table_config),
                        name=f"table_changes_{table.name}",
                    )
                    self.tasks[table.name] = task
                else:
                    # For batch mode, process once
                    await self._process_table_batch(table.name, table_config)

            self.logger.info("Schema processor started successfully")

        except Exception as e:
            self.logger.error("Failed to start schema processor", error=str(e))
            self.running = False
            raise

    async def stop(self) -> None:
        """Stop processing for this schema."""
        if not self.running:
            return

        self.logger.info("Stopping schema processor")
        self.running = False

        # Cancel all table processing tasks
        for table_name, task in self.tasks.items():
            if not task.done():
                self.logger.debug("Cancelling table task", table=table_name)
                task.cancel()

        # Wait for tasks to complete/cancel
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)

        self.tasks.clear()
        self.logger.info("Schema processor stopped")

    async def _process_table_changes(
        self, table_name: str, table_config: TableConfig
    ) -> None:
        """Process continuous changes for a table (stream mode).

        Args:
            table_name: Name of the table to process
            table_config: Configuration for this table
        """
        table_logger = self.logger.bind(table=table_name)
        table_logger.info("Starting table change processing")

        try:
            while self.running:
                # Get last marker for this table
                last_marker = await self.destination_connector.get_marker(
                    self.schema_name, table_name
                )

                # Process changes since last marker
                batch_size = table_config.stream_batch_size
                change_count = 0

                changes_iter = self.source_connector.get_changes(
                    self.schema_name, last_marker, batch_size
                )
                async for change_event in changes_iter:
                    if not self.running:
                        break

                    # Filter changes for this specific table
                    if change_event.record.table_name != table_name:
                        continue

                    # Process the change event
                    await self._process_change_event(change_event, table_config)
                    change_count += 1

                    # Update metrics
                    self.metrics.increment_records_processed(
                        self.schema_name,
                        table_name,
                        change_event.record.operation.value,
                    )

                if change_count > 0:
                    table_logger.debug("Processed changes", count=change_count)

                # Sleep before next poll
                await asyncio.sleep(table_config.polling_interval_seconds)

        except asyncio.CancelledError:
            table_logger.info("Table processing cancelled")
            raise
        except Exception as e:
            table_logger.error("Error processing table changes", error=str(e))
            # Update error metrics
            self.metrics.increment_error_count(
                self.schema_name, table_name, "processing_error"
            )
            raise

    async def _process_table_batch(
        self, table_name: str, table_config: TableConfig
    ) -> None:
        """Process changes for a table in batch mode.

        Args:
            table_name: Name of the table to process
            table_config: Configuration for this table
        """
        table_logger = self.logger.bind(table=table_name)
        table_logger.info("Starting table batch processing")

        try:
            # Get last marker for this table
            last_marker = await self.destination_connector.get_marker(
                self.schema_name, table_name
            )

            # Process changes since last marker
            batch_size = table_config.stream_batch_size
            change_count = 0

            changes_iter = self.source_connector.get_changes(
                self.schema_name, last_marker, batch_size
            )
            async for change_event in changes_iter:
                # Filter changes for this specific table
                if change_event.record.table_name != table_name:
                    continue

                # Process the change event
                await self._process_change_event(change_event, table_config)
                change_count += 1

                # Update metrics
                self.metrics.increment_records_processed(
                    self.schema_name, table_name, change_event.record.operation.value
                )

            table_logger.info(
                "Completed batch processing", changes_processed=change_count
            )

        except Exception as e:
            table_logger.error("Error in batch processing", error=str(e))
            self.metrics.increment_error_count(
                self.schema_name, table_name, "batch_error"
            )
            raise

    async def _process_change_event(
        self, change_event: ChangeEvent, table_config: TableConfig
    ) -> None:
        """Process a single change event.

        Args:
            change_event: The change event to process
            table_config: Configuration for the table
        """
        try:
            # Handle schema changes
            if change_event.record.operation == OperationType.SCHEMA_CHANGE:
                await self._handle_schema_change(change_event)
                return

            # Write the record to destination
            await self.destination_connector.write_batch(
                self.schema_name, [change_event.record]
            )

            # Update marker after successful write
            await self.destination_connector.update_marker(
                self.schema_name,
                change_event.record.table_name,
                change_event.position_marker,
            )

        except Exception as e:
            self.logger.error(
                "Failed to process change event",
                table=change_event.record.table_name,
                operation=change_event.record.operation.value,
                error=str(e),
            )
            raise

    async def _handle_schema_change(self, change_event: ChangeEvent) -> None:
        """Handle schema change events.

        Args:
            change_event: Schema change event
        """
        self.logger.info(
            "Processing schema change",
            table=change_event.record.table_name,
            details=change_event.record.data,
        )

        # Create SchemaChange object from the event data
        schema_change = SchemaChange(
            schema_name=self.schema_name,
            table_name=change_event.record.table_name,
            change_type=change_event.record.data.get("change_type", "unknown"),
            details=change_event.record.data,
            timestamp=change_event.record.timestamp,
        )

        # Apply the schema change
        await self.destination_connector.apply_schema_changes(
            self.schema_name, [schema_change]
        )

        # Update metrics
        self.metrics.increment_schema_changes(
            self.schema_name, change_event.record.table_name, schema_change.change_type
        )

    async def _full_table_sync(self, table_schema, table_config: TableConfig) -> None:
        """Perform a full synchronization of a table.

        Args:
            table_schema: Schema definition for the table
            table_config: Configuration for the table
        """
        table_name = table_schema.name
        self.logger.info("Starting full table sync", table=table_name)

        try:
            # Ensure destination table exists
            await self.destination_connector.create_table_if_not_exists(
                self.schema_name, table_schema
            )

            # Get full snapshot from source
            record_count = 0
            batch_size = table_config.full_load_batch_size

            snapshot_iter = await self.source_connector.get_full_snapshot(
                self.schema_name, table_name, batch_size
            )
            async for record in snapshot_iter:
                if not self.running:
                    break

                # Write records to destination in batches
                await self.destination_connector.write_batch(self.schema_name, [record])

                record_count += 1

                # Update metrics
                self.metrics.increment_records_processed(
                    self.schema_name, table_name, "full_load"
                )

            self.logger.info(
                "Completed full table sync", table=table_name, records=record_count
            )

        except Exception as e:
            self.logger.error("Failed full table sync", table=table_name, error=str(e))
            raise

    def _get_table_config(self, table_name: str) -> TableConfig:
        """Get configuration for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            TableConfig for the table, or default config if not found
        """
        # Look for table-specific config
        for table_config in self.schema_config.tables:
            if table_config.name == table_name:
                return table_config

        # Return default config
        return TableConfig(
            name=table_name,
            mode=self.schema_config.mode,
            stream_batch_size=self.schema_config.default_batch_size,
            write_batch_size=500,  # Default write batch size
            full_load_batch_size=10000,  # Default full load batch size
            polling_interval_seconds=self.schema_config.default_polling_interval,
            enable_schema_evolution=True,  # Default enable schema evolution
            deletion_strategy="hard",  # Default deletion strategy
            soft_delete_column="is_deleted",  # Default soft delete column
        )

    def get_status(self) -> dict[str, Any]:
        """Get current status of the schema processor.

        Returns:
            Dictionary with status information
        """
        return {
            "schema_name": self.schema_name,
            "running": self.running,
            "mode": self.schema_config.mode,
            "active_tables": len(self.tasks),
            "table_tasks": {
                table_name: not task.done() for table_name, task in self.tasks.items()
            },
        }


__all__ = ["SchemaProcessor"]
