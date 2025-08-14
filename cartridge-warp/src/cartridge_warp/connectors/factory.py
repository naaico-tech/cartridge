"""Connector factory for creating source and destination connectors."""

from typing import Protocol, runtime_checkable

from ..core.config import SourceConfig, DestinationConfig


@runtime_checkable
class SourceConnector(Protocol):
    """Protocol for source database connectors."""
    
    async def stream_changes(self, schema_name: str, last_position, batch_size: int):
        """Stream changes from the source database."""
        ...
    
    async def get_batch_changes(self, schema_name: str, since_timestamp, batch_size: int):
        """Get batch changes from the source database."""
        ...


@runtime_checkable
class DestinationConnector(Protocol):
    """Protocol for destination database connectors."""
    
    async def apply_changes(self, schema_name: str, table_name: str, changes):
        """Apply changes to the destination database."""
        ...
    
    async def write_records(self, schema_name: str, table_name: str, records):
        """Write records to the destination database."""
        ...


class ConnectorFactory:
    """Factory for creating database connectors."""
    
    async def create_source_connector(self, config: SourceConfig) -> SourceConnector:
        """Create a source connector based on configuration."""
        # Placeholder implementation
        raise NotImplementedError("Source connectors not yet implemented")
    
    async def create_destination_connector(self, config: DestinationConfig) -> DestinationConnector:
        """Create a destination connector based on configuration."""
        # Placeholder implementation  
        raise NotImplementedError("Destination connectors not yet implemented")
