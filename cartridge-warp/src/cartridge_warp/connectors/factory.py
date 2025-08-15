"""Connector factory for creating source and destination connectors."""

from typing import Optional

import structlog

from ..core.config import DestinationConfig, SourceConfig
from .base import (
    BaseDestinationConnector,
    BaseSourceConnector,
    DestinationConnector,
    SourceConnector,
)

logger = structlog.get_logger(__name__)


class ConnectorRegistry:
    """Registry for connector implementations."""

    def __init__(self) -> None:
        self._source_connectors: dict[str, type[BaseSourceConnector]] = {}
        self._destination_connectors: dict[str, type[BaseDestinationConnector]] = {}

    def register_source_connector(
        self, connector_type: str, connector_class: type[BaseSourceConnector]
    ) -> None:
        """Register a source connector implementation.

        Args:
            connector_type: Database type (e.g., "mongodb", "mysql")
            connector_class: Connector class that implements BaseSourceConnector
        """
        logger.info(
            "Registering source connector",
            type=connector_type,
            class_name=connector_class.__name__,
        )
        self._source_connectors[connector_type] = connector_class

    def register_destination_connector(
        self, connector_type: str, connector_class: type[BaseDestinationConnector]
    ) -> None:
        """Register a destination connector implementation.

        Args:
            connector_type: Database type (e.g., "postgresql", "bigquery")
            connector_class: Connector class that implements BaseDestinationConnector
        """
        logger.info(
            "Registering destination connector",
            type=connector_type,
            class_name=connector_class.__name__,
        )
        self._destination_connectors[connector_type] = connector_class

    def get_source_connector_class(
        self, connector_type: str
    ) -> Optional[type[BaseSourceConnector]]:
        """Get source connector class for given type.

        Args:
            connector_type: Database type

        Returns:
            Connector class or None if not found
        """
        return self._source_connectors.get(connector_type)

    def get_destination_connector_class(
        self, connector_type: str
    ) -> Optional[type[BaseDestinationConnector]]:
        """Get destination connector class for given type.

        Args:
            connector_type: Database type

        Returns:
            Connector class or None if not found
        """
        return self._destination_connectors.get(connector_type)

    def list_source_connectors(self) -> list[str]:
        """List all registered source connector types."""
        return list(self._source_connectors.keys())

    def list_destination_connectors(self) -> list[str]:
        """List all registered destination connector types."""
        return list(self._destination_connectors.keys())


# Global registry instance
_registry = ConnectorRegistry()


def register_source_connector(connector_type: str):
    """Decorator for registering source connector implementations.

    Usage:
        @register_source_connector("mongodb")
        class MongoDBSourceConnector(BaseSourceConnector):
            ...
    """

    def decorator(connector_class: type[BaseSourceConnector]):
        _registry.register_source_connector(connector_type, connector_class)
        return connector_class

    return decorator


def register_destination_connector(connector_type: str):
    """Decorator for registering destination connector implementations.

    Usage:
        @register_destination_connector("postgresql")
        class PostgreSQLDestinationConnector(BaseDestinationConnector):
            ...
    """

    def decorator(connector_class: type[BaseDestinationConnector]):
        _registry.register_destination_connector(connector_type, connector_class)
        return connector_class

    return decorator


class ConnectorFactory:
    """Factory for creating database connectors."""

    def __init__(self, registry: Optional[ConnectorRegistry] = None):
        """Initialize the factory with a connector registry.

        Args:
            registry: Connector registry to use. If None, uses global registry.
        """
        self.registry = registry or _registry

    async def create_source_connector(self, config: SourceConfig) -> SourceConnector:
        """Create a source connector based on configuration.

        Args:
            config: Source configuration

        Returns:
            Configured source connector instance

        Raises:
            ValueError: If connector type is not supported
            Exception: If connector creation fails
        """
        connector_class = self.registry.get_source_connector_class(config.type)

        if not connector_class:
            available_types = self.registry.list_source_connectors()
            raise ValueError(
                f"Unsupported source connector type: {config.type}. "
                f"Available types: {available_types}"
            )

        try:
            # Create connector instance with configuration
            connector = connector_class(
                connection_string=config.connection_string,
                database=config.database,
                change_detection_column=config.change_detection_column,
                change_detection_strategy=config.change_detection_strategy,
                timezone=config.timezone,
            )

            logger.info(
                "Created source connector",
                type=config.type,
                database=config.database,
                strategy=config.change_detection_strategy,
            )

            return connector

        except Exception as e:
            logger.error(
                "Failed to create source connector", type=config.type, error=str(e)
            )
            raise

    async def create_destination_connector(
        self, config: DestinationConfig
    ) -> DestinationConnector:
        """Create a destination connector based on configuration.

        Args:
            config: Destination configuration

        Returns:
            Configured destination connector instance

        Raises:
            ValueError: If connector type is not supported
            Exception: If connector creation fails
        """
        connector_class = self.registry.get_destination_connector_class(config.type)

        if not connector_class:
            available_types = self.registry.list_destination_connectors()
            raise ValueError(
                f"Unsupported destination connector type: {config.type}. "
                f"Available types: {available_types}"
            )

        try:
            # Create connector instance with configuration
            connector = connector_class(
                connection_string=config.connection_string,
                database=config.database,
                metadata_schema=config.metadata_schema,
            )

            logger.info(
                "Created destination connector",
                type=config.type,
                database=config.database,
                metadata_schema=config.metadata_schema,
            )

            return connector

        except Exception as e:
            logger.error(
                "Failed to create destination connector", type=config.type, error=str(e)
            )
            raise

    def list_available_connectors(self) -> dict[str, list[str]]:
        """List all available connector types.

        Returns:
            Dictionary with 'source' and 'destination' keys containing lists of available types
        """
        return {
            "source": self.registry.list_source_connectors(),
            "destination": self.registry.list_destination_connectors(),
        }


# Export for easy access
def get_connector_factory() -> ConnectorFactory:
    return ConnectorFactory()


__all__ = [
    "ConnectorFactory",
    "ConnectorRegistry",
    "register_source_connector",
    "register_destination_connector",
    "get_connector_factory",
]
