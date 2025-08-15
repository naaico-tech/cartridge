"""Database connectors for cartridge-warp."""

# Import connectors to register them
from . import mongodb_source  # noqa: F401
from .base import (
    BaseDestinationConnector,
    BaseSourceConnector,
    ChangeEvent,
    ColumnDefinition,
    ColumnType,
    DatabaseSchema,
    DestinationConnector,
    OperationType,
    Record,
    SchemaChange,
    SourceConnector,
    TableSchema,
)
from .factory import (
    ConnectorFactory,
    ConnectorRegistry,
    get_connector_factory,
    register_destination_connector,
    register_source_connector,
)

__all__ = [
    # Base types and interfaces
    "OperationType",
    "ColumnType",
    "ColumnDefinition",
    "TableSchema",
    "DatabaseSchema",
    "Record",
    "ChangeEvent",
    "SchemaChange",
    "SourceConnector",
    "DestinationConnector",
    "BaseSourceConnector",
    "BaseDestinationConnector",
    # Factory and registry
    "ConnectorFactory",
    "ConnectorRegistry",
    "register_source_connector",
    "register_destination_connector",
    "get_connector_factory",
]
