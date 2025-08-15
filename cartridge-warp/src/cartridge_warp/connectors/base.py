"""Base connector interfaces and abstractions for cartridge-warp."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Protocol, runtime_checkable


class OperationType(Enum):
    """Types of database operations."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    SCHEMA_CHANGE = "schema_change"


class ColumnType(Enum):
    """Supported column types for schema evolution."""

    STRING = "string"
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    JSON = "json"
    BINARY = "binary"


@dataclass
class ColumnDefinition:
    """Definition of a database column."""

    name: str
    type: ColumnType
    nullable: bool = True
    default: Optional[Any] = None
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class TableSchema:
    """Schema definition for a table."""

    name: str
    columns: list[ColumnDefinition]
    primary_keys: list[str]
    indexes: Optional[list[dict[str, Any]]] = None


@dataclass
class DatabaseSchema:
    """Schema definition for a database."""

    name: str
    tables: list[TableSchema]


@dataclass
class Record:
    """A database record with metadata."""

    table_name: str
    data: dict[str, Any]
    operation: OperationType
    timestamp: datetime
    primary_key_values: dict[str, Any]
    before_data: Optional[dict[str, Any]] = None  # For updates and deletes


@dataclass
class ChangeEvent:
    """A change event from a source database."""

    record: Record
    position_marker: Any  # Database-specific position (LSN, timestamp, etc.)
    schema_name: str


@dataclass
class SchemaChange:
    """A schema change event."""

    schema_name: str
    table_name: str
    change_type: (
        str  # "add_column", "drop_column", "modify_column", "add_table", "drop_table"
    )
    details: dict[str, Any]  # Change-specific details
    timestamp: datetime


@runtime_checkable
class SourceConnector(Protocol):
    """Protocol for source database connectors.

    Defines the interface that all source connectors must implement
    for reading data and detecting changes from source databases.
    """

    async def get_schema(self, schema_name: str) -> DatabaseSchema:
        """Get the current schema definition for a database schema.

        Args:
            schema_name: Name of the schema to retrieve

        Returns:
            DatabaseSchema object containing table and column definitions
        """
        ...

    async def get_changes(
        self, schema_name: str, marker: Optional[Any] = None, batch_size: int = 1000
    ) -> AsyncIterator[ChangeEvent]:
        """Get changes from the source database since the last marker.

        Args:
            schema_name: Name of the schema to monitor
            marker: Last processed position (LSN, timestamp, etc.)
            batch_size: Maximum number of changes to return in one batch

        Yields:
            ChangeEvent objects representing database changes
        """
        ...

    async def get_full_snapshot(
        self, schema_name: str, table_name: str, batch_size: int = 10000
    ) -> AsyncIterator[Record]:
        """Get a full snapshot of a table for initial load.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table to snapshot
            batch_size: Maximum number of records to return in one batch

        Yields:
            Record objects representing current table state
        """
        ...

    async def connect(self) -> None:
        """Establish connection to the source database."""
        ...

    async def disconnect(self) -> None:
        """Close connection to the source database."""
        ...

    async def test_connection(self) -> bool:
        """Test if the connection to the source database is working.

        Returns:
            True if connection is successful, False otherwise
        """
        ...


@runtime_checkable
class DestinationConnector(Protocol):
    """Protocol for destination database connectors.

    Defines the interface that all destination connectors must implement
    for writing data and managing schema changes in destination databases.
    """

    async def write_batch(self, schema_name: str, records: list[Record]) -> None:
        """Write a batch of records to the destination database.

        Args:
            schema_name: Name of the destination schema
            records: List of records to write
        """
        ...

    async def apply_schema_changes(
        self, schema_name: str, changes: list[SchemaChange]
    ) -> None:
        """Apply schema changes to the destination database.

        Args:
            schema_name: Name of the schema to modify
            changes: List of schema changes to apply
        """
        ...

    async def update_marker(
        self, schema_name: str, table_name: str, marker: Any
    ) -> None:
        """Update the processing position marker for a table.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            marker: New position marker value
        """
        ...

    async def get_marker(self, schema_name: str, table_name: str) -> Optional[Any]:
        """Get the current processing position marker for a table.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table

        Returns:
            Current marker value or None if not found
        """
        ...

    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        """Create schema in destination if it doesn't exist.

        Args:
            schema_name: Name of the schema to create
        """
        ...

    async def create_table_if_not_exists(
        self, schema_name: str, table_schema: TableSchema
    ) -> None:
        """Create table in destination if it doesn't exist.

        Args:
            schema_name: Name of the schema
            table_schema: Table schema definition
        """
        ...

    async def connect(self) -> None:
        """Establish connection to the destination database."""
        ...

    async def disconnect(self) -> None:
        """Close connection to the destination database."""
        ...

    async def test_connection(self) -> bool:
        """Test if the connection to the destination database is working.

        Returns:
            True if connection is successful, False otherwise
        """
        ...


class BaseSourceConnector(ABC):
    """Abstract base class for source connectors.

    Provides common functionality and helper methods for source connector implementations.
    """

    def __init__(self, connection_string: str, **kwargs):
        """Initialize the base source connector.

        Args:
            connection_string: Database connection string
            **kwargs: Additional connector-specific configuration
        """
        self.connection_string = connection_string
        self.config = kwargs
        self.connected = False

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

    @abstractmethod
    async def get_schema(self, schema_name: str) -> DatabaseSchema:
        """Get the current schema definition."""
        pass

    @abstractmethod
    async def get_changes(
        self, schema_name: str, marker: Optional[Any] = None, batch_size: int = 1000
    ) -> AsyncIterator[ChangeEvent]:
        """Get changes from the source database."""
        pass

    @abstractmethod
    async def get_full_snapshot(
        self, schema_name: str, table_name: str, batch_size: int = 10000
    ) -> AsyncIterator[Record]:
        """Get a full snapshot of a table."""
        pass

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the source database."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the source database."""
        pass

    async def test_connection(self) -> bool:
        """Default implementation of connection test."""
        try:
            await self.connect()
            await self.disconnect()
            return True
        except Exception:
            return False


class BaseDestinationConnector(ABC):
    """Abstract base class for destination connectors.

    Provides common functionality and helper methods for destination connector implementations.
    """

    def __init__(
        self,
        connection_string: str,
        metadata_schema: str = "cartridge_warp_metadata",
        **kwargs,
    ):
        """Initialize the base destination connector.

        Args:
            connection_string: Database connection string
            metadata_schema: Schema name for metadata tables
            **kwargs: Additional connector-specific configuration
        """
        self.connection_string = connection_string
        self.metadata_schema = metadata_schema
        self.config = kwargs
        self.connected = False

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

    @abstractmethod
    async def write_batch(self, schema_name: str, records: list[Record]) -> None:
        """Write a batch of records to the destination database."""
        pass

    @abstractmethod
    async def apply_schema_changes(
        self, schema_name: str, changes: list[SchemaChange]
    ) -> None:
        """Apply schema changes to the destination database."""
        pass

    @abstractmethod
    async def update_marker(
        self, schema_name: str, table_name: str, marker: Any
    ) -> None:
        """Update the processing position marker for a table."""
        pass

    @abstractmethod
    async def get_marker(self, schema_name: str, table_name: str) -> Optional[Any]:
        """Get the current processing position marker for a table."""
        pass

    @abstractmethod
    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        """Create schema in destination if it doesn't exist."""
        pass

    @abstractmethod
    async def create_table_if_not_exists(
        self, schema_name: str, table_schema: TableSchema
    ) -> None:
        """Create table in destination if it doesn't exist."""
        pass

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the destination database."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the destination database."""
        pass

    async def test_connection(self) -> bool:
        """Default implementation of connection test."""
        try:
            await self.connect()
            await self.disconnect()
            return True
        except Exception:
            return False


__all__ = [
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
]
