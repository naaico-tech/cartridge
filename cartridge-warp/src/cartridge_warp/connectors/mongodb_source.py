"""MongoDB source connector for change data capture and batch processing."""

import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from bson import ObjectId, Timestamp
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
)

from .base import (
    BaseSourceConnector,
    ChangeEvent,
    ColumnDefinition,
    ColumnType,
    DatabaseSchema,
    OperationType,
    Record,
    TableSchema,
)
from .factory import register_source_connector

logger = structlog.get_logger(__name__)


class MongoDBTypeMapper:
    """Maps MongoDB BSON types to SQL column types."""

    @staticmethod
    def map_bson_type(value: Any) -> ColumnType:
        """Map a BSON value to appropriate SQL column type.

        Args:
            value: BSON value to map

        Returns:
            Appropriate ColumnType
        """
        if value is None:
            return ColumnType.STRING  # Default for nullable fields

        if isinstance(value, bool):
            return ColumnType.BOOLEAN
        elif isinstance(value, int):
            # Check if it fits in a standard integer
            if -2147483648 <= value <= 2147483647:
                return ColumnType.INTEGER
            else:
                return ColumnType.BIGINT
        elif isinstance(value, float):
            return ColumnType.DOUBLE
        elif isinstance(value, str):
            return ColumnType.STRING
        elif isinstance(value, datetime):
            return ColumnType.TIMESTAMP
        elif isinstance(value, ObjectId):
            return ColumnType.STRING
        elif isinstance(value, Timestamp):
            return ColumnType.TIMESTAMP
        elif isinstance(value, (dict, list)):
            return ColumnType.JSON
        elif isinstance(value, bytes):
            return ColumnType.BINARY
        else:
            # For any unknown types, store as JSON
            return ColumnType.JSON

    @staticmethod
    def flatten_document(doc: Dict[str, Any], prefix: str = "", max_depth: int = 3) -> Dict[str, Any]:
        """Flatten a MongoDB document for relational storage.

        Args:
            doc: MongoDB document to flatten
            prefix: Prefix for nested field names
            max_depth: Maximum nesting depth to flatten

        Returns:
            Flattened document
        """
        if max_depth <= 0:
            return {prefix.rstrip("_"): json.dumps(doc) if doc else None}

        flattened = {}

        for key, value in doc.items():
            field_name = f"{prefix}{key}" if prefix else key

            if isinstance(value, dict):
                if value:
                    # Recursively flatten non-empty nested objects
                    nested = MongoDBTypeMapper.flatten_document(
                        value, f"{field_name}_", max_depth - 1
                    )
                    flattened.update(nested)
                else:
                    # Store empty dict as None
                    flattened[field_name] = None
            elif isinstance(value, list):
                # Store arrays as JSON
                flattened[field_name] = json.dumps(value) if value else None
            else:
                flattened[field_name] = value

        return flattened


@register_source_connector("mongodb")
class MongoDBSourceConnector(BaseSourceConnector):
    """MongoDB source connector supporting change streams and batch operations."""

    def __init__(
        self,
        connection_string: str,
        database: str,
        change_detection_column: str = "updated_at",
        change_detection_strategy: str = "timestamp",
        timezone: str = "UTC",
        **kwargs: Any
    ):
        """Initialize MongoDB source connector.

        Args:
            connection_string: MongoDB connection string
            database: Database name to connect to
            change_detection_column: Column used for timestamp-based change detection
            change_detection_strategy: Strategy for change detection ("timestamp" or "log")
            timezone: Timezone for timestamp operations
            **kwargs: Additional configuration options:
                - max_document_depth (int): Maximum depth for document flattening (default: 3)
                - use_change_streams (bool): Whether to use MongoDB change streams (default: True)
                - resume_token: Resume token for change stream continuation
                - full_document (str): Change stream full document mode ("updateLookup", etc.)
        """
        super().__init__(connection_string, **kwargs)

        self.database_name = database
        self.change_detection_column = change_detection_column
        self.change_detection_strategy = change_detection_strategy
        self.timezone = timezone

        # MongoDB-specific configuration
        self.max_document_depth = kwargs.get("max_document_depth", 3)
        self.use_change_streams = kwargs.get("use_change_streams", True)
        self.resume_token = kwargs.get("resume_token")
        self.full_document = kwargs.get("full_document", "updateLookup")

        # Connection components
        self._client: Optional[AsyncIOMotorClient] = None
        self._database: Optional[AsyncIOMotorDatabase] = None

        logger.info(
            "Initialized MongoDB source connector",
            database=database,
            strategy=change_detection_strategy,
            use_change_streams=self.use_change_streams,
        )

    async def connect(self) -> None:
        """Establish connection to MongoDB."""
        try:
            self._client = AsyncIOMotorClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                maxPoolSize=10,
                minPoolSize=1,
            )

            # Test the connection
            await self._client.admin.command("ping")

            self._database = self._client[self.database_name]
            self.connected = True

            logger.info(
                "Connected to MongoDB",
                database=self.database_name,
                server_info=await self._client.server_info()
            )

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error("Failed to connect to MongoDB", error=str(e))
            raise
        except Exception as e:
            logger.error("Unexpected error connecting to MongoDB", error=str(e))
            raise

    async def disconnect(self) -> None:
        """Close connection to MongoDB."""
        if self._client:
            self._client.close()
            self._client = None
            self._database = None
            self.connected = False
            logger.info("Disconnected from MongoDB")

    async def get_schema(self, schema_name: str) -> DatabaseSchema:
        """Get schema definition by analyzing collections and documents.

        Args:
            schema_name: Name of the schema (ignored for MongoDB, uses database)

        Returns:
            DatabaseSchema with discovered table schemas
        """
        if not self.connected or self._database is None:
            raise RuntimeError("Not connected to MongoDB")

        tables = []
        collection_names = await self._database.list_collection_names()

        logger.info(
            "Discovering schema",
            database=self.database_name,
            collections=len(collection_names)
        )

        for collection_name in collection_names:
            try:
                collection = self._database[collection_name]

                # Sample documents to infer schema
                sample_docs = []
                cursor = collection.find().limit(100)
                async for doc in cursor:
                    sample_docs.append(doc)

                if not sample_docs:
                    logger.warning("Empty collection found", collection=collection_name)
                    continue

                # Analyze document structure to create schema
                columns = self._infer_columns_from_documents(sample_docs)

                # Add MongoDB-specific metadata columns
                columns.extend([
                    ColumnDefinition(
                        name="_id",
                        type=ColumnType.STRING,
                        nullable=False
                    ),
                    ColumnDefinition(
                        name="_created_at",
                        type=ColumnType.TIMESTAMP,
                        nullable=True
                    ),
                    ColumnDefinition(
                        name="_updated_at",
                        type=ColumnType.TIMESTAMP,
                        nullable=True
                    ),
                ])

                table_schema = TableSchema(
                    name=collection_name,
                    columns=columns,
                    primary_keys=["_id"],
                    indexes=await self._get_collection_indexes(collection)
                )

                tables.append(table_schema)

                logger.debug(
                    "Inferred schema for collection",
                    collection=collection_name,
                    columns=len(columns),
                    sample_docs=len(sample_docs)
                )

            except Exception as e:
                logger.error(
                    "Failed to analyze collection",
                    collection=collection_name,
                    error=str(e)
                )
                continue

        return DatabaseSchema(name=self.database_name, tables=tables)

    async def get_changes(
        self,
        schema_name: str,
        marker: Optional[Any] = None,
        batch_size: int = 1000
    ) -> AsyncIterator[ChangeEvent]:
        """Get changes from MongoDB using change streams or timestamp-based detection.

        Args:
            schema_name: Schema name (ignored for MongoDB)
            marker: Resume token for change streams or timestamp for batch mode
            batch_size: Maximum number of changes per batch

        Yields:
            ChangeEvent objects representing changes
        """
        if not self.connected or self._database is None:
            raise RuntimeError("Not connected to MongoDB")

        if self.change_detection_strategy == "log" and self.use_change_streams:
            async for event in self._get_changes_from_streams(marker, batch_size):
                yield event
        else:
            async for event in self._get_changes_from_timestamps(marker, batch_size):
                yield event

    async def get_full_snapshot(
        self,
        schema_name: str,
        table_name: str,
        batch_size: int = 10000
    ) -> AsyncIterator[Record]:
        """Get full snapshot of a collection.

        Args:
            schema_name: Schema name (ignored for MongoDB)
            table_name: Collection name
            batch_size: Documents per batch

        Yields:
            Record objects for each document
        """
        if not self.connected or self._database is None:
            raise RuntimeError("Not connected to MongoDB")

        collection = self._database[table_name]

        logger.info(
            "Starting full snapshot",
            collection=table_name,
            batch_size=batch_size
        )

        batch = []
        batch_count = 0
        total_docs = 0

        cursor = collection.find().batch_size(batch_size)

        async for doc in cursor:
            try:
                record = self._document_to_record(doc, table_name, OperationType.INSERT)
                batch.append(record)
                total_docs += 1

                if len(batch) >= batch_size:
                    for record in batch:
                        yield record
                    batch_count += 1
                    logger.debug(
                        "Processed snapshot batch",
                        collection=table_name,
                        batch=batch_count,
                        docs_in_batch=len(batch),
                        total_docs=total_docs
                    )
                    batch = []

            except Exception as e:
                logger.error(
                    "Failed to process document in snapshot",
                    collection=table_name,
                    doc_id=doc.get("_id"),
                    error=str(e)
                )
                continue

        # Yield remaining documents
        for record in batch:
            yield record

        logger.info(
            "Completed full snapshot",
            collection=table_name,
            total_docs=total_docs,
            batches=batch_count + (1 if batch else 0)
        )

    async def _get_changes_from_streams(
        self,
        resume_token: Optional[Any],
        batch_size: int
    ) -> AsyncIterator[ChangeEvent]:
        """Get changes using MongoDB change streams.

        Args:
            resume_token: Token to resume from
            batch_size: Maximum changes per batch

        Yields:
            ChangeEvent objects
        """
        if self._database is None:
            raise RuntimeError("Database not initialized")

        try:
            # Prepare change stream options
            pipeline: List[Dict[str, Any]] = []
            options = {
                "full_document": self.full_document,
                "batch_size": batch_size,
            }

            if resume_token:
                options["resume_after"] = resume_token

            logger.info(
                "Starting change stream",
                resume_token=resume_token is not None,
                full_document=self.full_document
            )

            # Open change stream on the database
            async with self._database.watch(pipeline, **options) as stream:
                async for change in stream:
                    try:
                        event = self._change_to_event(change)
                        if event:
                            yield event
                    except Exception as e:
                        logger.error(
                            "Failed to process change event",
                            change_id=change.get("_id"),
                            error=str(e)
                        )
                        continue

        except OperationFailure as e:
            logger.error("Change stream operation failed", error=str(e))
            raise
        except Exception as e:
            logger.error("Unexpected error in change stream", error=str(e))
            raise

    async def _get_changes_from_timestamps(
        self,
        last_timestamp: Optional[datetime],
        batch_size: int
    ) -> AsyncIterator[ChangeEvent]:
        """Get changes using timestamp-based detection.

        Args:
            last_timestamp: Last processed timestamp
            batch_size: Maximum changes per batch

        Yields:
            ChangeEvent objects
        """
        if self._database is None:
            raise RuntimeError("Database not initialized")

        collection_names = await self._database.list_collection_names()

        for collection_name in collection_names:
            collection = self._database[collection_name]

            # Build query for changed documents
            query = {}
            if last_timestamp:
                query[self.change_detection_column] = {"$gt": last_timestamp}

            sort_field = [(self.change_detection_column, ASCENDING)]

            logger.debug(
                "Querying for changes",
                collection=collection_name,
                query=query,
                batch_size=batch_size
            )

            cursor = collection.find(query).sort(sort_field).limit(batch_size)

            async for doc in cursor:
                try:
                    # For timestamp-based detection, we assume all found documents are updates
                    record = self._document_to_record(doc, collection_name, OperationType.UPDATE)

                    event = ChangeEvent(
                        record=record,
                        position_marker=doc.get(self.change_detection_column),
                        schema_name=self.database_name
                    )

                    yield event

                except Exception as e:
                    logger.error(
                        "Failed to process document change",
                        collection=collection_name,
                        doc_id=doc.get("_id"),
                        error=str(e)
                    )
                    continue

    def _change_to_event(self, change: Dict[str, Any]) -> Optional[ChangeEvent]:
        """Convert MongoDB change event to ChangeEvent.

        Args:
            change: MongoDB change stream event

        Returns:
            ChangeEvent or None if conversion fails
        """
        operation_type_map = {
            "insert": OperationType.INSERT,
            "update": OperationType.UPDATE,
            "replace": OperationType.UPDATE,
            "delete": OperationType.DELETE,
        }

        mongo_op = change.get("operationType")
        if not mongo_op or not isinstance(mongo_op, str):
            logger.warning("Missing or invalid operation type", operation=mongo_op)
            return None

        operation = operation_type_map.get(mongo_op)

        if not operation:
            logger.warning("Unsupported operation type", operation=mongo_op)
            return None

        ns = change.get("ns", {})
        collection_name = ns.get("coll")

        if not collection_name:
            logger.warning("Change event missing collection name", change=change)
            return None

        # Extract document data based on operation type
        if operation == OperationType.DELETE:
            doc_id = change.get("documentKey", {}).get("_id")
            doc = {"_id": doc_id} if doc_id else {}
        else:
            doc = change.get("fullDocument") or change.get("documentKey", {})

        record = self._document_to_record(doc, collection_name, operation)

        # Add before data for updates
        if operation == OperationType.UPDATE and "updateDescription" in change:
            update_desc = change["updateDescription"]
            record.before_data = {
                "updated_fields": update_desc.get("updatedFields", {}),
                "removed_fields": update_desc.get("removedFields", []),
            }

        return ChangeEvent(
            record=record,
            position_marker=change.get("_id"),  # Resume token
            schema_name=self.database_name
        )

    def _document_to_record(
        self,
        doc: Dict[str, Any],
        collection_name: str,
        operation: OperationType
    ) -> Record:
        """Convert MongoDB document to Record.

        Args:
            doc: MongoDB document
            collection_name: Name of the collection
            operation: Type of operation

        Returns:
            Record object
        """
        # Flatten the document for relational storage
        flattened = MongoDBTypeMapper.flatten_document(doc, max_depth=self.max_document_depth)

        # Add metadata fields
        now = datetime.now(timezone.utc)
        flattened["_created_at"] = now if operation == OperationType.INSERT else None
        flattened["_updated_at"] = now

        # Ensure _id is string
        if "_id" in flattened and flattened["_id"]:
            flattened["_id"] = str(flattened["_id"])

        # Extract primary key
        primary_key_values = {"_id": flattened.get("_id")}

        return Record(
            table_name=collection_name,
            data=flattened,
            operation=operation,
            timestamp=now,
            primary_key_values=primary_key_values
        )

    def _infer_columns_from_documents(self, documents: List[Dict[str, Any]]) -> List[ColumnDefinition]:
        """Infer column definitions from sample documents.

        Args:
            documents: Sample documents to analyze

        Returns:
            List of column definitions
        """
        field_types: Dict[str, set] = {}
        field_nullable: Dict[str, bool] = {}

        for doc in documents:
            flattened = MongoDBTypeMapper.flatten_document(doc, max_depth=self.max_document_depth)

            for field, value in flattened.items():
                if field not in field_types:
                    field_types[field] = set()
                    field_nullable[field] = False

                if value is None:
                    field_nullable[field] = True
                else:
                    field_types[field].add(MongoDBTypeMapper.map_bson_type(value))

        columns = []
        for field, types in field_types.items():
            # If multiple types detected, use JSON
            if len(types) > 1:
                column_type = ColumnType.JSON
            elif types:
                column_type = list(types)[0]
            else:
                column_type = ColumnType.STRING

            columns.append(ColumnDefinition(
                name=field,
                type=column_type,
                nullable=field_nullable.get(field, True)
            ))

        return columns

    async def _get_collection_indexes(self, collection: Any) -> List[Dict[str, Any]]:
        """Get index information for a collection.

        Args:
            collection: MongoDB collection

        Returns:
            List of index definitions
        """
        try:
            indexes = []
            async for index_info in collection.list_indexes():
                indexes.append({
                    "name": index_info.get("name"),
                    "key": index_info.get("key"),
                    "unique": index_info.get("unique", False),
                    "sparse": index_info.get("sparse", False),
                })
            return indexes
        except Exception as e:
            logger.warning(
                "Failed to get index information",
                collection=collection.name,
                error=str(e)
            )
            return []


__all__ = ["MongoDBSourceConnector", "MongoDBTypeMapper"]
