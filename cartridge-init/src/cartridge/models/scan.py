"""Schema scanning and data source related models."""

from enum import Enum
from typing import Optional

from sqlalchemy import Column, Enum as SQLEnum, ForeignKey, Integer, String, Text, JSON, Boolean, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from cartridge.models.base import BaseModel


class DatabaseType(str, Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"


class ScanStatus(str, Enum):
    """Scan status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class DataSource(BaseModel):
    """Data source connection configuration."""
    
    __tablename__ = "data_sources"
    
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    database_type = Column(SQLEnum(DatabaseType), nullable=False)
    
    # Connection details (encrypted in production)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    database = Column(String(255), nullable=False)
    schema = Column(String(255), default="public", nullable=False)
    username = Column(String(255), nullable=False)
    password = Column(String(500), nullable=False)  # Should be encrypted
    
    # Additional connection parameters
    connection_params = Column(JSON, nullable=True)
    
    # Status
    is_active = Column(Boolean, default=True, nullable=False)
    last_tested_at = Column(String(50), nullable=True)  # ISO timestamp
    connection_status = Column(String(50), nullable=True)
    
    # Foreign keys
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    
    # Relationships
    owner = relationship("User", back_populates="data_sources")
    scan_results = relationship("ScanResult", back_populates="data_source", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        return f"<DataSource(name={self.name}, type={self.database_type})>"


class ScanResult(BaseModel):
    """Schema scan result."""
    
    __tablename__ = "scan_results"
    
    name = Column(String(255), nullable=False)
    status = Column(SQLEnum(ScanStatus), default=ScanStatus.PENDING, nullable=False)
    
    # Scan configuration
    tables_scanned = Column(JSON, nullable=True)  # List of table names
    include_samples = Column(Boolean, default=True, nullable=False)
    sample_size = Column(Integer, default=100, nullable=False)
    
    # Scan results
    total_tables = Column(Integer, default=0, nullable=False)
    total_columns = Column(Integer, default=0, nullable=False)
    scan_duration = Column(Float, nullable=True)  # In seconds
    
    # Metadata
    scan_timestamp = Column(String(50), nullable=False)  # ISO timestamp
    error_details = Column(JSON, nullable=True)
    
    # Foreign keys
    data_source_id = Column(UUID(as_uuid=True), ForeignKey("data_sources.id"), nullable=False)
    
    # Relationships
    data_source = relationship("DataSource", back_populates="scan_results")
    tables = relationship("TableInfo", back_populates="scan_result", cascade="all, delete-orphan")
    projects = relationship("Project", back_populates="scan_result")
    
    def __repr__(self) -> str:
        return f"<ScanResult(name={self.name}, status={self.status})>"


class TableInfo(BaseModel):
    """Table information from schema scan."""
    
    __tablename__ = "table_info"
    
    name = Column(String(255), nullable=False)
    schema_name = Column(String(255), nullable=False)
    table_type = Column(String(50), nullable=True)  # table, view, materialized_view
    
    # Table statistics
    row_count = Column(Integer, default=0, nullable=False)
    size_bytes = Column(Integer, nullable=True)
    
    # Table metadata
    columns = Column(JSON, nullable=False)  # List of column information
    constraints = Column(JSON, nullable=True)  # List of constraints
    indexes = Column(JSON, nullable=True)  # List of indexes
    
    # Sample data
    sample_data = Column(JSON, nullable=True)  # List of sample rows
    
    # Data quality metrics
    null_percentages = Column(JSON, nullable=True)  # Column -> percentage
    unique_counts = Column(JSON, nullable=True)  # Column -> unique count
    data_types_inferred = Column(JSON, nullable=True)  # Inferred semantic types
    
    # Foreign keys
    scan_result_id = Column(UUID(as_uuid=True), ForeignKey("scan_results.id"), nullable=False)
    
    # Relationships
    scan_result = relationship("ScanResult", back_populates="tables")
    
    def __repr__(self) -> str:
        return f"<TableInfo(name={self.name}, schema={self.schema_name})>"