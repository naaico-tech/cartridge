"""Project and model generation related models."""

from enum import Enum
from typing import Optional

from sqlalchemy import Column, Enum as SQLEnum, ForeignKey, Integer, String, Text, JSON, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from cartridge.models.base import BaseModel


class ProjectStatus(str, Enum):
    """Project status enumeration."""
    PENDING = "pending"
    SCANNING = "scanning"
    GENERATING = "generating"
    TESTING = "testing"
    COMPLETED = "completed"
    FAILED = "failed"


class ModelType(str, Enum):
    """dbt model type enumeration."""
    STAGING = "staging"
    INTERMEDIATE = "intermediate"
    MARTS = "marts"
    SNAPSHOT = "snapshot"


class Project(BaseModel):
    """Project model for managing dbt model generation projects."""
    
    __tablename__ = "projects"
    
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    status = Column(SQLEnum(ProjectStatus), default=ProjectStatus.PENDING, nullable=False)
    
    # Configuration
    ai_model = Column(String(100), default="gpt-4", nullable=False)
    model_types = Column(JSON, nullable=False)  # List of ModelType values
    include_tests = Column(Boolean, default=True, nullable=False)
    include_docs = Column(Boolean, default=True, nullable=False)
    
    # Generation results
    generation_settings = Column(JSON, nullable=True)
    execution_log = Column(JSON, nullable=True)
    error_details = Column(JSON, nullable=True)
    
    # File paths
    project_path = Column(String(500), nullable=True)
    archive_path = Column(String(500), nullable=True)
    
    # Foreign keys
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    scan_result_id = Column(UUID(as_uuid=True), ForeignKey("scan_results.id"), nullable=True)
    
    # Relationships
    owner = relationship("User", back_populates="projects")
    scan_result = relationship("ScanResult", back_populates="projects")
    models = relationship("GeneratedModel", back_populates="project", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        return f"<Project(name={self.name}, status={self.status})>"


class GeneratedModel(BaseModel):
    """Generated dbt model information."""
    
    __tablename__ = "generated_models"
    
    name = Column(String(255), nullable=False)
    model_type = Column(SQLEnum(ModelType), nullable=False)
    description = Column(Text, nullable=True)
    
    # Model content
    sql = Column(Text, nullable=False)
    tests = Column(JSON, nullable=True)  # List of test configurations
    dependencies = Column(JSON, nullable=True)  # List of model dependencies
    
    # Metadata
    table_name = Column(String(255), nullable=True)
    schema_name = Column(String(255), nullable=True)
    
    # Execution results
    execution_status = Column(String(50), nullable=True)
    execution_time = Column(Integer, nullable=True)  # In milliseconds
    rows_affected = Column(Integer, nullable=True)
    execution_error = Column(Text, nullable=True)
    
    # Foreign keys
    project_id = Column(UUID(as_uuid=True), ForeignKey("projects.id"), nullable=False)
    
    # Relationships
    project = relationship("Project", back_populates="models")
    
    def __repr__(self) -> str:
        return f"<GeneratedModel(name={self.name}, type={self.model_type})>"