"""Configuration management for Cartridge."""

import os
from typing import Any, Dict, List, Optional

from pydantic import Field, validator
from pydantic_settings import BaseSettings


class DatabaseConfig(BaseSettings):
    """Database connection configuration."""
    
    url: str = Field(
        default="postgresql://cartridge:cartridge@localhost:5432/cartridge",
        description="Database connection URL"
    )
    pool_size: int = Field(default=10, description="Connection pool size")
    max_overflow: int = Field(default=20, description="Maximum overflow connections")
    echo: bool = Field(default=False, description="Echo SQL queries")
    
    class Config:
        env_prefix = "CARTRIDGE_DB_"


class RedisConfig(BaseSettings):
    """Redis configuration for caching and task queue."""
    
    url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL"
    )
    max_connections: int = Field(default=20, description="Maximum Redis connections")
    
    class Config:
        env_prefix = "CARTRIDGE_REDIS_"


class AIConfig(BaseSettings):
    """AI model configuration."""
    
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API key")
    anthropic_api_key: Optional[str] = Field(default=None, description="Anthropic API key")
    gemini_api_key: Optional[str] = Field(default=None, description="Google Gemini API key")
    default_model: str = Field(default="gpt-4", description="Default AI model to use")
    max_tokens: int = Field(default=4000, description="Maximum tokens per request")
    temperature: float = Field(default=0.1, description="AI model temperature")
    
    class Config:
        env_prefix = "CARTRIDGE_AI_"
    
    @validator("openai_api_key", "anthropic_api_key", "gemini_api_key")
    def validate_api_keys(cls, v: Optional[str]) -> Optional[str]:
        """Validate API keys are provided when needed."""
        return v


class SecurityConfig(BaseSettings):
    """Security configuration."""
    
    secret_key: str = Field(
        default="your-secret-key-change-this-in-production",
        description="Secret key for JWT tokens"
    )
    algorithm: str = Field(default="HS256", description="JWT algorithm")
    access_token_expire_minutes: int = Field(
        default=30, description="Access token expiration time"
    )
    
    class Config:
        env_prefix = "CARTRIDGE_SECURITY_"


class AppConfig(BaseSettings):
    """Application configuration."""
    
    name: str = Field(default="Cartridge", description="Application name")
    version: str = Field(default="0.1.0", description="Application version")
    debug: bool = Field(default=False, description="Debug mode")
    environment: str = Field(default="development", description="Environment")
    host: str = Field(default="0.0.0.0", description="Host to bind to")
    port: int = Field(default=8000, description="Port to bind to")
    workers: int = Field(default=1, description="Number of worker processes")
    
    # File upload settings
    max_file_size: int = Field(default=100 * 1024 * 1024, description="Max file size in bytes (100MB)")
    upload_dir: str = Field(default="./uploads", description="Upload directory")
    
    # Project generation settings
    output_dir: str = Field(default="./output", description="Output directory for generated projects")
    temp_dir: str = Field(default="./temp", description="Temporary directory")
    
    class Config:
        env_prefix = "CARTRIDGE_"
    
    @validator("environment")
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of the allowed values."""
        allowed = ["development", "staging", "production", "test"]
        if v not in allowed:
            raise ValueError(f"Environment must be one of {allowed}")
        return v


class LoggingConfig(BaseSettings):
    """Logging configuration."""
    
    level: str = Field(default="INFO", description="Log level")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format"
    )
    file: Optional[str] = Field(default=None, description="Log file path")
    max_file_size: int = Field(default=10 * 1024 * 1024, description="Max log file size")
    backup_count: int = Field(default=5, description="Number of backup log files")
    
    class Config:
        env_prefix = "CARTRIDGE_LOG_"


class Settings(BaseSettings):
    """Main settings class that combines all configuration sections."""
    
    app: AppConfig = Field(default_factory=AppConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    ai: AIConfig = Field(default_factory=AIConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    def create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            self.app.upload_dir,
            self.app.output_dir,
            self.app.temp_dir,
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def get_database_url(self, async_driver: bool = False) -> str:
        """Get database URL with appropriate driver."""
        if async_driver:
            return self.database.url.replace("postgresql://", "postgresql+asyncpg://")
        return self.database.url.replace("postgresql://", "postgresql+psycopg2://")
    
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.app.environment == "development"
    
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.app.environment == "production"


# Global settings instance
settings = Settings()

# Create necessary directories on import
settings.create_directories()