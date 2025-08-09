"""Tests for configuration management."""

import os
import pytest
from unittest.mock import patch

from cartridge.core.config import (
    Settings, 
    AppConfig, 
    DatabaseConfig, 
    RedisConfig, 
    AIConfig, 
    SecurityConfig,
    LoggingConfig
)


class TestAppConfig:
    """Test application configuration."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = AppConfig()
        
        assert config.name == "Cartridge"
        assert config.version == "0.1.0"
        assert config.debug is False
        assert config.environment == "development"
        assert config.host == "0.0.0.0"
        assert config.port == 8000
        assert config.workers == 1
    
    def test_environment_validation(self):
        """Test environment validation."""
        # Valid environments
        for env in ["development", "staging", "production"]:
            config = AppConfig(environment=env)
            assert config.environment == env
        
        # Invalid environment should raise error
        with pytest.raises(ValueError):
            AppConfig(environment="invalid")
    
    @patch.dict(os.environ, {"CARTRIDGE_DEBUG": "true"})
    def test_env_override(self):
        """Test environment variable override."""
        config = AppConfig()
        assert config.debug is True


class TestDatabaseConfig:
    """Test database configuration."""
    
    def test_default_values(self):
        """Test default database configuration."""
        config = DatabaseConfig()
        
        assert "postgresql://" in config.url
        assert config.pool_size == 10
        assert config.max_overflow == 20
        assert config.echo is False
    
    @patch.dict(os.environ, {"CARTRIDGE_DB_POOL_SIZE": "20"})
    def test_env_override(self):
        """Test environment variable override."""
        config = DatabaseConfig()
        assert config.pool_size == 20


class TestRedisConfig:
    """Test Redis configuration."""
    
    def test_default_values(self):
        """Test default Redis configuration."""
        config = RedisConfig()
        
        assert "redis://" in config.url
        assert config.max_connections == 20
    
    @patch.dict(os.environ, {"CARTRIDGE_REDIS_MAX_CONNECTIONS": "50"})
    def test_env_override(self):
        """Test environment variable override."""
        config = RedisConfig()
        assert config.max_connections == 50


class TestAIConfig:
    """Test AI configuration."""
    
    def test_default_values(self):
        """Test default AI configuration."""
        config = AIConfig()
        
        assert config.openai_api_key is None
        assert config.anthropic_api_key is None
        assert config.default_model == "gpt-4"
        assert config.max_tokens == 4000
        assert config.temperature == 0.1
    
    @patch.dict(os.environ, {"CARTRIDGE_AI_OPENAI_API_KEY": "test-key"})
    def test_api_key_from_env(self):
        """Test API key from environment."""
        config = AIConfig()
        assert config.openai_api_key == "test-key"
    
    def test_api_key_validation(self):
        """Test API key validation."""
        # Should not raise error with None values
        config = AIConfig()
        assert config.openai_api_key is None
        
        # Should accept valid string
        config = AIConfig(openai_api_key="sk-test123")
        assert config.openai_api_key == "sk-test123"


class TestSecurityConfig:
    """Test security configuration."""
    
    def test_default_values(self):
        """Test default security configuration."""
        config = SecurityConfig()
        
        assert config.secret_key is not None
        assert config.algorithm == "HS256"
        assert config.access_token_expire_minutes == 30
    
    @patch.dict(os.environ, {"CARTRIDGE_SECURITY_SECRET_KEY": "custom-secret"})
    def test_custom_secret_key(self):
        """Test custom secret key."""
        config = SecurityConfig()
        assert config.secret_key == "custom-secret"


class TestLoggingConfig:
    """Test logging configuration."""
    
    def test_default_values(self):
        """Test default logging configuration."""
        config = LoggingConfig()
        
        assert config.level == "INFO"
        assert "%(asctime)s" in config.format
        assert config.file is None
        assert config.max_file_size == 10 * 1024 * 1024
        assert config.backup_count == 5
    
    @patch.dict(os.environ, {"CARTRIDGE_LOG_LEVEL": "DEBUG"})
    def test_log_level_override(self):
        """Test log level override."""
        config = LoggingConfig()
        assert config.level == "DEBUG"


class TestSettings:
    """Test main settings class."""
    
    def test_settings_initialization(self):
        """Test settings initialization."""
        settings = Settings()
        
        assert isinstance(settings.app, AppConfig)
        assert isinstance(settings.database, DatabaseConfig)
        assert isinstance(settings.redis, RedisConfig)
        assert isinstance(settings.ai, AIConfig)
        assert isinstance(settings.security, SecurityConfig)
        assert isinstance(settings.logging, LoggingConfig)
    
    def test_database_url_methods(self):
        """Test database URL methods."""
        settings = Settings()
        
        # Test sync URL
        sync_url = settings.get_database_url(async_driver=False)
        assert "postgresql+psycopg2://" in sync_url
        
        # Test async URL
        async_url = settings.get_database_url(async_driver=True)
        assert "postgresql+asyncpg://" in async_url
    
    def test_environment_methods(self):
        """Test environment helper methods."""
        # Test development
        settings = Settings()
        settings.app.environment = "development"
        assert settings.is_development() is True
        assert settings.is_production() is False
        
        # Test production
        settings.app.environment = "production"
        assert settings.is_development() is False
        assert settings.is_production() is True
    
    @patch("os.makedirs")
    def test_create_directories(self, mock_makedirs):
        """Test directory creation."""
        settings = Settings()
        settings.create_directories()
        
        # Should create directories for uploads, output, temp
        assert mock_makedirs.call_count >= 3
        
        # Check that makedirs was called with exist_ok=True
        for call in mock_makedirs.call_args_list:
            assert call[1]["exist_ok"] is True
    
    @patch.dict(os.environ, {
        "CARTRIDGE_ENVIRONMENT": "test",
        "CARTRIDGE_DEBUG": "true",
        "CARTRIDGE_DB_URL": "postgresql://test:test@localhost:5432/test",
        "CARTRIDGE_AI_OPENAI_API_KEY": "test-openai-key"
    })
    def test_env_file_loading(self):
        """Test loading from environment variables."""
        settings = Settings()
        
        assert settings.app.environment == "test"
        assert settings.app.debug is True
        assert "test:test" in settings.database.url
        assert settings.ai.openai_api_key == "test-openai-key"