"""Logging configuration for Cartridge."""

import logging
import logging.handlers
import sys
from typing import Optional

import structlog
from rich.console import Console
from rich.logging import RichHandler

from cartridge.core.config import settings


def setup_logging() -> None:
    """Set up structured logging with rich console output."""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if settings.app.environment == "production" 
            else structlog.dev.ConsoleRenderer(colors=True),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.logging.level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler with rich formatting
    if settings.app.environment != "production":
        console = Console(stderr=True)
        console_handler = RichHandler(
            console=console,
            show_time=True,
            show_path=True,
            markup=True,
            rich_tracebacks=True,
        )
        console_handler.setLevel(getattr(logging, settings.logging.level.upper()))
        root_logger.addHandler(console_handler)
    else:
        # Simple console handler for production
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, settings.logging.level.upper()))
        formatter = logging.Formatter(settings.logging.format)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # File handler if specified
    if settings.logging.file:
        file_handler = logging.handlers.RotatingFileHandler(
            settings.logging.file,
            maxBytes=settings.logging.max_file_size,
            backupCount=settings.logging.backup_count,
        )
        file_handler.setLevel(getattr(logging, settings.logging.level.upper()))
        formatter = logging.Formatter(settings.logging.format)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
    logging.getLogger("celery").setLevel(logging.WARNING)


def get_logger(name: Optional[str] = None) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


# Set up logging on import
setup_logging()

# Export the main logger
logger = get_logger(__name__)