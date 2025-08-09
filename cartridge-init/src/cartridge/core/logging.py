"""Logging configuration for Cartridge."""

import logging
import logging.handlers
import sys
from typing import Optional

import structlog
from rich.console import Console
from rich.logging import RichHandler

from cartridge.core.config import settings


def setup_logging(cli_mode: bool = False) -> None:
    """Set up structured logging with rich console output."""
    
    # Use quieter logging for CLI mode
    if cli_mode:
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            # Simple console renderer without timestamps for CLI
            structlog.processors.KeyValueRenderer(key_order=['event']),
        ]
    else:
        processors = [
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
        ]
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    root_logger = logging.getLogger()
    
    # Use WARNING level for CLI mode to suppress INFO logs
    if cli_mode:
        root_logger.setLevel(logging.WARNING)
        log_level = logging.WARNING
    else:
        root_logger.setLevel(getattr(logging, settings.logging.level.upper()))
        log_level = getattr(logging, settings.logging.level.upper())
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler with rich formatting
    if settings.app.environment != "production":
        console = Console(stderr=True)
        console_handler = RichHandler(
            console=console,
            show_time=not cli_mode,  # Hide timestamps in CLI mode
            show_path=not cli_mode,  # Hide paths in CLI mode
            markup=True,
            rich_tracebacks=True,
        )
        console_handler.setLevel(log_level)
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
    
    # Suppress factory registration logs in CLI mode
    if cli_mode:
        logging.getLogger("cartridge.scanner.factory").setLevel(logging.WARNING)
        logging.getLogger("cartridge.ai.factory").setLevel(logging.WARNING)


def get_logger(name: Optional[str] = None) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


# Only set up logging automatically if not in CLI context
# CLI will call setup_logging() explicitly
import os
if not os.environ.get('CARTRIDGE_CLI_MODE'):
    setup_logging()
else:
    # In CLI mode, immediately suppress factory logs
    import logging
    logging.getLogger("cartridge.scanner.factory").setLevel(logging.WARNING)
    logging.getLogger("cartridge.ai.factory").setLevel(logging.WARNING)

# Export the main logger
logger = get_logger(__name__)