"""Command line interface for Cartridge."""

import asyncio
from typing import Optional

import click
import uvicorn

from cartridge.core.config import settings
from cartridge.core.database import init_db, drop_tables
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


@click.group()
@click.version_option(version=settings.app.version)
def main():
    """Cartridge - AI-powered dbt model generator."""
    pass


@main.command()
@click.option("--host", default=settings.app.host, help="Host to bind to")
@click.option("--port", default=settings.app.port, help="Port to bind to")
@click.option("--workers", default=settings.app.workers, help="Number of worker processes")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
def serve(host: str, port: int, workers: int, reload: bool):
    """Start the Cartridge API server."""
    logger.info("Starting Cartridge server",
                host=host,
                port=port,
                workers=workers,
                reload=reload)
    
    uvicorn.run(
        "cartridge.api.main:app",
        host=host,
        port=port,
        workers=workers if not reload else 1,
        reload=reload,
        log_level=settings.logging.level.lower(),
    )


@main.command()
def init_database():
    """Initialize the database."""
    click.echo("Initializing database...")
    
    async def _init():
        await init_db()
        click.echo("Database initialized successfully!")
    
    asyncio.run(_init())


@main.command()
@click.confirmation_option(prompt="Are you sure you want to drop all tables?")
def reset_database():
    """Reset the database (drop all tables)."""
    click.echo("Dropping all database tables...")
    
    async def _reset():
        await drop_tables()
        await init_db()
        click.echo("Database reset successfully!")
    
    asyncio.run(_reset())


@main.command()
@click.argument("connection_string")
@click.option("--schema", default="public", help="Schema to scan")
@click.option("--output", "-o", help="Output file for scan results")
def scan(connection_string: str, schema: str, output: Optional[str]):
    """Scan a database schema."""
    click.echo(f"Scanning database: {connection_string}")
    click.echo(f"Schema: {schema}")
    
    # TODO: Implement actual schema scanning
    click.echo("Schema scanning not yet implemented!")
    
    if output:
        click.echo(f"Results would be saved to: {output}")


@main.command()
@click.argument("scan_file")
@click.option("--ai-model", default="gpt-4", help="AI model to use")
@click.option("--output", "-o", help="Output directory for generated project")
def generate(scan_file: str, ai_model: str, output: Optional[str]):
    """Generate dbt models from schema scan results."""
    click.echo(f"Generating models from: {scan_file}")
    click.echo(f"AI model: {ai_model}")
    
    # TODO: Implement actual model generation
    click.echo("Model generation not yet implemented!")
    
    if output:
        click.echo(f"Project would be generated in: {output}")


@main.command()
def config():
    """Show current configuration."""
    click.echo("Cartridge Configuration:")
    click.echo(f"  Version: {settings.app.version}")
    click.echo(f"  Environment: {settings.app.environment}")
    click.echo(f"  Debug: {settings.app.debug}")
    click.echo(f"  Host: {settings.app.host}")
    click.echo(f"  Port: {settings.app.port}")
    click.echo(f"  Database URL: {settings.database.url}")
    click.echo(f"  Redis URL: {settings.redis.url}")
    click.echo(f"  Log Level: {settings.logging.level}")


if __name__ == "__main__":
    main()