"""Command-line interface for cartridge-warp."""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from .core.config import WarpConfig
from .core.runner import WarpRunner

console = Console()


@click.group()
@click.version_option(version="0.1.0", prog_name="cartridge-warp")
def cli():
    """Cartridge-Warp: CDC Streaming Platform

    A modular Change Data Capture (CDC) streaming platform for real-time
    and batch data synchronization between various databases.
    """
    pass


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to configuration file",
)
@click.option(
    "--mode",
    type=click.Choice(["single", "multi"]),
    help="Execution mode (overrides config)",
)
@click.option("--schema", help="Schema name for single mode (overrides config)")
@click.option(
    "--dry-run", is_flag=True, help="Run in dry-run mode without making changes"
)
@click.option(
    "--full-resync", is_flag=True, help="Perform full resync ignoring existing markers"
)
def run(
    config: Path,
    mode: Optional[str],
    schema: Optional[str],
    dry_run: bool,
    full_resync: bool,
):
    """Run CDC streaming with the specified configuration."""

    try:
        # Load configuration
        console.print(f"[blue]Loading configuration from {config}[/blue]")
        warp_config = WarpConfig.from_file(config)

        # Override with CLI options
        if mode:
            warp_config.mode = mode  # type: ignore
        if schema:
            warp_config.single_schema_name = schema
        if dry_run:
            warp_config.dry_run = True
        if full_resync:
            warp_config.full_resync = True

        # Display configuration summary
        _display_config_summary(warp_config)

        # Run the CDC process
        console.print("[green]Starting cartridge-warp...[/green]")
        runner = WarpRunner(warp_config)

        # Run with proper signal handling
        async def run_with_signals():
            try:
                await runner.start()
            except KeyboardInterrupt:
                console.print("[yellow]Received interrupt signal, stopping...[/yellow]")
                await runner.stop()
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                sys.exit(1)

        asyncio.run(run_with_signals())

    except Exception as e:
        console.print(f"[red]Failed to start: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to configuration file",
)
def validate(config: Path):
    """Validate configuration file."""

    try:
        console.print(f"[blue]Validating configuration: {config}[/blue]")
        warp_config = WarpConfig.from_file(config)

        console.print("[green]✓ Configuration is valid[/green]")
        _display_config_summary(warp_config)

    except Exception as e:
        console.print(f"[red]✗ Configuration is invalid: {e}[/red]")
        sys.exit(1)


@cli.command()
def init():
    """Initialize a new cartridge-warp configuration file."""

    config_template = """# Cartridge-Warp Configuration
mode: single  # single or multi

# Source database configuration
source:
  type: mongodb  # mongodb, mysql, postgresql, bigquery
  connection_string: "mongodb://localhost:27017"
  database: "source_db"

  # Change detection settings
  change_detection_column: "updated_at"
  change_detection_strategy: "timestamp"  # timestamp, log, trigger
  timezone: "UTC"

# Destination database configuration
destination:
  type: postgresql  # postgresql, mysql, bigquery
  connection_string: "postgresql://localhost:5432/warehouse"
  database: "warehouse"
  metadata_schema: "cartridge_warp_metadata"

# Schema configurations
schemas:
  - name: "ecommerce"
    mode: "stream"  # stream or batch
    default_batch_size: 1000
    default_polling_interval: 5

    # Table-specific configurations (optional)
    tables:
      - name: "products"
        mode: "stream"
        stream_batch_size: 500
        write_batch_size: 250
        deletion_strategy: "soft"

      - name: "orders"
        mode: "batch"
        stream_batch_size: 2000
        polling_interval_seconds: 10

# Single schema mode setting (required when mode is 'single')
single_schema_name: "ecommerce"

# Monitoring configuration
monitoring:
  prometheus:
    enabled: true
    port: 8080
    path: "/metrics"
  log_level: "INFO"
  structured_logging: true

# Error handling configuration
error_handling:
  max_retries: 3
  backoff_factor: 2.0
  max_backoff_seconds: 300
  dead_letter_queue: true
  ignore_type_conversion_errors: true
  log_conversion_warnings: true

# Runtime settings
dry_run: false
full_resync: false
"""

    config_file = Path("cartridge-warp-config.yaml")

    if config_file.exists():
        console.print(
            f"[yellow]Configuration file already exists: {config_file}[/yellow]"
        )
        if not click.confirm("Overwrite existing file?"):
            return

    config_file.write_text(config_template)
    console.print(f"[green]Created configuration file: {config_file}[/green]")
    console.print(
        "[blue]Edit the file with your database connections and schema settings.[/blue]"
    )


def _display_config_summary(config: WarpConfig):
    """Display a summary of the configuration."""

    table = Table(title="Configuration Summary")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="magenta")

    table.add_row("Mode", config.mode)
    table.add_row("Source Type", config.source.type)
    table.add_row("Destination Type", config.destination.type)
    table.add_row("Schema Count", str(len(config.schemas)))

    if config.mode == "single":
        table.add_row("Single Schema", config.single_schema_name or "Not specified")

    table.add_row("Dry Run", "Yes" if config.dry_run else "No")
    table.add_row("Full Resync", "Yes" if config.full_resync else "No")
    table.add_row(
        "Prometheus", "Enabled" if config.monitoring.prometheus.enabled else "Disabled"
    )

    console.print(table)

    # Display schema details
    if config.schemas:
        schema_table = Table(title="Schema Configuration")
        schema_table.add_column("Schema", style="cyan")
        schema_table.add_column("Mode", style="green")
        schema_table.add_column("Tables", style="yellow")

        for schema in config.schemas:
            table_count = len(schema.tables) if schema.tables else "All"
            schema_table.add_row(schema.name, schema.mode, str(table_count))

        console.print(schema_table)


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
