"""Command line interface for Cartridge."""

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import click
import uvicorn

from cartridge.core.config import settings
from cartridge.core.database import init_db, drop_tables
from cartridge.core.logging import get_logger
from cartridge.scanner.factory import ConnectorFactory
from cartridge.ai.factory import AIProviderFactory
from cartridge.dbt.project_generator import DBTProjectGenerator

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
@click.option("--format", "output_format", default="json", type=click.Choice(["json", "yaml"]), help="Output format")
def scan(connection_string: str, schema: str, output: Optional[str], output_format: str):
    """Scan a database schema."""
    click.echo(f"🔍 Scanning database: {connection_string}")
    click.echo(f"📊 Schema: {schema}")
    
    async def _scan():
        try:
            # Parse connection string to determine database type
            parsed_url = urlparse(connection_string)
            db_type = parsed_url.scheme
            
            # Map URL schemes to connector types
            type_mapping = {
                "postgresql": "postgresql",
                "postgres": "postgresql", 
                "mysql": "mysql",
                "snowflake": "snowflake",
                "bigquery": "bigquery",
                "redshift": "redshift"
            }
            
            connector_type = type_mapping.get(db_type)
            if not connector_type:
                raise click.ClickException(f"Unsupported database type: {db_type}")
            
            # Create connection configuration
            connection_config = {
                "host": parsed_url.hostname,
                "port": parsed_url.port,
                "database": parsed_url.path.lstrip('/') if parsed_url.path else None,
                "username": parsed_url.username,
                "password": parsed_url.password,
                "schema": schema,
            }
            
            # Remove None values
            connection_config = {k: v for k, v in connection_config.items() if v is not None}
            
            click.echo("⚡ Creating database connector...")
            connector = ConnectorFactory.create_connector(connector_type, connection_config)
            
            click.echo("🔗 Testing connection...")
            await connector.test_connection()
            click.echo("✅ Connection successful!")
            
            click.echo(f"📋 Scanning schema '{schema}'...")
            scan_result = await connector.scan_schema()
            
            click.echo(f"✅ Scan completed! Found {len(scan_result.tables)} tables")
            
            # Prepare output data
            output_data = {
                "database_type": connector_type,
                "schema": schema,
                "connection_string": connection_string.replace(parsed_url.password or "", "***") if parsed_url.password else connection_string,
                "scan_timestamp": scan_result.scan_timestamp if scan_result.scan_timestamp else None,
                "tables": []
            }
            
            for table in scan_result.tables:
                table_data = {
                    "name": table.name,
                    "schema": table.schema,
                    "type": table.table_type,
                    "row_count": table.row_count,
                    "columns": [
                        {
                            "name": col.name,
                            "data_type": col.data_type,
                            "is_nullable": col.nullable,
                            "is_primary_key": col.is_primary_key,
                            "default_value": col.default_value,
                            "comment": col.comment
                        }
                        for col in table.columns
                    ],
                    "constraints": [
                        {
                            "name": constraint.name,
                            "type": constraint.type,
                            "columns": constraint.columns,
                            "referenced_table": constraint.referenced_table,
                            "referenced_columns": constraint.referenced_columns
                        }
                        for constraint in table.constraints
                    ],
                    "indexes": [
                        {
                            "name": index.name,
                            "columns": index.columns,
                            "is_unique": index.is_unique,
                            "is_primary": index.is_primary
                        }
                        for index in table.indexes
                    ],
                    "sample_data": table.sample_data
                }
                output_data["tables"].append(table_data)
            
            # Output results
            if output:
                output_path = Path(output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                if output_format == "json":
                    with open(output_path, 'w') as f:
                        json.dump(output_data, f, indent=2, default=str)
                elif output_format == "yaml":
                    import yaml
                    with open(output_path, 'w') as f:
                        yaml.dump(output_data, f, default_flow_style=False)
                
                click.echo(f"📄 Results saved to: {output_path}")
            else:
                # Print summary to console
                click.echo("\n📊 Scan Summary:")
                click.echo(f"   Database Type: {connector_type}")
                click.echo(f"   Schema: {schema}")
                click.echo(f"   Tables: {len(output_data['tables'])}")
                
                for table in output_data['tables']:
                    click.echo(f"   📋 {table['name']} ({table['type']}) - {len(table['columns'])} columns")
                
                if output_format == "json":
                    click.echo("\n📄 Full Results (JSON):")
                    click.echo(json.dumps(output_data, indent=2, default=str))
            
        except Exception as e:
            logger.error("Scan failed", error=str(e))
            raise click.ClickException(f"Scan failed: {str(e)}")
    
    asyncio.run(_scan())


@main.command()
@click.argument("scan_file")
@click.option("--ai-model", default="gpt-4", help="AI model to use (gpt-4, gpt-3.5-turbo, claude-3-sonnet, claude-3-haiku, gemini-pro)")
@click.option("--output", "-o", help="Output directory for generated project")
@click.option("--project-name", default="generated_dbt_project", help="Name for the generated dbt project")
@click.option("--business-context", help="Business context to guide model generation")
@click.option("--ai-provider", type=click.Choice(["openai", "anthropic", "gemini"]), help="AI provider (auto-detected from model if not specified)")
def generate(scan_file: str, ai_model: str, output: Optional[str], project_name: str, business_context: Optional[str], ai_provider: Optional[str]):
    """Generate dbt models from schema scan results."""
    click.echo(f"🤖 Generating models from: {scan_file}")
    click.echo(f"🧠 AI model: {ai_model}")
    
    async def _generate():
        try:
            # Make ai_provider accessible in inner function
            nonlocal ai_provider
            # Load scan results
            scan_path = Path(scan_file)
            if not scan_path.exists():
                raise click.ClickException(f"Scan file not found: {scan_file}")
            
            click.echo("📖 Loading scan results...")
            with open(scan_path, 'r') as f:
                if scan_path.suffix.lower() == '.json':
                    scan_data = json.load(f)
                elif scan_path.suffix.lower() in ['.yml', '.yaml']:
                    import yaml
                    scan_data = yaml.safe_load(f)
                else:
                    raise click.ClickException("Scan file must be JSON or YAML format")
            
            # Auto-detect AI provider if not specified
            if not ai_provider:
                if ai_model.startswith(("gpt-", "o1-")):
                    ai_provider = "openai"
                elif ai_model.startswith("claude-"):
                    ai_provider = "anthropic"
                elif ai_model.startswith("gemini-"):
                    ai_provider = "gemini"
                elif ai_model in ["mock", "test"]:
                    ai_provider = "mock"
                else:
                    raise click.ClickException(f"Cannot auto-detect provider for model: {ai_model}. Please specify --ai-provider")
            
            click.echo(f"🔌 Using AI provider: {ai_provider}")
            
            # Get API key from environment (skip for mock providers)
            if ai_provider in ["mock", "test"]:
                api_key = "mock_key"
            else:
                api_key_env_vars = {
                    "openai": "OPENAI_API_KEY",
                    "anthropic": "ANTHROPIC_API_KEY", 
                    "gemini": "GEMINI_API_KEY"
                }
                
                api_key = os.getenv(api_key_env_vars[ai_provider])
                if not api_key:
                    raise click.ClickException(f"API key not found. Please set {api_key_env_vars[ai_provider]} environment variable")
            
            # Create AI provider
            click.echo("🤖 Initializing AI provider...")
            config = {"api_key": api_key}
            ai_provider_instance = AIProviderFactory.create_provider(ai_model, config)
            
            # Convert scan data to TableMapping format
            from cartridge.ai.base import TableMapping, ColumnMapping
            
            tables = []
            for table_data in scan_data.get("tables", []):
                columns = []
                for col_data in table_data.get("columns", []):
                    column = ColumnMapping(
                        name=col_data["name"],
                        data_type=col_data["data_type"],
                        nullable=col_data.get("is_nullable", True),
                        is_primary_key=col_data.get("is_primary_key", False),
                        description=col_data.get("comment", "")
                    )
                    columns.append(column)
                
                table = TableMapping(
                    name=table_data["name"],
                    schema=table_data.get("schema", "public"),
                    table_type="table",
                    columns=columns,
                    description=f"Table {table_data['name']} with {len(columns)} columns"
                )
                tables.append(table)
            
            click.echo(f"📊 Processing {len(tables)} tables...")
            
            # Generate models using AI
            click.echo("🧠 Generating dbt models with AI...")
            from cartridge.ai.base import ModelGenerationRequest, ModelType
            
            request = ModelGenerationRequest(
                tables=tables,
                model_types=[ModelType.STAGING, ModelType.MARTS],
                business_context=business_context or f"Generated dbt models for {scan_data.get('database_type', 'unknown')} database",
                include_tests=True,
                include_documentation=True,
                target_warehouse=scan_data.get("database_type", "postgresql")
            )
            
            generation_result = await ai_provider_instance.generate_models(request)
            
            click.echo(f"✅ Generated {len(generation_result.models)} models")
            
            # Create dbt project
            click.echo("📦 Creating dbt project structure...")
            dbt_generator = DBTProjectGenerator(project_name=project_name)
            
            # Determine output directory
            if output:
                output_dir = Path(output)
            else:
                output_dir = Path.cwd() / project_name
            
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate project files
            project_info = dbt_generator.generate_project(
                generation_result=generation_result,
                output_dir=str(output_dir)
            )
            
            # Files are already written by generate_project
            files_written = project_info.get("files_generated", {})
            total_files = sum(files_written.values()) if isinstance(files_written, dict) else 0
            
            click.echo(f"📁 Created dbt project in: {output_dir}")
            click.echo(f"📄 Generated {total_files} files")
            
            # Show summary
            click.echo("\n📊 Generation Summary:")
            click.echo(f"   Project Name: {project_name}")
            click.echo(f"   Output Directory: {output_dir}")
            click.echo(f"   AI Provider: {ai_provider}")
            click.echo(f"   AI Model: {ai_model}")
            click.echo(f"   Tables Processed: {len(tables)}")
            click.echo(f"   Models Generated: {len(generation_result.models)}")
            click.echo(f"   Files Created: {total_files}")
            
            # Show model details
            click.echo("\n📋 Generated Models:")
            for model in generation_result.models:
                click.echo(f"   🔧 {model.name} ({model.model_type})")
                if hasattr(model, 'description') and model.description:
                    click.echo(f"      Description: {model.description[:100]}...")
            
            click.echo(f"\n🚀 Next steps:")
            click.echo(f"   1. cd {output_dir}")
            click.echo(f"   2. dbt deps")
            click.echo(f"   3. dbt run")
            
        except Exception as e:
            logger.error("Generation failed", error=str(e))
            raise click.ClickException(f"Generation failed: {str(e)}")
    
    asyncio.run(_generate())


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