"""Command line interface for Cartridge."""

import asyncio
import csv
import json
import os
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import click
import uvicorn

# Set CLI mode before importing other modules to suppress factory logs
os.environ['CARTRIDGE_CLI_MODE'] = '1'

from cartridge.core.config import settings
from cartridge.core.database import init_db, drop_tables
from cartridge.core.logging import get_logger, setup_logging
from cartridge.scanner.factory import ConnectorFactory
from cartridge.ai.factory import AIProviderFactory
from cartridge.dbt.project_generator import DBTProjectGenerator

logger = get_logger(__name__)


@click.group()
@click.version_option(version=settings.app.version)
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def main(verbose):
    """Cartridge - AI-powered dbt model generator."""
    # Set verbose environment variable if requested
    if verbose:
        os.environ['CARTRIDGE_VERBOSE'] = '1'
    
    # Set up CLI-specific logging (quiet by default, unless --verbose)
    setup_logging(cli_mode=not verbose)


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
@click.option("--schema", default="public", help="Schema to scan (single schema)")
@click.option("--schemas", help="Multiple schemas to scan (comma-separated: schema1,schema2,schema3)")
@click.option("--output", "-o", help="Output file for scan results")
@click.option("--format", "output_format", default="json", type=click.Choice(["json", "yaml"]), help="Output format")
def scan(connection_string: str, schema: str, schemas: Optional[str], output: Optional[str], output_format: str):
    """Scan a database schema or multiple schemas."""
    # Determine which schemas to scan
    if schemas:
        schema_list = [s.strip() for s in schemas.split(',') if s.strip()]
        if not schema_list:
            raise click.ClickException("Invalid schemas format. Use comma-separated values: schema1,schema2,schema3")
        click.echo(f"🔍 Scanning database: {connection_string}")
        click.echo(f"📊 Schemas: {', '.join(schema_list)}")
    else:
        schema_list = [schema]
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
            
            # Scan all schemas
            all_scan_results = []
            total_tables = 0
            
            for schema_name in schema_list:
                # Create connection configuration for this schema
                connection_config = {
                    "host": parsed_url.hostname,
                    "port": parsed_url.port,
                    "database": parsed_url.path.lstrip('/') if parsed_url.path else None,
                    "username": parsed_url.username,
                    "password": parsed_url.password,
                    "schema": schema_name,
                }
                
                # Remove None values
                connection_config = {k: v for k, v in connection_config.items() if v is not None}
                
                click.echo(f"⚡ Creating database connector for schema '{schema_name}'...")
                connector = ConnectorFactory.create_connector(connector_type, connection_config)
                
                click.echo(f"🔗 Testing connection to schema '{schema_name}'...")
                await connector.test_connection()
                click.echo(f"✅ Connection to schema '{schema_name}' successful!")
                
                click.echo(f"📋 Scanning schema '{schema_name}'...")
                scan_result = await connector.scan_schema()
                
                click.echo(f"✅ Schema '{schema_name}' scan completed! Found {len(scan_result.tables)} tables")
                all_scan_results.append(scan_result)
                total_tables += len(scan_result.tables)
            
            click.echo(f"🎉 All scans completed! Found {total_tables} tables across {len(schema_list)} schema(s)")
            
            # Prepare output data for multiple schemas
            if len(schema_list) == 1:
                # Single schema - maintain backward compatibility
                scan_result = all_scan_results[0]
                output_data = {
                    "database_type": connector_type,
                    "schema": schema_list[0],
                    "connection_string": connection_string.replace(parsed_url.password or "", "***") if parsed_url.password else connection_string,
                    "scan_timestamp": scan_result.scan_timestamp if scan_result.scan_timestamp else None,
                    "tables": []
                }
                
                # Process tables for single schema (backward compatibility)
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
            else:
                # Multiple schemas - new format
                output_data = {
                    "database_type": connector_type,
                    "schemas": schema_list,
                    "connection_string": connection_string.replace(parsed_url.password or "", "***") if parsed_url.password else connection_string,
                    "scan_timestamp": max(result.scan_timestamp for result in all_scan_results if result.scan_timestamp),
                    "total_schemas": len(schema_list),
                    "total_tables": total_tables,
                    "schemas_data": []
                }
                
                for i, scan_result in enumerate(all_scan_results):
                    schema_data = {
                        "schema": schema_list[i],
                        "scan_timestamp": scan_result.scan_timestamp,
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
                        schema_data["tables"].append(table_data)
                    
                    output_data["schemas_data"].append(schema_data)
            
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
                
                if len(schema_list) == 1:
                    # Single schema output (backward compatibility)
                    click.echo(f"   Schema: {schema_list[0]}")
                    click.echo(f"   Tables: {len(output_data['tables'])}")
                    
                    for table in output_data['tables']:
                        click.echo(f"   📋 {table['name']} ({table['type']}) - {len(table['columns'])} columns")
                else:
                    # Multiple schemas output
                    click.echo(f"   Schemas: {', '.join(schema_list)}")
                    click.echo(f"   Total Tables: {output_data['total_tables']}")
                    
                    for schema_data in output_data['schemas_data']:
                        click.echo(f"\n   📂 Schema: {schema_data['schema']}")
                        click.echo(f"      Tables: {len(schema_data['tables'])}")
                        for table in schema_data['tables']:
                            click.echo(f"      📋 {table['name']} ({table['type']}) - {len(table['columns'])} columns")
                
                if output_format == "json":
                    click.echo("\n📄 Full Results (JSON):")
                    click.echo(json.dumps(output_data, indent=2, default=str))
            
        except Exception as e:
            logger.error("Scan failed", error=str(e))
            raise click.ClickException(f"Scan failed: {str(e)}")
    
    asyncio.run(_scan())


@main.command()
@click.argument("config_file")
@click.option("--output", "-o", help="Output file for scan results")
@click.option("--format", "output_format", default="json", type=click.Choice(["json", "yaml"]), help="Output format")
def scan_multi(config_file: str, output: Optional[str], output_format: str):
    """Scan multiple databases and schemas from a configuration file."""
    import yaml
    
    click.echo(f"🔍 Scanning multiple databases from config: {config_file}")
    
    async def _scan_multi():
        try:
            # Load configuration file
            config_path = Path(config_file)
            if not config_path.exists():
                raise click.ClickException(f"Configuration file not found: {config_file}")
            
            with open(config_path, 'r') as f:
                if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                    config = yaml.safe_load(f)
                elif config_file.endswith('.json'):
                    config = json.load(f)
                else:
                    raise click.ClickException("Configuration file must be YAML (.yml/.yaml) or JSON (.json)")
            
            # Validate configuration structure
            if 'databases' not in config:
                raise click.ClickException("Configuration file must contain 'databases' key")
            
            databases = config['databases']
            if not isinstance(databases, list) or not databases:
                raise click.ClickException("'databases' must be a non-empty list")
            
            # Scan all databases
            all_database_results = []
            total_databases = len(databases)
            total_schemas = 0
            total_tables = 0
            
            for i, db_config in enumerate(databases, 1):
                try:
                    # Validate database configuration
                    required_fields = ['name', 'uri', 'schemas']
                    for field in required_fields:
                        if field not in db_config:
                            raise click.ClickException(f"Database config {i} missing required field: {field}")
                    
                    db_name = db_config['name']
                    connection_string = db_config['uri']
                    schema_list = db_config['schemas']
                    
                    if not isinstance(schema_list, list) or not schema_list:
                        raise click.ClickException(f"Database '{db_name}' schemas must be a non-empty list")
                    
                    click.echo(f"\n🏢 [{i}/{total_databases}] Processing database: {db_name}")
                    click.echo(f"📊 Schemas: {', '.join(schema_list)}")
                    
                    # Parse connection string
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
                    
                    # Scan all schemas for this database
                    database_scan_results = []
                    database_table_count = 0
                    
                    for schema_name in schema_list:
                        # Create connection configuration for this schema
                        connection_config = {
                            "host": parsed_url.hostname,
                            "port": parsed_url.port,
                            "database": parsed_url.path.lstrip('/') if parsed_url.path else None,
                            "username": parsed_url.username,
                            "password": parsed_url.password,
                            "schema": schema_name,
                        }
                        
                        # Remove None values
                        connection_config = {k: v for k, v in connection_config.items() if v is not None}
                        
                        click.echo(f"  ⚡ Scanning schema '{schema_name}'...")
                        connector = ConnectorFactory.create_connector(connector_type, connection_config)
                        
                        # Test connection
                        await connector.test_connection()
                        
                        # Perform scan
                        scan_result = await connector.scan_schema()
                        
                        click.echo(f"  ✅ Schema '{schema_name}' completed! Found {len(scan_result.tables)} tables")
                        database_scan_results.append(scan_result)
                        database_table_count += len(scan_result.tables)
                    
                    # Compile database results
                    database_result = {
                        "name": db_name,
                        "database_type": connector_type,
                        "connection_string": connection_string.replace(parsed_url.password or "", "***") if parsed_url.password else connection_string,
                        "schemas": schema_list,
                        "total_schemas": len(schema_list),
                        "total_tables": database_table_count,
                        "scan_timestamp": max(result.scan_timestamp for result in database_scan_results if result.scan_timestamp),
                        "schemas_data": []
                    }
                    
                    for j, scan_result in enumerate(database_scan_results):
                        schema_data = {
                            "schema": schema_list[j],
                            "scan_timestamp": scan_result.scan_timestamp,
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
                            schema_data["tables"].append(table_data)
                        
                        database_result["schemas_data"].append(schema_data)
                    
                    all_database_results.append(database_result)
                    total_schemas += len(schema_list)
                    total_tables += database_table_count
                    
                    click.echo(f"✅ Database '{db_name}' completed! {database_table_count} tables across {len(schema_list)} schema(s)")
                    
                except Exception as e:
                    logger.error(f"Failed to scan database {db_config.get('name', 'unknown')}", error=str(e))
                    click.echo(f"❌ Failed to scan database '{db_config.get('name', 'unknown')}': {str(e)}")
                    continue
            
            click.echo(f"\n🎉 Multi-database scan completed!")
            click.echo(f"   Databases: {len(all_database_results)}/{total_databases}")
            click.echo(f"   Total Schemas: {total_schemas}")
            click.echo(f"   Total Tables: {total_tables}")
            
            # Prepare final output
            import time
            output_data = {
                "scan_type": "multi_database",
                "total_databases": len(all_database_results),
                "total_schemas": total_schemas,
                "total_tables": total_tables,
                "scan_timestamp": max(db['scan_timestamp'] for db in all_database_results if db['scan_timestamp']) if all_database_results else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "databases": all_database_results
            }
            
            # Output results
            if output:
                output_path = Path(output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                if output_format == "json":
                    with open(output_path, 'w') as f:
                        json.dump(output_data, f, indent=2, default=str)
                elif output_format == "yaml":
                    with open(output_path, 'w') as f:
                        yaml.dump(output_data, f, default_flow_style=False)
                
                click.echo(f"📄 Results saved to: {output_path}")
            else:
                # Print summary to console
                click.echo("\n📊 Multi-Database Scan Summary:")
                for db_result in all_database_results:
                    click.echo(f"\n🏢 Database: {db_result['name']} ({db_result['database_type']})")
                    click.echo(f"   Schemas: {', '.join(db_result['schemas'])}")
                    click.echo(f"   Total Tables: {db_result['total_tables']}")
                    
                    for schema_data in db_result['schemas_data']:
                        click.echo(f"   📂 Schema: {schema_data['schema']}")
                        for table in schema_data['tables']:
                            click.echo(f"      📋 {table['name']} ({table['type']}) - {len(table['columns'])} columns")
                
                if output_format == "json":
                    click.echo("\n📄 Full Results (JSON):")
                    click.echo(json.dumps(output_data, indent=2, default=str))
            
        except Exception as e:
            logger.error("Multi-database scan failed", error=str(e))
            raise click.ClickException(f"Multi-database scan failed: {str(e)}")
    
    asyncio.run(_scan_multi())


@main.command()
@click.argument("scan_file")
@click.option("--ai-model", default="gpt-4", help="AI model to use (gpt-4, gpt-3.5-turbo, claude-3-sonnet, claude-3-haiku, gemini-pro)")
@click.option("--output", "-o", help="Output directory for generated project")
@click.option("--project-name", default="generated_dbt_project", help="Name for the generated dbt project")
@click.option("--business-context", help="Business context to guide model generation")
@click.option("--business-context-file", help="CSV file containing business context (alternative to --business-context)")
@click.option("--ai-provider", type=click.Choice(["openai", "anthropic", "gemini"]), help="AI provider (auto-detected from model if not specified)")
def generate(scan_file: str, ai_model: str, output: Optional[str], project_name: str, business_context: Optional[str], business_context_file: Optional[str], ai_provider: Optional[str]):
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
            
            # Process business context
            final_business_context = business_context
            
            # Check for conflicting options
            if business_context and business_context_file:
                raise click.ClickException("Cannot specify both --business-context and --business-context-file. Choose one.")
            
            # Load business context from file if provided
            if business_context_file:
                context_path = Path(business_context_file)
                if not context_path.exists():
                    raise click.ClickException(f"Business context file not found: {business_context_file}")
                
                click.echo(f"📋 Loading business context from: {business_context_file}")
                
                try:
                    with open(context_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        context_rows = list(reader)
                        
                        if not context_rows:
                            raise click.ClickException("Business context CSV file is empty")
                        
                        # Use the first row (assuming single business context)
                        context_data = context_rows[0]
                        
                        # Build comprehensive business context from CSV data
                        context_parts = []
                        
                        if context_data.get('business_name'):
                            context_parts.append(f"Business: {context_data['business_name']}")
                        
                        if context_data.get('industry'):
                            context_parts.append(f"Industry: {context_data['industry']}")
                        
                        if context_data.get('business_description'):
                            context_parts.append(f"Description: {context_data['business_description']}")
                        
                        if context_data.get('primary_metrics'):
                            context_parts.append(f"Primary Metrics: {context_data['primary_metrics']}")
                        
                        if context_data.get('secondary_metrics') and context_data['secondary_metrics'].strip():
                            context_parts.append(f"Secondary Metrics: {context_data['secondary_metrics']}")
                        
                        if context_data.get('business_model'):
                            context_parts.append(f"Business Model: {context_data['business_model']}")
                        
                        if context_data.get('target_audience'):
                            context_parts.append(f"Target Audience: {context_data['target_audience']}")
                        
                        if context_data.get('refresh_frequency_minutes'):
                            context_parts.append(f"Data Refresh Frequency: {context_data['refresh_frequency_minutes']} minutes")
                        
                        if context_data.get('reporting_needs'):
                            context_parts.append(f"Reporting Needs: {context_data['reporting_needs']}")
                        
                        if context_data.get('data_sources'):
                            context_parts.append(f"Data Sources: {context_data['data_sources']}")
                        
                        if context_data.get('use_cases'):
                            context_parts.append(f"Use Cases: {context_data['use_cases']}")
                        
                        if context_data.get('stakeholders'):
                            context_parts.append(f"Stakeholders: {context_data['stakeholders']}")
                        
                        if context_data.get('current_challenges') and context_data['current_challenges'].strip():
                            context_parts.append(f"Current Challenges: {context_data['current_challenges']}")
                        
                        if context_data.get('success_criteria'):
                            context_parts.append(f"Success Criteria: {context_data['success_criteria']}")
                        
                        final_business_context = "\n".join(context_parts)
                        
                        click.echo(f"✅ Loaded business context for: {context_data.get('business_name', 'Unknown Business')}")
                        
                except csv.Error as e:
                    raise click.ClickException(f"Error reading CSV file: {str(e)}")
                except Exception as e:
                    raise click.ClickException(f"Error processing business context file: {str(e)}")
            
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
            
            # Identify fact and dimension tables for better mart generation
            fact_tables = []
            dimension_tables = []
            
            for table in tables:
                table_name_lower = table.name.lower()
                # Heuristics to identify fact tables (transaction/event tables)
                if any(keyword in table_name_lower for keyword in ['order', 'transaction', 'sale', 'event', 'log', 'fact']):
                    fact_tables.append(table.name)
                # Heuristics to identify dimension tables (lookup/reference tables)
                elif any(keyword in table_name_lower for keyword in ['user', 'customer', 'product', 'category', 'location', 'dim']):
                    dimension_tables.append(table.name)
                # If not clearly identifiable, add to dimensions (safer default)
                else:
                    dimension_tables.append(table.name)
            
            request = ModelGenerationRequest(
                tables=tables,
                model_types=[ModelType.STAGING, ModelType.MARTS],
                business_context=final_business_context or f"Generated dbt models for {scan_data.get('database_type', 'unknown')} database",
                include_tests=True,
                include_documentation=True,
                target_warehouse=scan_data.get("database_type", "postgresql"),
                fact_tables=fact_tables if fact_tables else None,
                dimension_tables=dimension_tables if dimension_tables else None
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
@click.option("--output", "-o", help="Output CSV file for business context", default="business_context.csv")
def onboard(output: str):
    """Interactive onboarding to collect business context and analytics requirements."""
    click.echo("🚀 Welcome to Cartridge Onboarding!")
    click.echo("Let's gather some information about your business and analytics needs.\n")
    
    # Collect business information
    business_info = {}
    
    # Business name and industry
    business_info['business_name'] = click.prompt("📊 What is your business/company name?", type=str)
    business_info['industry'] = click.prompt("🏢 What industry are you in? (e.g., e-commerce, SaaS, healthcare)", type=str)
    
    # Business description
    business_info['business_description'] = click.prompt(
        "📝 Provide a brief description of your business (1-2 sentences)", 
        type=str
    )
    
    # Key metrics and KPIs
    click.echo("\n💡 Let's understand your key business metrics:")
    business_info['primary_metrics'] = click.prompt(
        "🎯 What are your primary business metrics? (e.g., revenue, user growth, conversion rate)", 
        type=str
    )
    
    business_info['secondary_metrics'] = click.prompt(
        "📈 What are some secondary metrics you track? (optional)", 
        type=str, 
        default="", 
        show_default=False
    )
    
    # Business model
    business_info['business_model'] = click.prompt(
        "🔄 What's your business model? (e.g., subscription, marketplace, direct sales)", 
        type=str
    )
    
    # Target audience
    business_info['target_audience'] = click.prompt(
        "👥 Who is your target audience/customer base?", 
        type=str
    )
    
    # Analytics requirements
    click.echo("\n📊 Now let's understand your analytics requirements:")
    
    # Refresh frequency
    while True:
        try:
            refresh_minutes = click.prompt(
                "⏱️  How frequently do you need your analytics data to be updated? (minimum 15 minutes)", 
                type=int
            )
            if refresh_minutes >= 15:
                business_info['refresh_frequency_minutes'] = refresh_minutes
                break
            else:
                click.echo("❌ Minimum refresh frequency is 15 minutes. Please enter a value >= 15.")
        except click.Abort:
            raise
        except:
            click.echo("❌ Please enter a valid number.")
    
    # Reporting needs
    business_info['reporting_needs'] = click.prompt(
        "📋 What kind of reports do you need? (e.g., daily sales, monthly cohorts, real-time dashboards)", 
        type=str
    )
    
    # Data sources
    business_info['data_sources'] = click.prompt(
        "🔌 What are your main data sources? (e.g., PostgreSQL, Stripe, Google Analytics)", 
        type=str
    )
    
    # Use cases
    business_info['use_cases'] = click.prompt(
        "🎯 What are your main analytics use cases? (e.g., customer segmentation, sales forecasting)", 
        type=str
    )
    
    # Stakeholders
    business_info['stakeholders'] = click.prompt(
        "👔 Who are the main stakeholders who will use these analytics? (e.g., executives, marketing team)", 
        type=str
    )
    
    # Current challenges
    business_info['current_challenges'] = click.prompt(
        "🚧 What are your current data/analytics challenges? (optional)", 
        type=str, 
        default="", 
        show_default=False
    )
    
    # Success criteria
    business_info['success_criteria'] = click.prompt(
        "🏆 How will you measure the success of your analytics implementation?", 
        type=str
    )
    
    # Save to CSV
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create CSV with headers and data
    fieldnames = [
        'business_name', 'industry', 'business_description', 'primary_metrics', 
        'secondary_metrics', 'business_model', 'target_audience', 'refresh_frequency_minutes',
        'reporting_needs', 'data_sources', 'use_cases', 'stakeholders', 
        'current_challenges', 'success_criteria'
    ]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(business_info)
    
    click.echo(f"\n✅ Business context saved to: {output_path}")
    click.echo(f"📁 You can now use this file with: --business-context-file {output_path}")
    
    # Show summary
    click.echo("\n📊 Onboarding Summary:")
    click.echo(f"   Business: {business_info['business_name']} ({business_info['industry']})")
    click.echo(f"   Primary Metrics: {business_info['primary_metrics']}")
    click.echo(f"   Refresh Frequency: {business_info['refresh_frequency_minutes']} minutes")
    click.echo(f"   Main Use Cases: {business_info['use_cases']}")
    
    click.echo(f"\n🚀 Next steps:")
    click.echo(f"   1. Scan your database: cartridge scan <CONNECTION_STRING> --schema <SCHEMA> --output scan.json")
    click.echo(f"   2. Generate models: cartridge generate scan.json --business-context-file {output_path}")


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