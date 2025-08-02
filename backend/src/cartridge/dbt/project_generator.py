"""DBT project generator for creating complete dbt projects."""

import os
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml

from cartridge.ai.base import GeneratedModel, ModelGenerationResult
from cartridge.dbt.file_generator import DBTFileGenerator
from cartridge.dbt.templates import DBTTemplates
from cartridge.core.config import settings
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


class DBTProjectGenerator:
    """Generator for complete dbt projects."""
    
    def __init__(self, project_name: str, target_warehouse: str = "postgresql"):
        """Initialize project generator."""
        self.project_name = project_name
        self.target_warehouse = target_warehouse
        self.file_generator = DBTFileGenerator()
        self.templates = DBTTemplates()
        self.logger = get_logger(__name__)
    
    def generate_project(
        self,
        generation_result: ModelGenerationResult,
        output_dir: Optional[str] = None,
        connection_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Generate a complete dbt project."""
        
        if not output_dir:
            output_dir = os.path.join(settings.app.output_dir, self.project_name)
        
        self.logger.info(f"Generating dbt project: {self.project_name}")
        
        try:
            # Create project directory structure
            self._create_project_structure(output_dir)
            
            # Generate core project files
            self._generate_dbt_project_yml(output_dir, generation_result)
            self._generate_profiles_yml(output_dir, connection_config)
            self._generate_packages_yml(output_dir)
            self._generate_gitignore(output_dir)
            self._generate_readme(output_dir, generation_result)
            
            # Generate source definitions
            self._generate_sources_yml(output_dir, generation_result)
            
            # Generate model files
            model_files = self._generate_model_files(output_dir, generation_result.models)
            
            # Generate schema files (tests and documentation)
            schema_files = self._generate_schema_files(output_dir, generation_result.models)
            
            # Generate macros
            macro_files = self._generate_macros(output_dir)
            
            # Generate analysis files
            analysis_files = self._generate_analysis_files(output_dir, generation_result)
            
            # Generate documentation
            docs_files = self._generate_docs_files(output_dir, generation_result)
            
            project_info = {
                "project_name": self.project_name,
                "project_path": output_dir,
                "target_warehouse": self.target_warehouse,
                "models_generated": len(generation_result.models),
                "files_created": {
                    "models": len(model_files),
                    "schemas": len(schema_files),
                    "macros": len(macro_files),
                    "analysis": len(analysis_files),
                    "docs": len(docs_files)
                },
                "project_structure": self._get_project_structure(output_dir),
                "generation_metadata": generation_result.generation_metadata
            }
            
            self.logger.info(f"dbt project generated successfully at {output_dir}")
            return project_info
            
        except Exception as e:
            self.logger.error(f"Failed to generate dbt project: {e}")
            raise
    
    def create_project_archive(self, project_path: str, archive_path: Optional[str] = None) -> str:
        """Create a tar.gz archive of the dbt project."""
        
        if not archive_path:
            archive_path = f"{project_path}.tar.gz"
        
        self.logger.info(f"Creating project archive: {archive_path}")
        
        try:
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(project_path, arcname=self.project_name)
            
            archive_size = os.path.getsize(archive_path)
            self.logger.info(f"Archive created successfully: {archive_size} bytes")
            
            return archive_path
            
        except Exception as e:
            self.logger.error(f"Failed to create project archive: {e}")
            raise
    
    def _create_project_structure(self, output_dir: str) -> None:
        """Create the dbt project directory structure."""
        
        directories = [
            output_dir,
            os.path.join(output_dir, "models"),
            os.path.join(output_dir, "models", "staging"),
            os.path.join(output_dir, "models", "intermediate"),
            os.path.join(output_dir, "models", "marts"),
            os.path.join(output_dir, "models", "marts", "core"),
            os.path.join(output_dir, "macros"),
            os.path.join(output_dir, "tests"),
            os.path.join(output_dir, "analysis"),
            os.path.join(output_dir, "snapshots"),
            os.path.join(output_dir, "seeds"),
            os.path.join(output_dir, "docs"),
            os.path.join(output_dir, "logs"),
            os.path.join(output_dir, "target"),
            os.path.join(output_dir, "dbt_packages"),
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def _generate_dbt_project_yml(self, output_dir: str, generation_result: ModelGenerationResult) -> None:
        """Generate dbt_project.yml file."""
        
        # Determine model configurations based on generated models
        model_config = {}
        
        for model in generation_result.models:
            model_type = model.model_type.value
            if model_type not in model_config:
                model_config[model_type] = {}
            
            model_config[model_type][model.materialization] = True
        
        project_config = {
            "name": self.project_name,
            "version": "1.0.0",
            "config-version": 2,
            
            "profile": self.project_name,
            
            "model-paths": ["models"],
            "analysis-paths": ["analysis"],
            "test-paths": ["tests"],
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            
            "target-path": "target",
            "clean-targets": [
                "target",
                "dbt_packages",
                "logs"
            ],
            
            "models": {
                self.project_name: {
                    "staging": {
                        "+materialized": "view",
                        "+tags": ["staging"]
                    },
                    "intermediate": {
                        "+materialized": "view",
                        "+tags": ["intermediate"]
                    },
                    "marts": {
                        "+materialized": "table",
                        "+tags": ["marts"],
                        "core": {
                            "+tags": ["marts", "core"]
                        }
                    }
                }
            },
            
            "tests": {
                "+store_failures": True
            },
            
            "vars": {
                "start_date": "2020-01-01",
                "timezone": "UTC"
            }
        }
        
        file_path = os.path.join(output_dir, "dbt_project.yml")
        with open(file_path, 'w') as f:
            yaml.dump(project_config, f, default_flow_style=False, sort_keys=False)
    
    def _generate_profiles_yml(self, output_dir: str, connection_config: Optional[Dict[str, Any]]) -> None:
        """Generate profiles.yml file."""
        
        if not connection_config:
            # Create a template profiles.yml
            profiles_config = {
                self.project_name: {
                    "target": "dev",
                    "outputs": {
                        "dev": {
                            "type": self.target_warehouse,
                            "host": "{{ env_var('DBT_HOST') }}",
                            "user": "{{ env_var('DBT_USER') }}",
                            "password": "{{ env_var('DBT_PASSWORD') }}",
                            "port": "{{ env_var('DBT_PORT') | as_number }}",
                            "dbname": "{{ env_var('DBT_DBNAME') }}",
                            "schema": "{{ env_var('DBT_SCHEMA') }}",
                            "threads": 4,
                            "keepalives_idle": 0
                        },
                        "prod": {
                            "type": self.target_warehouse,
                            "host": "{{ env_var('DBT_PROD_HOST') }}",
                            "user": "{{ env_var('DBT_PROD_USER') }}",
                            "password": "{{ env_var('DBT_PROD_PASSWORD') }}",
                            "port": "{{ env_var('DBT_PROD_PORT') | as_number }}",
                            "dbname": "{{ env_var('DBT_PROD_DBNAME') }}",
                            "schema": "{{ env_var('DBT_PROD_SCHEMA') }}",
                            "threads": 8,
                            "keepalives_idle": 0
                        }
                    }
                }
            }
        else:
            # Use provided connection config
            profiles_config = {
                self.project_name: {
                    "target": "dev",
                    "outputs": {
                        "dev": {
                            "type": connection_config.get("type", self.target_warehouse),
                            "host": connection_config.get("host"),
                            "user": connection_config.get("username"),
                            "password": connection_config.get("password"),
                            "port": connection_config.get("port"),
                            "dbname": connection_config.get("database"),
                            "schema": connection_config.get("schema", "public"),
                            "threads": 4,
                            "keepalives_idle": 0
                        }
                    }
                }
            }
        
        file_path = os.path.join(output_dir, "profiles.yml")
        with open(file_path, 'w') as f:
            yaml.dump(profiles_config, f, default_flow_style=False, sort_keys=False)
    
    def _generate_packages_yml(self, output_dir: str) -> None:
        """Generate packages.yml file with useful dbt packages."""
        
        packages_config = {
            "packages": [
                {
                    "package": "dbt-labs/dbt_utils",
                    "version": [">=1.0.0", "<2.0.0"]
                },
                {
                    "package": "calogica/dbt_expectations",
                    "version": [">=0.8.0", "<1.0.0"]
                },
                {
                    "package": "dbt-labs/codegen",
                    "version": [">=0.9.0", "<1.0.0"]
                }
            ]
        }
        
        file_path = os.path.join(output_dir, "packages.yml")
        with open(file_path, 'w') as f:
            yaml.dump(packages_config, f, default_flow_style=False, sort_keys=False)
    
    def _generate_gitignore(self, output_dir: str) -> None:
        """Generate .gitignore file."""
        
        gitignore_content = """
# dbt
target/
dbt_packages/
logs/
*.log

# OS
.DS_Store
Thumbs.db

# IDE
.vscode/
.idea/
*.swp
*.swo

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.env
.venv

# Jupyter
.ipynb_checkpoints

# Local configuration
profiles.yml
""".strip()
        
        file_path = os.path.join(output_dir, ".gitignore")
        with open(file_path, 'w') as f:
            f.write(gitignore_content)
    
    def _generate_readme(self, output_dir: str, generation_result: ModelGenerationResult) -> None:
        """Generate README.md file."""
        
        readme_content = f"""# {self.project_name}

This dbt project was automatically generated by Cartridge.

## Project Overview

- **Models Generated**: {len(generation_result.models)}
- **Target Warehouse**: {self.target_warehouse}
- **Generated By**: {generation_result.generation_metadata.get('ai_provider', 'Unknown')}
- **AI Model Used**: {generation_result.generation_metadata.get('model_used', 'Unknown')}

## Project Structure

```
{self.project_name}/
├── models/
│   ├── staging/          # Raw data transformations
│   ├── intermediate/     # Business logic transformations
│   └── marts/           # Final analytical models
│       └── core/        # Core business entities
├── macros/              # Reusable SQL functions
├── tests/               # Custom data tests
├── analysis/            # Analytical queries
├── snapshots/           # Slowly changing dimensions
└── seeds/               # Static reference data
```

## Model Types

### Staging Models
Clean and standardize raw data from sources.

### Intermediate Models  
Apply business logic and combine staging models.

### Mart Models
Final analytical models optimized for consumption.

## Getting Started

1. **Install dbt**:
   ```bash
   pip install dbt-{self.target_warehouse}
   ```

2. **Install dependencies**:
   ```bash
   dbt deps
   ```

3. **Configure your connection**:
   Update `profiles.yml` with your database credentials.

4. **Test your connection**:
   ```bash
   dbt debug
   ```

5. **Run the models**:
   ```bash
   dbt run
   ```

6. **Test the models**:
   ```bash
   dbt test
   ```

7. **Generate documentation**:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Model Documentation

{self._generate_model_documentation_section(generation_result.models)}

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [dbt Style Guide](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)

---

*Generated by Cartridge - AI-powered dbt model generator*
"""
        
        file_path = os.path.join(output_dir, "README.md")
        with open(file_path, 'w') as f:
            f.write(readme_content)
    
    def _generate_sources_yml(self, output_dir: str, generation_result: ModelGenerationResult) -> None:
        """Generate sources.yml file."""
        
        # Extract unique source schemas and tables from models
        sources = {}
        
        for model in generation_result.models:
            if hasattr(model, 'meta') and model.meta and 'source_table' in model.meta:
                source_table = model.meta['source_table']
                if '.' in source_table:
                    schema, table = source_table.split('.', 1)
                    if schema not in sources:
                        sources[schema] = []
                    if table not in sources[schema]:
                        sources[schema].append(table)
        
        if sources:
            sources_config = {
                "version": 2,
                "sources": []
            }
            
            for schema, tables in sources.items():
                source_config = {
                    "name": schema,
                    "description": f"Raw data from {schema} schema",
                    "tables": [
                        {
                            "name": table,
                            "description": f"Raw {table} data"
                        }
                        for table in tables
                    ]
                }
                sources_config["sources"].append(source_config)
            
            file_path = os.path.join(output_dir, "models", "staging", "sources.yml")
            with open(file_path, 'w') as f:
                yaml.dump(sources_config, f, default_flow_style=False, sort_keys=False)
    
    def _generate_model_files(self, output_dir: str, models: List[GeneratedModel]) -> List[str]:
        """Generate SQL files for all models."""
        
        generated_files = []
        
        for model in models:
            # Determine subdirectory based on model type
            if model.model_type.value == "staging":
                model_dir = os.path.join(output_dir, "models", "staging")
            elif model.model_type.value == "intermediate":
                model_dir = os.path.join(output_dir, "models", "intermediate")
            elif model.model_type.value == "marts":
                model_dir = os.path.join(output_dir, "models", "marts", "core")
            else:
                model_dir = os.path.join(output_dir, "models")
            
            # Generate SQL file
            file_path = self.file_generator.generate_model_file(model, model_dir)
            generated_files.append(file_path)
        
        return generated_files
    
    def _generate_schema_files(self, output_dir: str, models: List[GeneratedModel]) -> List[str]:
        """Generate schema.yml files with tests and documentation."""
        
        generated_files = []
        
        # Group models by directory for schema files
        model_groups = {}
        
        for model in models:
            if model.model_type.value == "staging":
                group_key = "staging"
                schema_dir = os.path.join(output_dir, "models", "staging")
            elif model.model_type.value == "intermediate":
                group_key = "intermediate"
                schema_dir = os.path.join(output_dir, "models", "intermediate")
            elif model.model_type.value == "marts":
                group_key = "marts"
                schema_dir = os.path.join(output_dir, "models", "marts", "core")
            else:
                group_key = "other"
                schema_dir = os.path.join(output_dir, "models")
            
            if group_key not in model_groups:
                model_groups[group_key] = {"models": [], "dir": schema_dir}
            
            model_groups[group_key]["models"].append(model)
        
        # Generate schema files for each group
        for group_key, group_data in model_groups.items():
            file_path = self.file_generator.generate_schema_file(
                group_data["models"], 
                group_data["dir"], 
                f"schema.yml"
            )
            generated_files.append(file_path)
        
        return generated_files
    
    def _generate_macros(self, output_dir: str) -> List[str]:
        """Generate macro files."""
        
        generated_files = []
        macros_dir = os.path.join(output_dir, "macros")
        
        # Generate common utility macros
        utility_macros = self.templates.get_utility_macros()
        
        for macro_name, macro_content in utility_macros.items():
            file_path = os.path.join(macros_dir, f"{macro_name}.sql")
            with open(file_path, 'w') as f:
                f.write(macro_content)
            generated_files.append(file_path)
        
        return generated_files
    
    def _generate_analysis_files(self, output_dir: str, generation_result: ModelGenerationResult) -> List[str]:
        """Generate analysis files."""
        
        generated_files = []
        analysis_dir = os.path.join(output_dir, "analysis")
        
        # Generate basic analysis queries
        analysis_queries = self.templates.get_analysis_templates(generation_result.models)
        
        for query_name, query_content in analysis_queries.items():
            file_path = os.path.join(analysis_dir, f"{query_name}.sql")
            with open(file_path, 'w') as f:
                f.write(query_content)
            generated_files.append(file_path)
        
        return generated_files
    
    def _generate_docs_files(self, output_dir: str, generation_result: ModelGenerationResult) -> List[str]:
        """Generate documentation files."""
        
        generated_files = []
        docs_dir = os.path.join(output_dir, "docs")
        
        # Generate model documentation
        docs_content = self.templates.get_documentation_template(generation_result)
        
        file_path = os.path.join(docs_dir, "models.md")
        with open(file_path, 'w') as f:
            f.write(docs_content)
        generated_files.append(file_path)
        
        return generated_files
    
    def _get_project_structure(self, output_dir: str) -> Dict[str, Any]:
        """Get the project directory structure."""
        
        structure = {}
        
        for root, dirs, files in os.walk(output_dir):
            rel_path = os.path.relpath(root, output_dir)
            if rel_path == ".":
                rel_path = ""
            
            current_level = structure
            if rel_path:
                for part in rel_path.split(os.sep):
                    if part not in current_level:
                        current_level[part] = {}
                    current_level = current_level[part]
            
            for file in files:
                current_level[file] = "file"
        
        return structure
    
    def _generate_model_documentation_section(self, models: List[GeneratedModel]) -> str:
        """Generate model documentation section for README."""
        
        if not models:
            return "No models generated."
        
        # Group models by type
        model_groups = {}
        for model in models:
            model_type = model.model_type.value
            if model_type not in model_groups:
                model_groups[model_type] = []
            model_groups[model_type].append(model)
        
        sections = []
        
        for model_type, type_models in model_groups.items():
            section = f"\n### {model_type.title()} Models\n\n"
            
            for model in type_models:
                section += f"- **{model.name}**: {model.description}\n"
            
            sections.append(section)
        
        return "".join(sections)