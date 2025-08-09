# Cartridge 🚀

**AI-powered dbt model generator that scans data sources and creates optimized dbt models**

Cartridge automatically analyzes your database schemas, uses AI to infer optimal data models, and generates complete dbt projects with proper documentation, tests, and best practices.

## Features

- 🔍 **Schema Scanning**: Automatically scan and analyze database schemas from multiple sources
- 🤖 **AI-Powered Generation**: Use GPT-4, Claude, or other AI models to generate optimal dbt models  
- 🧪 **Test Portal**: Interactive web interface to test and validate generated models
- 📦 **Project Export**: Download complete dbt projects as tar files
- 🔌 **Multi-Database Support**: PostgreSQL, MySQL, Snowflake, BigQuery, Redshift
- 📊 **Smart Model Types**: Generate staging, intermediate, and mart models with proper dependencies
- ✅ **Auto-Testing**: Generate appropriate dbt tests and documentation
- 🐳 **Docker Ready**: Containerized for easy deployment

## Quick Start

### Using Docker (Recommended)

1. **Clone and start the application:**
   ```bash
   git clone <your-repo-url>
   cd cartridge
   cp env.example .env
   # Edit .env with your configuration
    docker-compose up -d
   ```

2. **Access the application:**
   - API: http://localhost:8000
   - Documentation: http://localhost:8000/docs
   - Health Check: http://localhost:8000/api/v1/health
   - Task Monitor: http://localhost:5555 (Flower)

### Local Development

1. **Prerequisites:**
   - Python 3.9+
   - PostgreSQL
   - Redis

2. **Setup:**
   ```bash
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -e ".[dev,test]"
   
   # Set up environment
   cp env.example .env
   # Edit .env with your configuration
   
   # Initialize database
   cartridge init-database
   
   # Start the server
   cartridge serve --reload
   ```

## Usage

### 1. Scan Your Database Schema

```bash
# Using CLI
cartridge scan "postgresql://user:pass@host:5432/db" --schema public --output scan_results.json

# Using API
curl -X POST "http://localhost:8000/api/v1/scanner/scan" \
  -H "Content-Type: application/json" \
  -d '{
    "connection": {
      "type": "postgresql",
      "host": "localhost",
      "port": 5432,
      "database": "analytics",
      "username": "user",
      "password": "password",
      "schema": "public"
    },
    "include_samples": true,
    "sample_size": 100
  }'
```

### 2. Generate dbt Models

```bash
# Using CLI
cartridge generate scan_results.json --ai-model gpt-4 --output ./my_dbt_project

# Using API
curl -X POST "http://localhost:8000/api/v1/projects/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_data": {...},
    "model_types": ["staging", "intermediate", "marts"],
    "ai_model": "gpt-4",
    "include_tests": true,
    "include_docs": true
  }'
```

### 3. Test Generated Models

```bash
# Using API
curl -X POST "http://localhost:8000/api/v1/projects/test-run" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "proj_123456",
    "dry_run": true
  }'
```

### 4. Download Complete Project

```bash
# Download as tar file
curl -X GET "http://localhost:8000/api/v1/projects/proj_123456/download" \
  --output my_dbt_project.tar.gz
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web Portal    │    │    FastAPI       │    │   AI Models     │
│   (React)       │◄──►│    Backend       │◄──►│   (GPT-4,       │
│                 │    │                  │    │    Claude)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │◄──►│   Schema         │◄──►│   dbt Runner    │
│   (Metadata)    │    │   Scanner        │    │   (Testing)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   Project        │
                       │   Generator      │
                       └──────────────────┘
```

## Project Structure

```
cartridge-init/
├── src/cartridge/
│   ├── api/                 # FastAPI application
│   │   ├── routes/          # API endpoints
│   │   └── main.py          # FastAPI app setup
│   ├── core/                # Core functionality
│   │   ├── config.py        # Configuration management
│   │   ├── database.py      # Database setup
│   │   └── logging.py       # Logging configuration
│   ├── scanner/             # Schema scanning
│   ├── ai/                  # AI model integration
│   ├── dbt/                 # dbt model generation
│   ├── portal/              # Web portal
│   ├── utils/               # Utilities
│   └── cli.py               # Command line interface
├── tests/                   # Test suite
├── docs/                    # Documentation
├── docker-compose.yml       # Docker services (at repo root)
├── Dockerfile               # Container definition (inside `cartridge-init`)
└── pyproject.toml          # Python project configuration (inside `cartridge-init`)
```

## Configuration

All configuration is managed through environment variables. See `env.example` for all available options.

Key configuration sections:
- **Application**: Basic app settings
- **Database**: PostgreSQL connection
- **Redis**: Caching and task queue
- **AI Models**: API keys and model settings
- **Security**: Authentication and encryption
- **Logging**: Log levels and output

## Development

### Setting up Development Environment

```bash
# Install development dependencies
pip install -e ".[dev,test,docs]"

# Install pre-commit hooks
pre-commit install

# Run tests
pytest

# Run linting
black src/ tests/
isort src/ tests/
flake8 src/ tests/
mypy src/

# Build documentation
mkdocs serve
```

### Running Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# All tests with coverage
pytest --cov=cartridge --cov-report=html
```

## Roadmap

- [ ] **Phase 1**: Schema scanning and basic AI integration
- [ ] **Phase 2**: Web portal and test execution
- [ ] **Phase 3**: Advanced AI features and optimization
- [ ] **Phase 4**: Enterprise features and integrations

See our [Development Plan](docs/development-plan.md) for detailed milestones.

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details. Please fork the repo, create a branch, and open a PR referencing any related issue.

## License

This project is licensed under the MIT License — you can copy, modify, and redistribute with attribution. See the [LICENSE](LICENSE) file.

## Support

- 📖 [Documentation](https://cartridge.readthedocs.io)
- 🐛 [Issue Tracker](https://github.com/yourusername/cartridge/issues)
- 💬 [Discussions](https://github.com/yourusername/cartridge/discussions)
- 📧 [Email Support](mailto:support@cartridge.dev)

---

**Built with ❤️ for the data community**