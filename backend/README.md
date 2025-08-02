# Cartridge Backend 🚀

**AI-powered dbt model generator - Backend API**

FastAPI-based backend service that handles schema scanning, AI model generation, and dbt project management.

## Features

- 🔍 **Schema Scanning**: Multi-database schema analysis and profiling
- 🤖 **AI Integration**: GPT-4, Claude, and other LLM integrations for model generation
- 📊 **dbt Generation**: Automated dbt model, test, and documentation generation
- 🧪 **Test Execution**: Isolated dbt model testing and validation
- 📦 **Project Export**: Complete dbt project packaging and download
- 🔒 **Security**: JWT authentication and role-based access control

## Quick Start

### Using Docker (Recommended)

```bash
# From the backend directory
cd backend
cp env.example .env
# Edit .env with your configuration
docker-compose up -d
```

### Local Development

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev,test]"
cartridge serve --reload
```

## API Documentation

Once running, visit:
- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/api/v1/health

## Project Structure

```
backend/
├── src/cartridge/           # Main application code
│   ├── api/                 # FastAPI routes and middleware
│   ├── core/                # Configuration, database, logging
│   ├── models/              # SQLAlchemy database models
│   ├── scanner/             # Schema scanning engine
│   ├── ai/                  # AI model integration
│   ├── dbt/                 # dbt project generation
│   ├── tasks/               # Celery background tasks
│   └── utils/               # Utility functions
├── alembic/                 # Database migrations
├── scripts/                 # Setup and utility scripts
├── tests/                   # Test suite
└── docs/                    # API documentation
```

## Development

```bash
# Install dependencies
make install

# Start development server
make dev

# Run tests
make test

# Format code
make format

# Database operations
make db-init
make db-migration MESSAGE="description"
make db-upgrade
```

## Environment Variables

See `env.example` for all configuration options.

## Contributing

Please see the main project README for contributing guidelines.