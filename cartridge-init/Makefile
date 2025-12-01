# Cartridge Makefile
# Provides convenient commands for development and deployment

.PHONY: help install dev test lint format clean build docker-build docker-up docker-down migration docs

# Default target
help: ## Show this help message
	@echo "Cartridge - AI-powered dbt model generator"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Development setup
install: ## Install dependencies for production
	pip install -e .

install-dev: ## Install dependencies for development
	pip install -e ".[dev,test]"
	pre-commit install

dev: ## Start development server with auto-reload
	cartridge serve --reload

# Testing and quality
test: ## Run all tests
	pytest

test-cov: ## Run tests with coverage report
	pytest --cov=cartridge --cov-report=html --cov-report=term

test-unit: ## Run unit tests only
	pytest tests/unit/ -v

test-integration: ## Run integration tests only
	pytest tests/integration/ -v

test-performance: ## Run performance tests
	pytest tests/performance/ -v -m slow

test-models: ## Run model tests only
	pytest tests/unit/test_models.py -v

test-api: ## Run API tests only
	pytest tests/unit/test_api.py -v

test-tasks: ## Run task tests only
	pytest tests/unit/test_tasks.py -v

test-config: ## Run configuration tests only
	pytest tests/unit/test_config.py -v

test-db: ## Run database tests only
	pytest tests/test_database_setup.py -v

test-failed: ## Re-run only failed tests
	pytest --lf

test-parallel: ## Run tests in parallel
	pytest -n auto

lint: ## Run linting checks (ruff + mypy)
	ruff check src/ tests/
	mypy src/

format: ## Format code with black and ruff
	black src/ tests/
	ruff check --fix src/ tests/

format-check: ## Check code formatting without making changes
	black --check src/ tests/
	ruff check src/ tests/

type-check: ## Run type checking with mypy
	mypy src/

# Database operations
db-init: ## Initialize database
	cartridge init-database

db-reset: ## Reset database (drops all tables)
	cartridge reset-database

db-migration: ## Create new database migration
	alembic revision --autogenerate -m "$(MESSAGE)"

db-upgrade: ## Apply database migrations
	alembic upgrade head

db-downgrade: ## Rollback database migration
	alembic downgrade -1

# Docker operations
docker-build: ## Build Docker images
	docker-compose build

docker-up: ## Start all services with Docker
	docker-compose up -d

docker-down: ## Stop all Docker services
	docker-compose down

docker-logs: ## Show Docker logs
	docker-compose logs -f

docker-clean: ## Clean up Docker resources
	docker-compose down -v --remove-orphans
	docker system prune -f

# Development utilities
clean: ## Clean up build artifacts and cache
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/

build: ## Build package for distribution
	python -m build

docs: ## Start documentation server
	mkdocs serve

docs-build: ## Build documentation
	mkdocs build

# Configuration
config: ## Show current configuration
	cartridge config

# Production deployment
deploy-prod: ## Deploy to production (placeholder)
	@echo "Production deployment not yet implemented"
	@echo "Please refer to deployment documentation"

# Backup and restore
backup-db: ## Backup database (requires running database)
	@echo "Creating database backup..."
	docker-compose exec postgres pg_dump -U cartridge cartridge > backup_$$(date +%Y%m%d_%H%M%S).sql

restore-db: ## Restore database from backup file (set BACKUP_FILE variable)
	@if [ -z "$(BACKUP_FILE)" ]; then \
		echo "Please specify BACKUP_FILE variable: make restore-db BACKUP_FILE=backup.sql"; \
		exit 1; \
	fi
	docker-compose exec -T postgres psql -U cartridge -d cartridge < $(BACKUP_FILE)

# Security
security-check: ## Run security checks
	safety check
	bandit -r src/

# Performance
benchmark: ## Run performance benchmarks (placeholder)
	@echo "Performance benchmarks not yet implemented"

# CI/CD helpers
ci-install: ## Install dependencies for CI
	pip install -e ".[dev,test]"

ci-test: ## Run tests in CI environment
	pytest --cov=cartridge --cov-report=xml --cov-report=term

ci-lint: ## Run linting in CI environment
	black --check src/ tests/
	isort --check-only src/ tests/
	flake8 src/ tests/
	mypy src/

# Quick development commands
quick-start: docker-up ## Quick start for development
	@echo "Starting Cartridge development environment..."
	@echo "Services starting up..."
	@sleep 10
	@echo ""
	@echo "🚀 Cartridge is running!"
	@echo ""
	@echo "  API:           http://localhost:8000"
	@echo "  Documentation: http://localhost:8000/docs"
	@echo "  Health Check:  http://localhost:8000/api/v1/health"
	@echo "  Task Monitor:  http://localhost:5555"
	@echo ""
	@echo "To view logs: make docker-logs"
	@echo "To stop:      make docker-down"

# Environment setup
setup-env: ## Set up environment file
	@if [ ! -f .env ]; then \
		cp env.example .env; \
		echo "Created .env file from env.example"; \
		echo "Please edit .env with your configuration"; \
	else \
		echo ".env file already exists"; \
	fi