# Makefile for cartridge-warp development

.PHONY: help install install-dev test test-coverage lint format type-check clean build docker-build docker-run

# Default target
help:
	@echo "Available targets:"
	@echo "  install       Install package for production"
	@echo "  install-dev   Install package with development dependencies"
	@echo "  test          Run tests"
	@echo "  test-coverage Run tests with coverage report"
	@echo "  lint          Run linting checks"
	@echo "  format        Format code with black"
	@echo "  type-check    Run type checking with mypy"
	@echo "  clean         Clean build artifacts"
	@echo "  build         Build distribution packages"
	@echo "  docker-build  Build Docker image"
	@echo "  docker-run    Run Docker container"

# Installation targets
install:
	pip install -e .

install-dev:
	pip install -e ".[dev,test]"

# Testing targets
test:
	pytest

test-coverage:
	pytest --cov=cartridge_warp --cov-report=html --cov-report=term

# Code quality targets
lint:
	ruff check src/ tests/
	black --check src/ tests/

format:
	black src/ tests/
	ruff check --fix src/ tests/

type-check:
	mypy src/

# Utility targets
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

# Docker targets
docker-build:
	docker build -t cartridge-warp:latest .

docker-run:
	docker run --rm -it \
		-p 8080:8080 \
		-v $(PWD)/examples:/app/examples \
		cartridge-warp:latest \
		cartridge-warp --config examples/mongodb-to-postgresql-single.yaml validate

# Example usage targets
example-validate:
	cartridge-warp validate --config examples/mongodb-to-postgresql-single.yaml

example-init:
	cartridge-warp init

# Development setup
setup-dev: install-dev
	pre-commit install
