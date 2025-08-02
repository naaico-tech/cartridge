#!/bin/bash

# Test runner script for Cartridge backend
# This script provides various test running options with proper setup

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if database is running
check_database() {
    print_status "Checking database connection..."
    
    if ! pg_isready -h localhost -p 5432 -U cartridge -d cartridge_test > /dev/null 2>&1; then
        print_warning "Test database is not running. Starting with Docker..."
        
        # Check if Docker is running
        if ! docker info > /dev/null 2>&1; then
            print_error "Docker is not running. Please start Docker first."
            exit 1
        fi
        
        # Start test database
        docker run -d --name cartridge_test_db \
            -e POSTGRES_DB=cartridge_test \
            -e POSTGRES_USER=cartridge \
            -e POSTGRES_PASSWORD=cartridge \
            -p 5432:5432 \
            postgres:15-alpine > /dev/null 2>&1 || true
        
        # Wait for database to be ready
        print_status "Waiting for database to be ready..."
        for i in {1..30}; do
            if pg_isready -h localhost -p 5432 -U cartridge -d cartridge_test > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        
        if ! pg_isready -h localhost -p 5432 -U cartridge -d cartridge_test > /dev/null 2>&1; then
            print_error "Database failed to start"
            exit 1
        fi
    fi
    
    print_success "Database is ready"
}

# Function to check if Redis is running
check_redis() {
    print_status "Checking Redis connection..."
    
    if ! redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; then
        print_warning "Redis is not running. Starting with Docker..."
        
        # Start Redis
        docker run -d --name cartridge_test_redis \
            -p 6379:6379 \
            redis:7-alpine > /dev/null 2>&1 || true
        
        # Wait for Redis to be ready
        print_status "Waiting for Redis to be ready..."
        for i in {1..10}; do
            if redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        
        if ! redis-cli -h localhost -p 6379 ping > /dev/null 2>&1; then
            print_error "Redis failed to start"
            exit 1
        fi
    fi
    
    print_success "Redis is ready"
}

# Function to setup test environment
setup_test_env() {
    print_status "Setting up test environment..."
    
    # Set test environment variables
    export CARTRIDGE_ENVIRONMENT=test
    export CARTRIDGE_DB_URL=postgresql://cartridge:cartridge@localhost:5432/cartridge_test
    export CARTRIDGE_REDIS_URL=redis://localhost:6379/1
    export CARTRIDGE_LOG_LEVEL=WARNING
    
    # Create test directories
    mkdir -p uploads output temp logs
    
    print_success "Test environment ready"
}

# Function to cleanup test environment
cleanup() {
    print_status "Cleaning up test environment..."
    
    # Stop test containers if they were started by this script
    docker stop cartridge_test_db cartridge_test_redis > /dev/null 2>&1 || true
    docker rm cartridge_test_db cartridge_test_redis > /dev/null 2>&1 || true
    
    # Clean up test directories
    rm -rf htmlcov/ .coverage .pytest_cache/
    
    print_success "Cleanup complete"
}

# Function to run specific test suite
run_tests() {
    local test_type=$1
    local extra_args=${@:2}
    
    print_status "Running $test_type tests..."
    
    case $test_type in
        "unit")
            pytest tests/unit/ -v $extra_args
            ;;
        "integration")
            pytest tests/integration/ -v $extra_args
            ;;
        "performance")
            pytest tests/performance/ -v -m slow $extra_args
            ;;
        "models")
            pytest tests/unit/test_models.py -v $extra_args
            ;;
        "api")
            pytest tests/unit/test_api.py -v $extra_args
            ;;
        "tasks")
            pytest tests/unit/test_tasks.py -v $extra_args
            ;;
        "config")
            pytest tests/unit/test_config.py -v $extra_args
            ;;
        "database")
            pytest tests/test_database_setup.py -v $extra_args
            ;;
        "all")
            pytest $extra_args
            ;;
        "coverage")
            pytest --cov=cartridge --cov-report=html --cov-report=term $extra_args
            ;;
        *)
            print_error "Unknown test type: $test_type"
            echo "Available types: unit, integration, performance, models, api, tasks, config, database, all, coverage"
            exit 1
            ;;
    esac
}

# Main script logic
main() {
    local test_type=${1:-"all"}
    local skip_setup=${2:-"false"}
    
    # Handle special flags
    if [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
        echo "Usage: $0 [test_type] [skip_setup]"
        echo ""
        echo "Test types:"
        echo "  unit         - Run unit tests"
        echo "  integration  - Run integration tests"
        echo "  performance  - Run performance tests"
        echo "  models       - Run model tests only"
        echo "  api          - Run API tests only"
        echo "  tasks        - Run task tests only"
        echo "  config       - Run configuration tests only"
        echo "  database     - Run database tests only"
        echo "  all          - Run all tests (default)"
        echo "  coverage     - Run tests with coverage report"
        echo ""
        echo "Options:"
        echo "  --skip-setup - Skip database and Redis setup"
        echo "  --cleanup    - Only run cleanup"
        echo "  --help, -h   - Show this help"
        exit 0
    fi
    
    if [[ "$1" == "--cleanup" ]]; then
        cleanup
        exit 0
    fi
    
    # Trap cleanup on exit
    trap cleanup EXIT
    
    print_status "Starting Cartridge backend test runner..."
    
    # Setup test environment unless skipped
    if [[ "$skip_setup" != "--skip-setup" ]]; then
        setup_test_env
        check_database
        check_redis
    fi
    
    # Run tests
    run_tests $test_type
    
    print_success "All tests completed successfully!"
}

# Run main function with all arguments
main "$@"