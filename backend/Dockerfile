# Multi-stage build for Cartridge
FROM python:3.11-slim as builder

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY pyproject.toml /tmp/
RUN pip install --upgrade pip setuptools wheel
RUN pip install /tmp/

# Production stage
FROM python:3.11-slim as production

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PATH="/opt/venv/bin:$PATH"

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create app user
RUN groupadd -r cartridge && useradd -r -g cartridge cartridge

# Create app directory
WORKDIR /app

# Copy application code
COPY src/ /app/src/
COPY alembic.ini /app/
COPY alembic/ /app/alembic/

# Create necessary directories
RUN mkdir -p /app/uploads /app/output /app/temp /app/logs

# Change ownership to app user
RUN chown -R cartridge:cartridge /app

# Switch to app user
USER cartridge

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

# Default command
CMD ["uvicorn", "cartridge.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Development stage
FROM production as development

# Switch back to root for development tools
USER root

# Install development dependencies
RUN pip install -e ".[dev,test,docs]"

# Install additional development tools
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Switch back to app user
USER cartridge

# Default command for development
CMD ["uvicorn", "cartridge.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]