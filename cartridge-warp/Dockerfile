# Example Dockerfile for cartridge-warp

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY pyproject.toml ./
RUN pip install -e .

# Copy application code
COPY src/ ./src/
COPY examples/ ./examples/

# Create non-root user
RUN useradd --create-home --shell /bin/bash cartridge
RUN chown -R cartridge:cartridge /app
USER cartridge

# Expose metrics port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/metrics || exit 1

# Default command
CMD ["cartridge-warp", "--help"]
