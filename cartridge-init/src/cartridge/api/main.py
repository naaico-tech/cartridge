"""Main FastAPI application."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from cartridge.api.routes import health, projects, scanner
from cartridge.core.config import settings
from cartridge.core.database import close_db, init_db
from cartridge.core.logging import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager."""
    # Startup
    logger.info("Starting Cartridge application", version=settings.app.version)
    
    try:
        await init_db()
        logger.info("Application startup completed")
        yield
    except Exception as e:
        logger.error("Failed to start application", error=str(e))
        raise
    finally:
        # Shutdown
        logger.info("Shutting down Cartridge application")
        await close_db()
        logger.info("Application shutdown completed")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    
    app = FastAPI(
        title=settings.app.name,
        version=settings.app.version,
        description="AI-powered dbt model generator that scans data sources and creates optimized dbt models",
        debug=settings.app.debug,
        lifespan=lifespan,
    )
    
    # Add middleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    
    if settings.is_development():
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    else:
        # More restrictive CORS for production
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["https://yourdomain.com"],  # Update with your domain
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE"],
            allow_headers=["*"],
        )
    
    # Include routers
    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    app.include_router(scanner.router, prefix="/api/v1/scanner", tags=["scanner"])
    app.include_router(projects.router, prefix="/api/v1/projects", tags=["projects"])
    
    return app


# Create the application instance
app = create_app()


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Welcome to Cartridge - AI-powered dbt model generator",
        "version": settings.app.version,
        "docs": "/docs",
        "health": "/api/v1/health",
    }