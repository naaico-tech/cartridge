"""Health check endpoints."""

from typing import Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from cartridge.core.config import settings
from cartridge.core.database import get_db
from cartridge.core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/health")
async def health_check(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Basic health check endpoint."""
    try:
        # Test database connection
        db.execute("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        logger.error("Database health check failed", error=str(e))
        db_status = "unhealthy"
    
    return {
        "status": "healthy" if db_status == "healthy" else "unhealthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": settings.app.version,
        "environment": settings.app.environment,
        "services": {
            "database": db_status,
        }
    }


@router.get("/health/detailed")
async def detailed_health_check(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Detailed health check with component status."""
    components = {}
    
    # Database check
    try:
        result = db.execute("SELECT version()").fetchone()
        components["database"] = {
            "status": "healthy",
            "version": str(result[0]) if result else "unknown",
        }
    except Exception as e:
        logger.error("Database detailed health check failed", error=str(e))
        components["database"] = {
            "status": "unhealthy",
            "error": str(e),
        }
    
    # Redis check (TODO: implement when Redis is set up)
    components["redis"] = {
        "status": "not_implemented",
        "message": "Redis health check not yet implemented"
    }
    
    # AI services check (TODO: implement when AI services are set up)
    components["ai_services"] = {
        "status": "not_implemented",
        "message": "AI services health check not yet implemented"
    }
    
    overall_status = "healthy" if all(
        comp.get("status") in ["healthy", "not_implemented"] 
        for comp in components.values()
    ) else "unhealthy"
    
    return {
        "status": overall_status,
        "timestamp": datetime.utcnow().isoformat(),
        "version": settings.app.version,
        "environment": settings.app.environment,
        "components": components,
    }