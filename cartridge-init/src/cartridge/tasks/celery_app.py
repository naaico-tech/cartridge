"""Celery application configuration."""

from celery import Celery

from cartridge.core.config import settings

# Create Celery app
celery_app = Celery(
    "cartridge",
    broker=settings.redis.url,
    backend=settings.redis.url,
    include=[
        "cartridge.tasks.scan_tasks",
        "cartridge.tasks.generation_tasks", 
        "cartridge.tasks.test_tasks",
    ]
)

# Configure Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
)

# Task routing
celery_app.conf.task_routes = {
    "cartridge.tasks.scan_tasks.*": {"queue": "scan"},
    "cartridge.tasks.generation_tasks.*": {"queue": "generation"},
    "cartridge.tasks.test_tasks.*": {"queue": "test"},
}