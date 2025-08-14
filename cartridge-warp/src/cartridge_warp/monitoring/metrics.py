"""Prometheus metrics collection for cartridge-warp."""

from typing import Dict, Optional
from prometheus_client import Counter, Gauge, Histogram, start_http_server, CollectorRegistry
import structlog

logger = structlog.get_logger(__name__)


class MetricsCollector:
    """Collects and exposes Prometheus metrics for cartridge-warp."""
    
    def __init__(self, prometheus_config):
        """Initialize metrics collector."""
        self.config = prometheus_config
        self.registry = CollectorRegistry()
        self._server = None
        
        # Initialize metrics
        self._init_metrics()
    
    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        # Throughput metrics
        self.records_processed_total = Counter(
            'cartridge_warp_records_processed_total',
            'Total number of records processed',
            ['database', 'schema', 'table', 'operation'],
            registry=self.registry
        )
        
        self.processing_rate = Gauge(
            'cartridge_warp_processing_rate',
            'Current processing rate (records/second)',
            ['database', 'schema', 'table'],
            registry=self.registry
        )
        
        self.lag_seconds = Gauge(
            'cartridge_warp_lag_seconds',
            'Processing lag in seconds',
            ['database', 'schema', 'table'],
            registry=self.registry
        )
        
        # Health metrics
        self.sync_status = Gauge(
            'cartridge_warp_sync_status',
            'Sync status (1=active, 0=inactive, -1=error)',
            ['database', 'schema', 'table'],
            registry=self.registry
        )
        
        self.last_successful_sync = Gauge(
            'cartridge_warp_last_successful_sync',
            'Timestamp of last successful sync',
            ['database', 'schema', 'table'],
            registry=self.registry
        )
        
        self.error_count_total = Counter(
            'cartridge_warp_error_count_total',
            'Total number of errors',
            ['database', 'schema', 'table', 'error_type'],
            registry=self.registry
        )
        
        # Performance metrics
        self.batch_processing_duration = Histogram(
            'cartridge_warp_batch_processing_duration_seconds',
            'Time spent processing batches',
            ['database', 'schema', 'table'],
            registry=self.registry
        )
        
        self.queue_size = Gauge(
            'cartridge_warp_queue_size',
            'Current queue size',
            ['database', 'schema', 'table'],
            registry=self.registry
        )
        
        # Schema evolution metrics
        self.schema_changes_total = Counter(
            'cartridge_warp_schema_changes_total',
            'Total number of schema changes',
            ['database', 'schema', 'table', 'change_type'],
            registry=self.registry
        )
    
    async def start_server(self):
        """Start the Prometheus metrics server."""
        if not self.config.enabled:
            return
        
        logger.info("Starting Prometheus metrics server", port=self.config.port)
        
        # Note: start_http_server doesn't support custom registry in all versions
        # This is a simplified implementation
        self._server = start_http_server(self.config.port)
    
    async def stop_server(self):
        """Stop the Prometheus metrics server."""
        if self._server:
            logger.info("Stopping Prometheus metrics server")
            # Server stopping logic would go here
            self._server = None
    
    def record_records_processed(self, schema_name: str, count: int, table_name: str = "", operation: str = "sync"):
        """Record the number of records processed."""
        self.records_processed_total.labels(
            database="", schema=schema_name, table=table_name, operation=operation
        ).inc(count)
    
    def record_schema_status(self, schema_name: str, status: str):
        """Record schema sync status."""
        status_value = {"active": 1, "inactive": 0, "error": -1}.get(status, 0)
        self.sync_status.labels(database="", schema=schema_name, table="").set(status_value)
    
    def record_table_sync(self, schema_name: str, table_name: str, status: str, count: int):
        """Record table sync operation."""
        status_value = {"success": 1, "error": -1}.get(status, 0)
        self.sync_status.labels(database="", schema=schema_name, table=table_name).set(status_value)
        
        if status == "success":
            self.records_processed_total.labels(
                database="", schema=schema_name, table=table_name, operation="sync"
            ).inc(count)
