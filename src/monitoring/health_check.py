import time
import threading
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import structlog

from .logger import get_logger

logger = get_logger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoints."""
    
    def __init__(self, health_checker: 'HealthChecker', *args, **kwargs):
        self.health_checker = health_checker
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests for health checks."""
        if self.path == "/health":
            self._handle_health_check()
        elif self.path == "/health/ready":
            self._handle_readiness_check()
        elif self.path == "/health/live":
            self._handle_liveness_check()
        else:
            self._send_response(404, {"error": "Not found"})
    
    def _handle_health_check(self):
        """Handle general health check."""
        health_status = self.health_checker.get_health_status()
        status_code = 200 if health_status["healthy"] else 503
        self._send_response(status_code, health_status)
    
    def _handle_readiness_check(self):
        """Handle readiness check."""
        is_ready = self.health_checker.is_ready()
        status_code = 200 if is_ready else 503
        self._send_response(status_code, {"ready": is_ready})
    
    def _handle_liveness_check(self):
        """Handle liveness check."""
        is_alive = self.health_checker.is_alive()
        status_code = 200 if is_alive else 503
        self._send_response(status_code, {"alive": is_alive})
    
    def _send_response(self, status_code: int, data: Dict[str, Any]):
        """Send HTTP response."""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def log_message(self, format, *args):
        """Override to use structured logging."""
        logger.info("Health check request", message=format % args)


class HealthChecker:
    """Health checker for the Event Hub Delta pipeline."""
    
    def __init__(self, port: int = 8081):
        self.port = port
        self.logger = logger.bind(component="health_checker")
        self.checks: Dict[str, Callable[[], bool]] = {}
        self.last_check_results: Dict[str, Dict[str, Any]] = {}
        self.server: Optional[HTTPServer] = None
        self.server_thread: Optional[threading.Thread] = None
        self.is_running = False
        
        self._register_default_checks()
    
    def _register_default_checks(self):
        """Register default health checks."""
        self.register_check("system", self._check_system_health)
    
    def register_check(self, name: str, check_func: Callable[[], bool]):
        """Register a health check function."""
        self.checks[name] = check_func
        self.logger.info("Registered health check", check_name=name)
    
    def _check_system_health(self) -> bool:
        """Basic system health check."""
        try:
            test_data = [0] * 1000
            del test_data
            return True
        except Exception:
            return False
    
    def run_checks(self) -> Dict[str, Dict[str, Any]]:
        """Run all registered health checks."""
        results = {}
        
        for check_name, check_func in self.checks.items():
            start_time = time.time()
            try:
                is_healthy = check_func()
                duration = time.time() - start_time
                
                results[check_name] = {
                    "healthy": is_healthy,
                    "duration_seconds": duration,
                    "timestamp": datetime.utcnow().isoformat(),
                    "error": None
                }
                
            except Exception as e:
                duration = time.time() - start_time
                results[check_name] = {
                    "healthy": False,
                    "duration_seconds": duration,
                    "timestamp": datetime.utcnow().isoformat(),
                    "error": str(e)
                }
                
                self.logger.error(
                    "Health check failed",
                    check_name=check_name,
                    error=str(e)
                )
        
        self.last_check_results = results
        return results
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status."""
        check_results = self.run_checks()
        
        overall_healthy = all(
            result["healthy"] for result in check_results.values()
        )
        
        return {
            "healthy": overall_healthy,
            "timestamp": datetime.utcnow().isoformat(),
            "checks": check_results
        }
    
    def is_ready(self) -> bool:
        """Check if the service is ready to accept requests."""
        health_status = self.get_health_status()
        return health_status["healthy"]
    
    def is_alive(self) -> bool:
        """Check if the service is alive (basic liveness check)."""
        return True
    
    def start_server(self):
        """Start the health check HTTP server."""
        if self.is_running:
            self.logger.warning("Health check server is already running")
            return
        
        try:
            def handler(*args, **kwargs):
                return HealthCheckHandler(self, *args, **kwargs)
            
            self.server = HTTPServer(('', self.port), handler)
            self.server_thread = threading.Thread(
                target=self.server.serve_forever,
                daemon=True
            )
            
            self.server_thread.start()
            self.is_running = True
            
            self.logger.info(
                "Started health check server",
                port=self.port
            )
            
        except Exception as e:
            self.logger.error(
                "Failed to start health check server",
                error=str(e),
                port=self.port
            )
            raise
    
    def stop_server(self):
        """Stop the health check HTTP server."""
        if not self.is_running:
            return
        
        try:
            if self.server:
                self.server.shutdown()
                self.server.server_close()
            
            if self.server_thread:
                self.server_thread.join(timeout=5)
            
            self.is_running = False
            self.logger.info("Stopped health check server")
            
        except Exception as e:
            self.logger.error(
                "Error stopping health check server",
                error=str(e)
            )


class StreamingHealthChecker:
    """Specialized health checker for Spark streaming queries."""
    
    def __init__(self, health_checker: HealthChecker):
        self.health_checker = health_checker
        self.streaming_queries: Dict[str, Any] = {}
        self.logger = logger.bind(component="streaming_health_checker")
        
        self.health_checker.register_check("streaming_queries", self._check_streaming_queries)
    
    def register_streaming_query(self, query_name: str, query):
        """Register a streaming query for health monitoring."""
        self.streaming_queries[query_name] = query
        self.logger.info("Registered streaming query for health monitoring", query_name=query_name)
    
    def _check_streaming_queries(self) -> bool:
        """Check health of all registered streaming queries."""
        if not self.streaming_queries:
            return True
        
        try:
            for query_name, query in self.streaming_queries.items():
                if not query.isActive:
                    self.logger.error(
                        "Streaming query is not active",
                        query_name=query_name,
                        query_id=query.id
                    )
                    return False
                
                last_progress = query.lastProgress
                if last_progress:
                    batch_timestamp = last_progress.get("timestamp")
                    if batch_timestamp:
                        try:
                            from datetime import datetime
                            batch_time = datetime.fromisoformat(batch_timestamp.replace('Z', '+00:00'))
                            time_diff = datetime.utcnow().replace(tzinfo=batch_time.tzinfo) - batch_time
                            
                            if time_diff > timedelta(minutes=5):
                                self.logger.warning(
                                    "Streaming query has stale progress",
                                    query_name=query_name,
                                    last_batch_time=batch_timestamp,
                                    minutes_ago=time_diff.total_seconds() / 60
                                )
                                return False
                        except Exception as e:
                            self.logger.warning(
                                "Could not parse batch timestamp",
                                query_name=query_name,
                                timestamp=batch_timestamp,
                                error=str(e)
                            )
            
            return True
            
        except Exception as e:
            self.logger.error(
                "Error checking streaming queries health",
                error=str(e)
            )
            return False
    
    def get_streaming_status(self) -> Dict[str, Any]:
        """Get detailed status of all streaming queries."""
        status = {}
        
        for query_name, query in self.streaming_queries.items():
            try:
                status[query_name] = {
                    "id": query.id,
                    "name": query.name,
                    "is_active": query.isActive,
                    "last_progress": query.lastProgress,
                    "exception": str(query.exception) if query.exception else None
                }
            except Exception as e:
                status[query_name] = {
                    "error": str(e)
                }
        
        return status
