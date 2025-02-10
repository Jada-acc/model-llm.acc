from typing import Dict, Any
import time
from prometheus_client import Counter, Histogram, Gauge
from functools import wraps

# Metrics
REQUESTS = Counter(
    'llm_requests_total',
    'Total number of requests',
    ['endpoint', 'model', 'status']
)

LATENCY = Histogram(
    'llm_request_duration_seconds',
    'Request duration in seconds',
    ['endpoint', 'model']
)

GPU_MEMORY = Gauge(
    'llm_gpu_memory_used_bytes',
    'GPU memory usage in bytes'
)

MODEL_CACHE_SIZE = Gauge(
    'llm_model_cache_size',
    'Number of models in cache'
)

def track_request(endpoint: str):
    """Decorator to track request metrics."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            model_name = kwargs.get('model_name', 'default')
            
            try:
                result = await func(*args, **kwargs)
                REQUESTS.labels(
                    endpoint=endpoint,
                    model=model_name,
                    status='success'
                ).inc()
                return result
            except Exception as e:
                REQUESTS.labels(
                    endpoint=endpoint,
                    model=model_name,
                    status='error'
                ).inc()
                raise
            finally:
                LATENCY.labels(
                    endpoint=endpoint,
                    model=model_name
                ).observe(time.time() - start_time)
        return wrapper
    return decorator

class MetricsCollector:
    """Collector for system and model metrics."""
    
    @staticmethod
    def update_gpu_metrics():
        """Update GPU metrics."""
        if torch.cuda.is_available():
            memory_used = torch.cuda.memory_allocated()
            GPU_MEMORY.set(memory_used)
    
    @staticmethod
    def update_cache_metrics(cache_size: int):
        """Update cache metrics."""
        MODEL_CACHE_SIZE.set(cache_size) 