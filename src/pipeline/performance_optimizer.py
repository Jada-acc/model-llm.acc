import time
from typing import Dict, Any, Callable
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

class PerformanceOptimizer:
    """Optimize pipeline performance."""
    
    def __init__(self):
        self.performance_metrics = {}
        
    @lru_cache(maxsize=128)
    def cache_expensive_operation(self, operation_key: str, *args) -> Any:
        """Cache results of expensive operations."""
        return self.expensive_operation(*args)
    
    def measure_execution_time(self, func: Callable) -> Callable:
        """Decorator to measure function execution time."""
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # Store metrics
            self.performance_metrics[func.__name__] = {
                'last_execution_time': execution_time,
                'timestamp': time.time()
            }
            
            return result
        return wrapper 