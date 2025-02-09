import time
from typing import Dict, Any, Callable, List, Optional
import logging
from functools import lru_cache
from datetime import datetime
import json
import zlib
from concurrent.futures import ThreadPoolExecutor
import hashlib
import statistics

logger = logging.getLogger(__name__)

class PerformanceOptimizer:
    """Optimize data processing for LLM pipeline."""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.optimization_strategies = {
            'llm_input': self._optimize_llm_input,
            'llm_output': self._optimize_llm_output,
            'model_metrics': self._optimize_metrics,
            'training_data': self._optimize_training_data
        }
        self.batch_size = 1000
        self.compression_threshold = 1000
        self.cache_enabled = True
        self.performance_metrics = {}
        
    def optimize_for_loading(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Optimize data for efficient loading with batching."""
        try:
            # Split into batches
            batches = [data[i:i + self.batch_size] for i in range(0, len(data), self.batch_size)]
            optimized_data = []
            
            # Process batches in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for batch in batches:
                    future_to_batch = {executor.submit(self._optimize_batch, batch): batch}
                    for future in future_to_batch:
                        try:
                            result = future.result()
                            optimized_data.extend(result)
                        except Exception as e:
                            logger.error(f"Batch optimization failed: {str(e)}")
            
            # Apply global optimizations
            optimized_data = self._apply_global_optimizations(optimized_data)
            
            logger.info(f"Optimized {len(data)} records in {len(batches)} batches")
            return optimized_data
            
        except Exception as e:
            logger.error(f"Optimization failed: {str(e)}")
            return data
    
    def _optimize_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Optimize a batch of records."""
        return [self._optimize_record(record) for record in batch]
    
    def _apply_global_optimizations(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply optimizations across the entire dataset."""
        try:
            # Deduplicate similar prompts
            prompt_map = {}
            for record in data:
                if 'prompt' in record:
                    prompt_hash = self._compute_similarity_hash(record['prompt'])
                    if prompt_hash in prompt_map:
                        record['prompt_ref'] = prompt_map[prompt_hash]['id']
                        del record['prompt']
                    else:
                        record['id'] = len(prompt_map)
                        prompt_map[prompt_hash] = record
            
            # Optimize numeric fields
            self._optimize_numeric_fields(data)
            
            return data
            
        except Exception as e:
            logger.error(f"Global optimization failed: {str(e)}")
            return data
    
    def _compute_similarity_hash(self, text: str) -> str:
        """Compute a similarity hash for text deduplication."""
        # Implement locality-sensitive hashing or similar technique
        return hashlib.md5(text.encode()).hexdigest()
    
    def _optimize_numeric_fields(self, data: List[Dict[str, Any]]) -> None:
        """Optimize numeric fields across the dataset."""
        numeric_fields = {'latency', 'tokens_per_second', 'memory_usage', 'error_rate'}
        
        for field in numeric_fields:
            values = [record[field] for record in data if field in record]
            if values:
                mean = sum(values) / len(values)
                std = statistics.stdev(values) if len(values) > 1 else 0
                
                # Store statistics for later use
                self.performance_metrics[f'{field}_stats'] = {
                    'mean': mean,
                    'std': std,
                    'min': min(values),
                    'max': max(values)
                }
    
    def _optimize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize a single record."""
        try:
            # Determine record type and apply specific optimizations
            record_type = self._determine_record_type(record)
            if record_type in self.optimization_strategies:
                return self.optimization_strategies[record_type](record)
            
            return record
            
        except Exception as e:
            logger.warning(f"Record optimization failed: {str(e)}")
            return record
    
    def _optimize_llm_input(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize LLM input data."""
        try:
            # Compress large text fields
            if len(record.get('prompt', '')) > 1000:
                record['prompt'] = self._compress_text(record['prompt'])
            
            # Convert parameters to compact format
            if 'parameters' in record:
                record['parameters'] = json.dumps(record['parameters'])
            
            return record
            
        except Exception as e:
            logger.warning(f"LLM input optimization failed: {str(e)}")
            return record
    
    def _optimize_llm_output(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize LLM output data."""
        try:
            # Compress response text
            if len(record.get('response', '')) > 1000:
                record['response'] = self._compress_text(record['response'])
            
            # Optimize metrics storage
            if 'metrics' in record:
                record['metrics'] = json.dumps(record['metrics'])
            
            return record
            
        except Exception as e:
            logger.warning(f"LLM output optimization failed: {str(e)}")
            return record
    
    def _optimize_metrics(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize metrics data."""
        try:
            # Round floating point values
            for key in ['latency', 'tokens_per_second', 'memory_usage', 'error_rate']:
                if key in record:
                    record[key] = round(record[key], 4)
            
            return record
            
        except Exception as e:
            logger.warning(f"Metrics optimization failed: {str(e)}")
            return record
    
    def _optimize_training_data(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize training data."""
        try:
            # Compress input/output text
            for field in ['input', 'output']:
                if len(record.get(field, '')) > 1000:
                    record[field] = self._compress_text(record[field])
            
            # Optimize metadata storage
            if 'metadata' in record:
                record['metadata'] = json.dumps(record['metadata'])
            
            return record
            
        except Exception as e:
            logger.warning(f"Training data optimization failed: {str(e)}")
            return record
    
    def _determine_record_type(self, record: Dict[str, Any]) -> Optional[str]:
        """Determine the type of record based on its fields."""
        if 'prompt' in record and 'parameters' in record:
            return 'llm_input'
        elif 'response' in record and 'tokens' in record:
            return 'llm_output'
        elif 'latency' in record and 'tokens_per_second' in record:
            return 'model_metrics'
        elif 'input' in record and 'output' in record:
            return 'training_data'
        return None
    
    def _compress_text(self, text: str) -> str:
        """Compress text data."""
        try:
            compressed = zlib.compress(text.encode())
            return compressed.hex()
        except Exception:
            return text
    
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