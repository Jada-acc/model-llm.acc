from typing import Dict, Any, List, Optional, Union, Tuple
import logging
import torch
import psutil
import gc
from datetime import datetime
from functools import lru_cache
from .model_registry import ModelRegistry

logger = logging.getLogger(__name__)

class InferencePipeline:
    """Pipeline for model inference with advanced optimization features."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize inference pipeline."""
        self.config = config
        self.model_registry = ModelRegistry(config.get('model_registry', {}))
        
        # Inference settings
        self.max_batch_size = config.get('max_batch_size', 32)
        self.max_length = config.get('max_length', 512)
        self.default_model = config.get('default_model')
        self.default_version = config.get('default_version', 'latest')
        
        # Performance settings
        self.use_cuda = torch.cuda.is_available()
        self.device = 'cuda' if self.use_cuda else 'cpu'
        self.fp16 = config.get('fp16', self.use_cuda)
        self.cache_models = config.get('cache_models', True)
        
        # Advanced optimization settings
        self.dynamic_batching = config.get('dynamic_batching', True)
        self.min_batch_size = config.get('min_batch_size', 1)
        self.memory_threshold = config.get('memory_threshold', 0.9)  # 90% memory threshold
        self.cache_size = config.get('cache_size', 1024)  # LRU cache size
        self.optimize_for_inference = config.get('optimize_for_inference', True)
        
        # Initialize caches
        self._init_caches()
        
        # Metrics tracking
        self.metrics = {
            'total_requests': 0,
            'total_tokens': 0,
            'avg_latency': 0,
            'cache_hits': 0,
            'memory_usage': [],
            'batch_sizes': []
        }
    
    def _init_caches(self):
        """Initialize various caches for optimization."""
        # LRU cache for generated responses
        self.response_cache = lru_cache(maxsize=self.cache_size)(self._generate_cached)
        
        # Tokenization cache
        self.tokenization_cache = {}
        
        # Model optimization states
        self.optimized_models = set()
    
    async def generate(
        self,
        prompts: Union[str, List[str]],
        model_name: Optional[str] = None,
        version: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Generate responses with optimizations."""
        try:
            start_time = datetime.now()
            
            # Memory management
            self._manage_memory()
            
            # Prepare inputs
            if isinstance(prompts, str):
                prompts = [prompts]
            
            # Check cache first
            if self.cache_models:
                cached_results = self._check_cache(prompts, kwargs)
                if cached_results:
                    return cached_results
            
            # Load and optimize model
            model_data = await self._load_and_optimize_model(model_name, version)
            if not model_data:
                raise ValueError(f"Failed to load model {model_name}:{version}")
            
            # Dynamic batching
            batches = self._dynamic_batch(prompts) if self.dynamic_batching else self._batch_inputs(prompts)
            
            # Process batches
            results = []
            for batch in batches:
                batch_results = await self._generate_optimized(
                    model_data['model'],
                    model_data['tokenizer'],
                    batch,
                    **kwargs
                )
                results.extend(batch_results)
            
            # Update metrics
            self._update_metrics(
                len(prompts),
                results,
                (datetime.now() - start_time).total_seconds()
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error in inference: {e}")
            raise
    
    async def _load_and_optimize_model(
        self,
        model_name: Optional[str],
        version: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Load and optimize model for inference."""
        model_name = model_name or self.default_model
        version = version or self.default_version
        
        model_data = self.model_registry.load_model(
            model_name,
            version,
            self.device
        )
        
        if not model_data:
            return None
        
        model_key = f"{model_name}:{version}"
        if self.optimize_for_inference and model_key not in self.optimized_models:
            model_data['model'] = self._optimize_model(model_data['model'])
            self.optimized_models.add(model_key)
        
        return model_data
    
    def _optimize_model(self, model: torch.nn.Module) -> torch.nn.Module:
        """Apply various optimization techniques to the model."""
        try:
            # Convert to inference mode
            model.eval()
            
            if self.optimize_for_inference:
                # Fuse operations where possible
                if hasattr(torch.ao.quantization, 'fuse_modules'):
                    torch.ao.quantization.fuse_modules(model, ['conv', 'bn', 'relu'])
                
                # Enable torch script if supported
                if hasattr(model, 'torchscript'):
                    model = torch.jit.script(model)
                
                # Optimize memory format
                if self.use_cuda:
                    model = model.to(memory_format=torch.channels_last)
            
            return model
            
        except Exception as e:
            logger.warning(f"Model optimization failed: {e}")
            return model
    
    def _manage_memory(self):
        """Manage system and GPU memory."""
        try:
            if self.use_cuda:
                # GPU memory management
                allocated = torch.cuda.memory_allocated()
                max_allocated = torch.cuda.max_memory_allocated()
                
                if max_allocated > 0 and allocated / max_allocated > self.memory_threshold:
                    torch.cuda.empty_cache()
                    gc.collect()
            
            # System memory management
            if psutil.virtual_memory().percent > self.memory_threshold * 100:
                gc.collect()
            
            # Update metrics
            self.metrics['memory_usage'].append({
                'timestamp': datetime.now().isoformat(),
                'system_memory': psutil.virtual_memory().percent,
                'gpu_memory': torch.cuda.memory_allocated() if self.use_cuda else 0
            })
            
        except Exception as e:
            logger.warning(f"Memory management failed: {e}")
    
    def _dynamic_batch(self, inputs: List[str]) -> List[List[str]]:
        """Dynamically determine optimal batch size based on system resources."""
        try:
            if not self.use_cuda:
                return self._batch_inputs(inputs)
            
            # Calculate available memory
            free_memory = torch.cuda.get_device_properties(0).total_memory - torch.cuda.memory_allocated()
            
            # Estimate memory per input (based on previous runs)
            if self.metrics['batch_sizes']:
                avg_memory_per_input = torch.cuda.memory_allocated() / sum(self.metrics['batch_sizes'])
                optimal_batch_size = min(
                    int(free_memory / avg_memory_per_input * 0.8),  # 80% of theoretical max
                    self.max_batch_size
                )
                optimal_batch_size = max(optimal_batch_size, self.min_batch_size)
            else:
                optimal_batch_size = self.max_batch_size
            
            # Update metrics
            self.metrics['batch_sizes'].append(optimal_batch_size)
            
            # Create batches
            return [
                inputs[i:i + optimal_batch_size]
                for i in range(0, len(inputs), optimal_batch_size)
            ]
            
        except Exception as e:
            logger.warning(f"Dynamic batching failed: {e}")
            return self._batch_inputs(inputs)
    
    @lru_cache(maxsize=1024)
    def _generate_cached(
        self,
        prompt: str,
        model_name: str,
        version: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Cached version of single prompt generation."""
        return {
            'prompt': prompt,
            'generated': None,  # Placeholder for actual generation
            'cached': True
        }
    
    def _check_cache(
        self,
        prompts: List[str],
        kwargs: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """Check cache for existing generations."""
        try:
            cache_key = (
                tuple(prompts),
                kwargs.get('model_name', self.default_model),
                kwargs.get('version', self.default_version),
                frozenset(kwargs.items())
            )
            
            if cache_key in self.tokenization_cache:
                self.metrics['cache_hits'] += 1
                return self.tokenization_cache[cache_key]
            
            return None
            
        except Exception as e:
            logger.warning(f"Cache check failed: {e}")
            return None
    
    def get_detailed_metrics(self) -> Dict[str, Any]:
        """Get detailed metrics including memory and batch statistics."""
        metrics = self.get_metrics()
        
        # Add memory statistics
        if self.metrics['memory_usage']:
            recent_memory = self.metrics['memory_usage'][-10:]  # Last 10 measurements
            metrics['memory_stats'] = {
                'avg_system_memory': sum(m['system_memory'] for m in recent_memory) / len(recent_memory),
                'avg_gpu_memory': sum(m['gpu_memory'] for m in recent_memory) / len(recent_memory) if self.use_cuda else 0
            }
        
        # Add batch statistics
        if self.metrics['batch_sizes']:
            metrics['batch_stats'] = {
                'avg_batch_size': sum(self.metrics['batch_sizes']) / len(self.metrics['batch_sizes']),
                'min_batch_size': min(self.metrics['batch_sizes']),
                'max_batch_size': max(self.metrics['batch_sizes'])
            }
        
        return metrics
    
    def _update_metrics(
        self,
        num_requests: int,
        results: List[Dict[str, Any]],
        latency: float
    ):
        """Update inference metrics."""
        self.metrics['total_requests'] += num_requests
        self.metrics['total_tokens'] += sum(r['tokens'] for r in results)
        
        # Update running average latency
        total_requests = self.metrics['total_requests']
        self.metrics['avg_latency'] = (
            (self.metrics['avg_latency'] * (total_requests - num_requests) + latency)
            / total_requests
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current inference metrics."""
        return {
            **self.metrics,
            'device': self.device,
            'fp16': self.fp16,
            'timestamp': datetime.now().isoformat()
        }

    def _batch_inputs(self, inputs: List[str]) -> List[List[str]]:
        """Split inputs into batches."""
        for i in range(0, len(inputs), self.max_batch_size):
            yield inputs[i:i + self.max_batch_size]
    
    async def _generate_optimized(
        self,
        model: Any,
        tokenizer: Any,
        prompts: List[str],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Generate responses for a batch of prompts with optimizations."""
        try:
            # Prepare generation config
            gen_kwargs = {
                'max_length': kwargs.get('max_length', self.max_length),
                'num_return_sequences': kwargs.get('num_sequences', 1),
                'temperature': kwargs.get('temperature', 0.7),
                'top_p': kwargs.get('top_p', 0.9),
                'top_k': kwargs.get('top_k', 50),
                'do_sample': kwargs.get('do_sample', True),
                'pad_token_id': tokenizer.pad_token_id,
                'eos_token_id': tokenizer.eos_token_id
            }
            
            # Tokenize inputs
            inputs = tokenizer(
                prompts,
                padding=True,
                truncation=True,
                return_tensors='pt'
            ).to(self.device)
            
            # Generate
            with torch.inference_mode():
                if self.fp16 and self.use_cuda:
                    with torch.cuda.amp.autocast():
                        outputs = model.generate(**inputs, **gen_kwargs)
                else:
                    outputs = model.generate(**inputs, **gen_kwargs)
            
            # Decode outputs
            decoded = tokenizer.batch_decode(
                outputs,
                skip_special_tokens=True
            )
            
            # Format results
            results = []
            for i, prompt in enumerate(prompts):
                results.append({
                    'prompt': prompt,
                    'generated': decoded[i],
                    'tokens': len(outputs[i]),
                    'finish_reason': 'length' if len(outputs[i]) >= gen_kwargs['max_length'] else 'stop'
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error in batch generation: {e}")
            raise 