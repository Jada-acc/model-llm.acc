from typing import Dict, Any, List, Optional, Union
import logging
from pathlib import Path
import torch
import asyncio
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from transformers import AutoModelForCausalLM, AutoTokenizer
import uvicorn
from .monitoring import track_request, MetricsCollector
from prometheus_client import make_asgi_app

from ..model_registry import ModelRegistry
from ..inference_pipeline import InferencePipeline

logger = logging.getLogger(__name__)

class GenerationRequest(BaseModel):
    """Request model for text generation."""
    prompt: Union[str, List[str]] = Field(..., description="Input prompt(s) for generation")
    model_name: Optional[str] = Field(None, description="Model name to use")
    version: Optional[str] = Field(None, description="Model version to use")
    max_length: Optional[int] = Field(512, description="Maximum length of generated text")
    temperature: Optional[float] = Field(0.7, description="Sampling temperature")
    top_p: Optional[float] = Field(0.9, description="Top-p sampling parameter")
    top_k: Optional[int] = Field(50, description="Top-k sampling parameter")
    num_return_sequences: Optional[int] = Field(1, description="Number of sequences to return")

class GenerationResponse(BaseModel):
    """Response model for text generation."""
    generated_text: Union[str, List[str]]
    model_name: str
    version: str
    generation_time: float
    token_count: int

class ModelServer:
    """Server for model deployment and inference."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize model server."""
        self.config = config
        self.model_registry = ModelRegistry(config.get('model_registry', {}))
        self.inference_pipeline = InferencePipeline(config.get('inference_pipeline', {}))
        
        # Server settings
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 8000)
        self.workers = config.get('workers', 1)
        
        # Model settings
        self.default_model = config.get('default_model')
        self.default_version = config.get('default_version', 'latest')
        self.max_batch_size = config.get('max_batch_size', 32)
        
        # Initialize FastAPI app
        self.app = FastAPI(
            title="LLM Model Server",
            description="API for LLM inference",
            version="1.0.0"
        )
        self._setup_routes()
        
        # Model cache
        self._model_cache = {}
        self._cache_lock = asyncio.Lock()
        
        # Add metrics endpoint
        metrics_app = make_asgi_app()
        self.app.mount("/metrics", metrics_app)
        
        # Setup metrics collection
        self.metrics_collector = MetricsCollector()
    
    def _setup_routes(self):
        """Setup API routes."""
        @self.app.post("/generate", response_model=GenerationResponse)
        async def generate(request: GenerationRequest, background_tasks: BackgroundTasks):
            try:
                # Get model and version
                model_name = request.model_name or self.default_model
                version = request.version or self.default_version
                
                # Generate text
                result = await self.inference_pipeline.generate(
                    prompt=request.prompt,
                    model_name=model_name,
                    version=version,
                    max_length=request.max_length,
                    temperature=request.temperature,
                    top_p=request.top_p,
                    top_k=request.top_k,
                    num_return_sequences=request.num_return_sequences
                )
                
                # Schedule cache cleanup
                background_tasks.add_task(self._cleanup_cache)
                
                return GenerationResponse(
                    generated_text=[r['generated'] for r in result],
                    model_name=model_name,
                    version=version,
                    generation_time=sum(r.get('generation_time', 0) for r in result),
                    token_count=sum(r.get('tokens', 0) for r in result)
                )
                
            except Exception as e:
                logger.error(f"Generation error: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/models")
        async def list_models():
            """List available models."""
            try:
                return {
                    'models': self.model_registry.list_models(),
                    'default_model': self.default_model,
                    'default_version': self.default_version
                }
            except Exception as e:
                logger.error(f"Error listing models: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {
                'status': 'healthy',
                'gpu_available': torch.cuda.is_available(),
                'loaded_models': list(self._model_cache.keys())
            }
    
    @track_request("generate")
    async def generate(self, request: GenerationRequest):
        """Generate text from request."""
        try:
            model_name = request.model_name or self.default_model
            version = request.version or self.default_version
            
            result = await self.inference_pipeline.generate(
                prompt=request.prompt,
                model_name=model_name,
                version=version,
                max_length=request.max_length,
                temperature=request.temperature,
                top_p=request.top_p,
                top_k=request.top_k,
                num_return_sequences=request.num_return_sequences
            )
            
            return GenerationResponse(
                generated_text=[r['generated'] for r in result],
                model_name=model_name,
                version=version,
                generation_time=sum(r.get('generation_time', 0) for r in result),
                token_count=sum(r.get('tokens', 0) for r in result)
            )
        except Exception as e:
            logger.error(f"Generation error: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def _cleanup_cache(self):
        """Clean up model cache."""
        async with self._cache_lock:
            current_memory = torch.cuda.memory_allocated() if torch.cuda.is_available() else 0
            if current_memory > self.config.get('max_memory', 0.9) * torch.cuda.max_memory_allocated():
                self._model_cache.clear()
                torch.cuda.empty_cache()
            self.metrics_collector.update_cache_metrics(len(self._model_cache))
            self.metrics_collector.update_gpu_metrics()
    
    def start(self):
        """Start the model server."""
        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            workers=self.workers
        )

class LoadBalancer:
    """Load balancer for multiple model servers."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize load balancer."""
        self.config = config
        self.servers = []
        self.current_server = 0
        
        # Initialize servers
        for server_config in config.get('servers', []):
            self.servers.append(ModelServer(server_config))
    
    def get_next_server(self) -> ModelServer:
        """Get next available server (round-robin)."""
        server = self.servers[self.current_server]
        self.current_server = (self.current_server + 1) % len(self.servers)
        return server
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of all servers."""
        results = {}
        for i, server in enumerate(self.servers):
            try:
                health = await server.app.get("/health")
                results[f'server_{i}'] = health
            except Exception as e:
                results[f'server_{i}'] = {'status': 'unhealthy', 'error': str(e)}
        return results