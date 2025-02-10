from typing import Dict, Any, Optional, List
import logging
import os
import json
from datetime import datetime
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, PreTrainedModel, PreTrainedTokenizer
import hashlib

logger = logging.getLogger(__name__)

class ModelRegistry:
    """Registry for managing LLM models and their versions."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize model registry with configuration."""
        self.config = config
        self.models_dir = config.get('models_dir', 'models')
        self.registry_file = os.path.join(self.models_dir, 'registry.json')
        self.default_device = 'cuda' if torch.cuda.is_available() else 'cpu'
        
        # Create models directory
        os.makedirs(self.models_dir, exist_ok=True)
        
        # Load registry
        self.registry = self._load_registry()
        
        # Active models cache
        self.active_models: Dict[str, Dict[str, Any]] = {}
    
    def register_model(
        self,
        model_name: str,
        model_path: str,
        version: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Register a new model version."""
        try:
            # Validate model files
            if not self._validate_model_files(model_path):
                logger.error(f"Invalid model files at {model_path}")
                return False
            
            # Calculate model hash
            model_hash = self._calculate_model_hash(model_path)
            
            # Prepare model info
            model_info = {
                'name': model_name,
                'version': version,
                'path': model_path,
                'hash': model_hash,
                'registered_at': datetime.now().isoformat(),
                'metadata': metadata or {},
                'status': 'registered'
            }
            
            # Update registry
            if model_name not in self.registry:
                self.registry[model_name] = {'versions': {}}
            
            self.registry[model_name]['versions'][version] = model_info
            self._save_registry()
            
            logger.info(f"Registered model {model_name} version {version}")
            return True
            
        except Exception as e:
            logger.error(f"Error registering model: {e}")
            return False
    
    def load_model(
        self,
        model_name: str,
        version: Optional[str] = None,
        device: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Load a model and its tokenizer."""
        try:
            # Get model info
            model_info = self._get_model_info(model_name, version)
            if not model_info:
                return None
            
            # Check if already loaded
            model_key = f"{model_name}:{model_info['version']}"
            if model_key in self.active_models:
                return self.active_models[model_key]
            
            # Load model and tokenizer
            device = device or self.default_device
            model = AutoModelForCausalLM.from_pretrained(
                model_info['path'],
                device_map='auto' if device == 'cuda' else None,
                torch_dtype=torch.float16 if device == 'cuda' else torch.float32
            )
            tokenizer = AutoTokenizer.from_pretrained(model_info['path'])
            
            # Update model info
            loaded_model = {
                'model': model,
                'tokenizer': tokenizer,
                'info': model_info,
                'device': device,
                'loaded_at': datetime.now().isoformat()
            }
            self.active_models[model_key] = loaded_model
            
            logger.info(f"Loaded model {model_name} version {model_info['version']}")
            return loaded_model
            
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return None
    
    def unload_model(self, model_name: str, version: Optional[str] = None) -> bool:
        """Unload a model from memory."""
        try:
            model_info = self._get_model_info(model_name, version)
            if not model_info:
                return False
            
            model_key = f"{model_name}:{model_info['version']}"
            if model_key in self.active_models:
                del self.active_models[model_key]
                torch.cuda.empty_cache()
                logger.info(f"Unloaded model {model_name} version {model_info['version']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error unloading model: {e}")
            return False
    
    def get_model_info(
        self,
        model_name: str,
        version: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get information about a registered model."""
        return self._get_model_info(model_name, version)
    
    def list_models(self) -> List[Dict[str, Any]]:
        """List all registered models and their versions."""
        models = []
        for model_name, model_data in self.registry.items():
            for version, info in model_data['versions'].items():
                models.append({
                    'name': model_name,
                    'version': version,
                    'registered_at': info['registered_at'],
                    'status': info['status'],
                    'metadata': info['metadata']
                })
        return models
    
    def _load_registry(self) -> Dict[str, Any]:
        """Load model registry from file."""
        if os.path.exists(self.registry_file):
            try:
                with open(self.registry_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading registry: {e}")
        return {}
    
    def _save_registry(self) -> bool:
        """Save model registry to file."""
        try:
            with open(self.registry_file, 'w') as f:
                json.dump(self.registry, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving registry: {e}")
            return False
    
    def _validate_model_files(self, model_path: str) -> bool:
        """Validate required model files exist."""
        required_files = ['config.json', 'pytorch_model.bin', 'tokenizer.json']
        return all(
            os.path.exists(os.path.join(model_path, f))
            for f in required_files
        )
    
    def _calculate_model_hash(self, model_path: str) -> str:
        """Calculate hash of model files for version validation."""
        hasher = hashlib.sha256()
        for root, _, files in os.walk(model_path):
            for file in sorted(files):
                file_path = os.path.join(root, file)
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b''):
                        hasher.update(chunk)
        return hasher.hexdigest()
    
    def _get_model_info(
        self,
        model_name: str,
        version: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get model information from registry."""
        try:
            if model_name not in self.registry:
                logger.error(f"Model {model_name} not found in registry")
                return None
            
            versions = self.registry[model_name]['versions']
            if not version:
                # Get latest version
                version = max(versions.keys())
            
            if version not in versions:
                logger.error(f"Version {version} not found for model {model_name}")
                return None
            
            return versions[version]
            
        except Exception as e:
            logger.error(f"Error getting model info: {e}")
            return None 