import unittest
from unittest.mock import Mock, patch, mock_open
import os
import json
import tempfile
import shutil
from datetime import datetime
import torch
from src.llm.model_registry import ModelRegistry

class TestModelRegistry(unittest.TestCase):
    """Test suite for ModelRegistry class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directory for test models
        self.test_dir = tempfile.mkdtemp()
        self.models_dir = os.path.join(self.test_dir, 'models')
        
        # Test configuration
        self.config = {
            'models_dir': self.models_dir,
            'default_device': 'cpu'
        }
        
        # Create test model files
        self.model_path = os.path.join(self.test_dir, 'test_model')
        os.makedirs(self.model_path)
        
        # Create dummy model files
        self._create_dummy_model_files(self.model_path)
        
        # Initialize registry
        self.registry = ModelRegistry(self.config)
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)
    
    def _create_dummy_model_files(self, path: str):
        """Create dummy model files for testing."""
        files = {
            'config.json': json.dumps({
                "architectures": ["GPT2LMHeadModel"],
                "model_type": "gpt2",
                "vocab_size": 50257
            }),
            'pytorch_model.bin': b'dummy_model_data',
            'tokenizer.json': json.dumps({
                "model_type": "gpt2",
                "vocab_size": 50257,
                "merges": [],
                "vocab": {}
            }),
            'tokenizer_config.json': json.dumps({
                "model_type": "gpt2",
                "tokenizer_class": "GPT2Tokenizer"
            })
        }
        
        for filename, content in files.items():
            file_path = os.path.join(path, filename)
            mode = 'w' if isinstance(content, str) else 'wb'
            with open(file_path, mode) as f:
                f.write(content)
    
    @patch('torch.cuda.is_available')
    def test_init(self, mock_cuda):
        """Test initialization of ModelRegistry."""
        mock_cuda.return_value = False
        
        registry = ModelRegistry(self.config)
        
        self.assertEqual(registry.models_dir, self.models_dir)
        self.assertEqual(registry.default_device, 'cpu')
        self.assertTrue(os.path.exists(self.models_dir))
        self.assertEqual(registry.registry, {})
        self.assertEqual(registry.active_models, {})
    
    def test_register_model(self):
        """Test model registration."""
        result = self.registry.register_model(
            'test_model',
            self.model_path,
            '1.0.0',
            {'description': 'Test model'}
        )
        
        self.assertTrue(result)
        self.assertIn('test_model', self.registry.registry)
        self.assertIn('1.0.0', self.registry.registry['test_model']['versions'])
        
        model_info = self.registry.registry['test_model']['versions']['1.0.0']
        self.assertEqual(model_info['name'], 'test_model')
        self.assertEqual(model_info['version'], '1.0.0')
        self.assertEqual(model_info['path'], self.model_path)
        self.assertEqual(model_info['status'], 'registered')
        self.assertEqual(model_info['metadata'], {'description': 'Test model'})
    
    @patch('transformers.AutoModelForCausalLM.from_pretrained')
    @patch('transformers.AutoTokenizer.from_pretrained')
    def test_load_model(self, mock_tokenizer, mock_model):
        """Test model loading."""
        # Register test model
        self.registry.register_model('test_model', self.model_path, '1.0.0')
        
        # Mock model and tokenizer
        mock_model.return_value = Mock()
        mock_tokenizer.return_value = Mock()
        
        # Load model
        result = self.registry.load_model('test_model', '1.0.0')
        
        self.assertIsNotNone(result)
        self.assertIn('model', result)
        self.assertIn('tokenizer', result)
        self.assertIn('info', result)
        self.assertEqual(result['device'], 'cpu')
        
        # Test caching
        cached_result = self.registry.load_model('test_model', '1.0.0')
        self.assertEqual(id(result), id(cached_result))
    
    @patch('transformers.AutoModelForCausalLM.from_pretrained')
    @patch('transformers.AutoTokenizer.from_pretrained')
    def test_unload_model(self, mock_tokenizer, mock_model):
        """Test model unloading."""
        # Register test model
        self.registry.register_model('test_model', self.model_path, '1.0.0')
        
        # Mock model and tokenizer
        mock_model_instance = Mock()
        mock_tokenizer_instance = Mock()
        mock_model.return_value = mock_model_instance
        mock_tokenizer.return_value = mock_tokenizer_instance
        
        # Load model
        loaded_model = self.registry.load_model('test_model', '1.0.0')
        self.assertIsNotNone(loaded_model)
        
        # Verify model is loaded
        model_key = 'test_model:1.0.0'
        self.assertIn(model_key, self.registry.active_models)
        
        # Unload model
        result = self.registry.unload_model('test_model', '1.0.0')
        
        self.assertTrue(result)
        self.assertNotIn(model_key, self.registry.active_models)
        
        # Verify mocks were called
        mock_model.assert_called_once_with(
            self.model_path,
            device_map=None,
            torch_dtype=torch.float32
        )
        mock_tokenizer.assert_called_once_with(self.model_path)
    
    def test_get_model_info(self):
        """Test getting model information."""
        # Register test model
        self.registry.register_model('test_model', self.model_path, '1.0.0')
        
        # Get model info
        info = self.registry.get_model_info('test_model', '1.0.0')
        
        self.assertIsNotNone(info)
        self.assertEqual(info['name'], 'test_model')
        self.assertEqual(info['version'], '1.0.0')
        self.assertEqual(info['path'], self.model_path)
    
    def test_list_models(self):
        """Test listing registered models."""
        # Register multiple model versions
        self.registry.register_model('test_model', self.model_path, '1.0.0')
        self.registry.register_model('test_model', self.model_path, '2.0.0')
        
        models = self.registry.list_models()
        
        self.assertEqual(len(models), 2)
        versions = {model['version'] for model in models}
        self.assertEqual(versions, {'1.0.0', '2.0.0'})
    
    def test_validate_model_files(self):
        """Test model file validation."""
        # Test with valid model path
        self.assertTrue(self.registry._validate_model_files(self.model_path))
        
        # Test with invalid model path
        invalid_path = os.path.join(self.test_dir, 'invalid_model')
        os.makedirs(invalid_path)
        self.assertFalse(self.registry._validate_model_files(invalid_path))
    
    def test_calculate_model_hash(self):
        """Test model hash calculation."""
        hash1 = self.registry._calculate_model_hash(self.model_path)
        
        # Calculate hash again
        hash2 = self.registry._calculate_model_hash(self.model_path)
        
        # Hashes should be identical
        self.assertEqual(hash1, hash2)
        
        # Modify a file and check hash changes
        with open(os.path.join(self.model_path, 'config.json'), 'w') as f:
            f.write('{"model_type": "modified"}')
        
        hash3 = self.registry._calculate_model_hash(self.model_path)
        self.assertNotEqual(hash1, hash3)
    
    def test_error_handling(self):
        """Test error handling scenarios."""
        # Test loading non-existent model
        result = self.registry.load_model('nonexistent_model')
        self.assertIsNone(result)
        
        # Test unloading non-existent model
        result = self.registry.unload_model('nonexistent_model')
        self.assertFalse(result)
        
        # Test registering model with invalid path
        result = self.registry.register_model('test_model', '/invalid/path', '1.0.0')
        self.assertFalse(result)

    def _verify_model_loaded(self, model_name: str, version: str) -> bool:
        """Helper to verify model is properly loaded."""
        model_key = f"{model_name}:{version}"
        if model_key not in self.registry.active_models:
            return False
            
        loaded_model = self.registry.active_models[model_key]
        return all(
            key in loaded_model
            for key in ['model', 'tokenizer', 'info', 'device', 'loaded_at']
        )

if __name__ == '__main__':
    unittest.main() 