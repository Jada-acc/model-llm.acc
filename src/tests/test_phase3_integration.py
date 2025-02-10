import unittest
from unittest.mock import patch, Mock, AsyncMock
import asyncio
import torch
from fastapi.testclient import TestClient
import yaml
import os

from src.llm.deployment.model_server import ModelServer
from src.llm.training_workflow import TrainingWorkflow
from src.llm.data_preparation import DataPreparationPipeline
from src.llm.model_registry import ModelRegistry

class TestPhase3Integration(unittest.TestCase):
    """Integration test for Phase 3 components."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.deploy_config = {
            'model_server': {
                'host': 'localhost',
                'port': 8000,
                'workers': 1,
                'model_registry': {
                    'models_dir': 'test_models',
                    'cache_dir': 'test_cache'
                }
            }
        }
        
        # Create test directories
        os.makedirs('test_models', exist_ok=True)
        os.makedirs('test_cache', exist_ok=True)
        
        # Mock InferencePipeline
        cls.mock_inference = AsyncMock()
        cls.mock_inference.generate.return_value = [{
            'generated': 'Test response',
            'tokens': 10,
            'generation_time': 0.5
        }]
    
    @patch('src.llm.deployment.model_server.InferencePipeline')
    def test_full_pipeline(self, mock_pipeline_class):
        """Test the full pipeline from training to deployment."""
        # Setup mock
        mock_pipeline_class.return_value = self.mock_inference
        
        # Create server with mock
        server = ModelServer(self.deploy_config['model_server'])
        client = TestClient(server.app)
        
        # Test generation endpoint
        response = client.post(
            "/generate",
            json={
                'prompt': 'Test prompt',
                'max_length': 128
            }
        )
        self.assertEqual(response.status_code, 200)
        
        # Verify mock was called correctly
        self.mock_inference.generate.assert_called_once()
        call_args = self.mock_inference.generate.call_args[1]
        self.assertEqual(call_args['prompts'], ['Test prompt'])
        
        # Test monitoring
        response = client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        self.assertIn('llm_requests_total', response.text)
        
        # Test health check
        response = client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['status'], 'healthy')
    
    @patch('src.llm.deployment.model_server.InferencePipeline')
    @patch('torch.cuda.memory_allocated')
    def test_autoscaling_triggers(self, mock_memory, mock_pipeline_class):
        """Test conditions that would trigger autoscaling."""
        # Setup mocks
        mock_memory.return_value = 15 * 1024**3  # 15GB
        mock_pipeline_class.return_value = self.mock_inference
        
        server = ModelServer(self.deploy_config['model_server'])
        client = TestClient(server.app)
        
        # Generate load
        for _ in range(10):
            response = client.post(
                "/generate",
                json={'prompt': 'Test prompt'}
            )
            self.assertEqual(response.status_code, 200)
        
        # Check metrics that would trigger HPA
        response = client.get("/metrics")
        self.assertIn('llm_gpu_memory_used_bytes', response.text)
    
    def tearDown(self):
        """Clean up after tests."""
        import shutil
        if os.path.exists('test_models'):
            shutil.rmtree('test_models')
        if os.path.exists('test_cache'):
            shutil.rmtree('test_cache')

if __name__ == '__main__':
    unittest.main() 