import unittest
from unittest.mock import Mock, patch, AsyncMock
import torch
import asyncio
from fastapi.testclient import TestClient
from pathlib import Path

from src.llm.deployment.model_server import ModelServer, LoadBalancer, GenerationRequest

class TestModelServer(unittest.TestCase):
    """Test suite for ModelServer."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            'host': 'localhost',
            'port': 8000,
            'workers': 1,
            'max_batch_size': 4,
            'default_model': 'test-model',
            'default_version': 'v1',
            'model_registry': {
                'models_dir': 'test_models'
            },
            'inference_pipeline': {
                'max_length': 128,
                'use_cuda': False
            }
        }
        
        self.server = ModelServer(self.config)
        self.client = TestClient(self.server.app)
    
    @patch('src.llm.inference_pipeline.InferencePipeline.generate')
    def test_generate_endpoint(self, mock_generate):
        """Test text generation endpoint."""
        # Mock generation result
        mock_generate.return_value = [{
            'generated': 'Test response',
            'tokens': 10,
            'generation_time': 0.5
        }]
        
        # Test request
        response = self.client.post(
            "/generate",
            json={
                'prompt': 'Test prompt',
                'max_length': 128,
                'temperature': 0.7
            }
        )
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('generated_text', data)
        self.assertIn('model_name', data)
        self.assertIn('version', data)
        
        # Verify generation was called with correct parameters
        mock_generate.assert_called_once()
    
    @patch('src.llm.model_registry.ModelRegistry.list_models')
    def test_models_endpoint(self, mock_list_models):
        """Test models listing endpoint."""
        # Mock model list
        mock_list_models.return_value = [
            {'name': 'test-model', 'versions': ['v1', 'v2']}
        ]
        
        response = self.client.get("/models")
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('models', data)
        self.assertIn('default_model', data)
        self.assertEqual(data['default_model'], 'test-model')
    
    def test_health_endpoint(self):
        """Test health check endpoint."""
        response = self.client.get("/health")
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data['status'], 'healthy')
        self.assertIn('gpu_available', data)
        self.assertIn('loaded_models', data)
    
    @patch('torch.cuda.memory_allocated')
    @patch('torch.cuda.empty_cache')
    async def test_cache_cleanup(self, mock_empty_cache, mock_memory_allocated):
        """Test model cache cleanup."""
        # Mock high memory usage
        mock_memory_allocated.return_value = 8 * 1024**3  # 8GB
        
        # Trigger cleanup
        await self.server._cleanup_cache()
        
        # Verify cache was cleared
        self.assertEqual(len(self.server._model_cache), 0)
        mock_empty_cache.assert_called_once()
    
    def test_error_handling(self):
        """Test error handling in endpoints."""
        # Test with invalid request
        response = self.client.post(
            "/generate",
            json={
                'prompt': None,  # Invalid prompt
                'max_length': -1  # Invalid length
            }
        )
        
        self.assertEqual(response.status_code, 422)  # Validation error

class TestLoadBalancer(unittest.TestCase):
    """Test suite for LoadBalancer."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            'servers': [
                {
                    'host': 'localhost',
                    'port': 8001,
                    'workers': 1
                },
                {
                    'host': 'localhost',
                    'port': 8002,
                    'workers': 1
                }
            ]
        }
        
        self.load_balancer = LoadBalancer(self.config)
    
    def test_server_rotation(self):
        """Test round-robin server selection."""
        first_server = self.load_balancer.get_next_server()
        second_server = self.load_balancer.get_next_server()
        third_server = self.load_balancer.get_next_server()
        
        # Verify round-robin
        self.assertNotEqual(first_server, second_server)
        self.assertEqual(first_server, third_server)
    
    @patch('aiohttp.ClientSession.get')
    async def test_health_check(self, mock_get):
        """Test health check of all servers."""
        # Mock healthy responses
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={'status': 'healthy'}
        )
        
        health_status = await self.load_balancer.health_check()
        
        self.assertEqual(len(health_status), 2)  # Two servers
        for server_status in health_status.values():
            self.assertEqual(server_status['status'], 'healthy')
    
    @patch('aiohttp.ClientSession.get')
    async def test_unhealthy_server(self, mock_get):
        """Test handling of unhealthy server."""
        # Mock one server as unhealthy
        mock_get.side_effect = [
            AsyncMock(return_value={'status': 'healthy'}),
            Exception("Connection failed")
        ]
        
        health_status = await self.load_balancer.health_check()
        
        self.assertIn('unhealthy', health_status['server_1']['status'])

if __name__ == '__main__':
    unittest.main() 