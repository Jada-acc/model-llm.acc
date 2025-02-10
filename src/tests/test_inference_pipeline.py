import unittest
from unittest.mock import Mock, patch
import torch
import asyncio
from src.llm.inference_pipeline import InferencePipeline

class TestInferencePipeline(unittest.TestCase):
    """Test suite for InferencePipeline."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            'model_registry': {
                'models_dir': 'test_models'
            },
            'max_batch_size': 2,
            'max_length': 10,
            'default_model': 'test_model',
            'default_version': '1.0.0',
            'fp16': False
        }
        
        self.pipeline = InferencePipeline(self.config)
    
    @patch('src.llm.model_registry.ModelRegistry.load_model')
    def test_generate_single(self, mock_load_model):
        """Test generation with single prompt."""
        # Mock model and tokenizer
        mock_model = Mock()
        mock_tokenizer = Mock()
        
        mock_model.generate.return_value = torch.tensor([[1, 2, 3]])
        mock_tokenizer.batch_decode.return_value = ['Test response']
        mock_tokenizer.pad_token_id = 0
        mock_tokenizer.eos_token_id = 2
        
        mock_load_model.return_value = {
            'model': mock_model,
            'tokenizer': mock_tokenizer
        }
        
        # Test generation
        result = asyncio.run(self.pipeline.generate('Test prompt'))
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['prompt'], 'Test prompt')
        self.assertEqual(result[0]['generated'], 'Test response')
        
        # Verify model was called correctly
        mock_model.generate.assert_called_once()
        mock_tokenizer.batch_decode.assert_called_once()
    
    @patch('src.llm.model_registry.ModelRegistry.load_model')
    def test_generate_batch(self, mock_load_model):
        """Test generation with multiple prompts."""
        # Mock model and tokenizer
        mock_model = Mock()
        mock_tokenizer = Mock()
        
        mock_model.generate.return_value = torch.tensor([[1, 2], [3, 4]])
        mock_tokenizer.batch_decode.return_value = ['Response 1', 'Response 2']
        mock_tokenizer.pad_token_id = 0
        mock_tokenizer.eos_token_id = 2
        
        mock_load_model.return_value = {
            'model': mock_model,
            'tokenizer': mock_tokenizer
        }
        
        # Test generation
        prompts = ['Prompt 1', 'Prompt 2']
        result = asyncio.run(self.pipeline.generate(prompts))
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['prompt'], 'Prompt 1')
        self.assertEqual(result[1]['prompt'], 'Prompt 2')
        self.assertEqual(result[0]['generated'], 'Response 1')
        self.assertEqual(result[1]['generated'], 'Response 2')
    
    def test_metrics(self):
        """Test metrics tracking."""
        # Initial metrics should be zero
        metrics = self.pipeline.get_metrics()
        self.assertEqual(metrics['total_requests'], 0)
        self.assertEqual(metrics['total_tokens'], 0)
        
        # Update metrics
        self.pipeline._update_metrics(
            num_requests=2,
            results=[
                {'tokens': 10},
                {'tokens': 15}
            ],
            latency=1.0
        )
        
        # Check updated metrics
        metrics = self.pipeline.get_metrics()
        self.assertEqual(metrics['total_requests'], 2)
        self.assertEqual(metrics['total_tokens'], 25)
        self.assertEqual(metrics['avg_latency'], 0.5)
    
    def test_batch_inputs(self):
        """Test input batching."""
        inputs = ['1', '2', '3', '4', '5']
        batches = list(self.pipeline._batch_inputs(inputs))
        
        self.assertEqual(len(batches), 3)
        self.assertEqual(batches[0], ['1', '2'])
        self.assertEqual(batches[1], ['3', '4'])
        self.assertEqual(batches[2], ['5'])
    
    @patch('torch.cuda')
    @patch('psutil.virtual_memory')
    def test_memory_management(self, mock_vmem, mock_cuda):
        """Test memory management functionality."""
        # Configure CUDA mock
        mock_cuda.is_available.return_value = True
        mock_cuda.memory_allocated.return_value = 4 * 1024**3  # 4GB used
        mock_cuda.max_memory_allocated.return_value = 8 * 1024**3  # 8GB total
        
        # Configure system memory mock
        mock_vmem.return_value.percent = 85  # 85% system memory used
        
        # Create pipeline with CUDA enabled
        test_config = {
            **self.config,
            'use_cuda': True  # Force CUDA usage for test
        }
        test_pipeline = InferencePipeline(test_config)
        
        # Test memory management
        test_pipeline._manage_memory()
        
        # Verify metrics were updated
        self.assertTrue(len(test_pipeline.metrics['memory_usage']) > 0)
        latest_memory = test_pipeline.metrics['memory_usage'][-1]
        self.assertEqual(latest_memory['system_memory'], 85)
        self.assertEqual(latest_memory['gpu_memory'], 4 * 1024**3)
        
        # Verify memory management calls
        mock_cuda.empty_cache.assert_not_called()  # Should not be called as memory usage is 50%
        
        # Test memory threshold trigger
        mock_cuda.memory_allocated.return_value = 7.5 * 1024**3  # 7.5GB used (above threshold)
        test_pipeline._manage_memory()
        mock_cuda.empty_cache.assert_called_once()
    
    @patch('torch.cuda')
    def test_dynamic_batching(self, mock_cuda):
        """Test dynamic batch size adjustment."""
        # Mock GPU memory
        mock_cuda.is_available.return_value = True
        mock_cuda.get_device_properties.return_value.total_memory = 8 * 1024**3
        mock_cuda.memory_allocated.return_value = 2 * 1024**3
        
        # Test with no previous metrics
        inputs = ['prompt'] * 10
        batches = list(self.pipeline._dynamic_batch(inputs))
        self.assertEqual(len(batches), 5)  # Default max_batch_size = 2
        
        # Test with previous metrics
        self.pipeline.metrics['batch_sizes'] = [4, 4, 4]  # Previous successful batches
        batches = list(self.pipeline._dynamic_batch(inputs))
        self.assertTrue(all(1 <= len(batch) <= self.pipeline.max_batch_size for batch in batches))
    
    @patch('torch.jit')
    def test_model_optimization(self, mock_jit):
        """Test model optimization features."""
        # Create mock model
        mock_model = Mock()
        mock_model.eval = Mock()
        mock_model.to = Mock(return_value=mock_model)
        
        # Test optimization
        optimized = self.pipeline._optimize_model(mock_model)
        
        # Verify optimizations were applied
        mock_model.eval.assert_called_once()
        if self.pipeline.use_cuda:
            mock_model.to.assert_called_with(memory_format=torch.channels_last)
    
    def test_caching(self):
        """Test response and tokenization caching."""
        # Test response cache
        prompt = "test prompt"
        cached_response = self.pipeline._generate_cached(
            prompt=prompt,
            model_name='test_model',
            version='1.0.0'
        )
        self.assertTrue(cached_response['cached'])
        
        # Test tokenization cache
        prompts = ["prompt1", "prompt2"]
        kwargs = {'temperature': 0.7}
        
        # First call should miss cache
        result1 = self.pipeline._check_cache(prompts, kwargs)
        self.assertIsNone(result1)
        
        # Add to cache
        self.pipeline.tokenization_cache[(
            tuple(prompts),
            self.pipeline.default_model,
            self.pipeline.default_version,
            frozenset(kwargs.items())
        )] = ['response1', 'response2']
        
        # Second call should hit cache
        result2 = self.pipeline._check_cache(prompts, kwargs)
        self.assertIsNotNone(result2)
        self.assertEqual(self.pipeline.metrics['cache_hits'], 1)

if __name__ == '__main__':
    unittest.main() 