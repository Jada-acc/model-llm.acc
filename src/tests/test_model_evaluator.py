import unittest
from unittest.mock import Mock, patch
import torch
import asyncio
from datetime import datetime
from src.llm.model_evaluator import ModelEvaluator

class TestModelEvaluator(unittest.TestCase):
    """Test suite for ModelEvaluator."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            'inference_pipeline': {
                'model_registry': {
                    'models_dir': 'test_models'
                }
            },
            'metrics': ['accuracy', 'perplexity', 'latency'],
            'batch_size': 2
        }
        
        self.evaluator = ModelEvaluator(self.config)
        
        # Sample evaluation data
        self.eval_data = [
            {'prompt': 'Test 1', 'expected': 'Response 1'},
            {'prompt': 'Test 2', 'expected': 'Response 2'}
        ]
        
        # Set up event loop for async tests
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
    
    def tearDown(self):
        """Clean up after tests."""
        self.loop.close()
    
    @patch('src.llm.inference_pipeline.InferencePipeline.generate')
    def test_accuracy_evaluation(self, mock_generate):
        """Test accuracy evaluation."""
        # Mock generation results
        mock_generate.return_value = [
            {'generated': 'Response 1', 'tokens': 10},
            {'generated': 'Response 2', 'tokens': 12}
        ]
        
        results = self.loop.run_until_complete(
            self.evaluator._evaluate_accuracy(
                'test_model',
                '1.0.0',
                self.eval_data
            )
        )
        
        self.assertIn('accuracy', results)
        self.assertIn('precision', results)
        self.assertIn('recall', results)
        self.assertIn('f1', results)
        self.assertEqual(results['accuracy'], 1.0)  # Perfect match
    
    @patch('src.llm.model_registry.ModelRegistry.load_model')
    @patch('src.llm.inference_pipeline.InferencePipeline.generate')
    def test_full_evaluation(self, mock_generate, mock_load_model):
        """Test full model evaluation."""
        # Mock model and tokenizer
        mock_model = Mock()
        mock_tokenizer = Mock()
        
        # Set up model mock
        mock_model.return_value = Mock(loss=torch.tensor(2.0))
        mock_model.to = Mock(return_value=mock_model)
        mock_model.__call__ = Mock(return_value=Mock(loss=torch.tensor(2.0)))
        
        # Set up tokenizer mock
        mock_tokenizer.pad_token_id = 0
        mock_tokenizer.return_value = Mock(
            input_ids=torch.ones(2, 10),
            to=Mock(return_value={'input_ids': torch.ones(2, 10)})
        )
        
        # Set up model loading
        mock_load_model.return_value = {
            'model': mock_model,
            'tokenizer': mock_tokenizer
        }
        
        # Mock generation results
        mock_generate.return_value = [
            {'generated': 'Response 1', 'tokens': 10},
            {'generated': 'Response 2', 'tokens': 12}
        ]
        
        results = self.loop.run_until_complete(
            self.evaluator.evaluate_model(
                'test_model',
                '1.0.0',
                self.eval_data
            )
        )
        
        self.assertIn('metrics', results)
        self.assertIn('samples_evaluated', results)
        self.assertIn('start_time', results)
        self.assertIn('end_time', results)
        self.assertIn('duration_seconds', results)
    
    @patch('src.llm.model_registry.ModelRegistry.load_model')
    def test_perplexity_evaluation(self, mock_load_model):
        """Test perplexity evaluation."""
        # Mock model and tokenizer
        mock_model = Mock()
        mock_tokenizer = Mock()
        
        # Set up model mock with proper loss value
        outputs = Mock()
        outputs.loss = Mock()
        outputs.loss.item.return_value = 2.0  # Return float value for loss
        mock_model.__call__ = Mock(return_value=outputs)
        
        # Set up tokenizer mock with proper return structure
        def tokenizer_side_effect(*args, **kwargs):
            encoded = Mock()
            # Mock input_ids tensor
            input_ids = torch.ones(2, 10)
            # Mock ne() to return a tensor of ones for token counting
            input_ids.ne = Mock(return_value=torch.ones(2, 10))
            
            encoded.to = Mock(return_value={
                'input_ids': input_ids,
                'attention_mask': torch.ones(2, 10)
            })
            return encoded
        
        mock_tokenizer.side_effect = tokenizer_side_effect
        mock_tokenizer.pad_token_id = 0
        
        # Set up model loading
        mock_load_model.return_value = {
            'model': mock_model,
            'tokenizer': mock_tokenizer
        }
        
        results = self.loop.run_until_complete(
            self.evaluator._evaluate_perplexity(
                'test_model',
                '1.0.0',
                self.eval_data
            )
        )
        
        self.assertIn('perplexity', results)
        self.assertIn('avg_loss', results)
        
        # Verify the model was called correctly
        mock_model.__call__.assert_called()
        # Verify tokenizer was called for both inputs and labels
        self.assertEqual(mock_tokenizer.call_count, 2)
        
        # Verify the loss calculation
        self.assertGreater(results['perplexity'], 0)
        self.assertGreater(results['avg_loss'], 0)
    
    @patch('src.llm.inference_pipeline.InferencePipeline.generate')
    def test_latency_evaluation(self, mock_generate):
        """Test latency evaluation."""
        # Mock generation results
        mock_generate.return_value = [
            {'generated': 'Response 1', 'tokens': 10},
            {'generated': 'Response 2', 'tokens': 12}
        ]
        
        results = self.loop.run_until_complete(
            self.evaluator._evaluate_latency(
                'test_model',
                '1.0.0',
                self.eval_data
            )
        )
        
        self.assertIn('avg_latency', results)
        self.assertIn('p50_latency', results)
        self.assertIn('p90_latency', results)
        self.assertIn('p99_latency', results)
        self.assertIn('avg_throughput', results)
        self.assertEqual(results['samples_processed'], 2)
    
    def test_batch_data(self):
        """Test data batching."""
        data = [{'id': i} for i in range(5)]
        batches = list(self.evaluator._batch_data(data))
        
        self.assertEqual(len(batches), 3)
        self.assertEqual(len(batches[0]), 2)
        self.assertEqual(len(batches[1]), 2)
        self.assertEqual(len(batches[2]), 1)

if __name__ == '__main__':
    unittest.main() 