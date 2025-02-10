from unittest.mock import patch, Mock
import torch
from torch.utils.data import DataLoader
from transformers import PreTrainedModel, PreTrainedTokenizer

class TestTrainingWorkflow:
    @patch('torch.distributed.is_initialized', return_value=False)
    @patch('torch.distributed.init_process_group')
    @patch('torch.cuda.set_device')
    def test_distributed_training(self, mock_set_device, mock_init_group, mock_is_init):
        """Test distributed training setup and execution."""
        # Configure for distributed
        dist_config = {
            **self.config,
            'distributed': True,
            'local_rank': 0,
            'world_size': 2
        }
        workflow = TrainingWorkflow(dist_config)
        
        # Mock model and tokenizer
        model = Mock(spec=PreTrainedModel)
        model.device = torch.device('cuda:0')
        model.to = Mock(return_value=model)
        
        outputs = Mock()
        outputs.loss = torch.tensor(0.5)
        model.forward = Mock(return_value=outputs)
        
        tokenizer = Mock(spec=PreTrainedTokenizer)
        
        # Mock data
        batch = {
            'input_ids': torch.ones(2, 10),
            'attention_mask': torch.ones(2, 10),
            'labels': torch.ones(2, 10)
        }
        train_data = DataLoader([batch])
        
        # Run distributed training
        results = workflow.train(model, tokenizer, train_data=train_data)
        
        # Verify distributed setup
        mock_init_group.assert_called_once()
        mock_set_device.assert_called_once_with(0)
        
        # Verify results
        self.assertIn('epochs_completed', results)
        self.assertIn('global_steps', results) 