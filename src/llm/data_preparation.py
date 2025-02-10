from typing import Dict, Any, List, Optional, Union, Iterator
import logging
from pathlib import Path
import json
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import PreTrainedTokenizer
import pandas as pd
from datasets import load_dataset, Dataset as HFDataset

logger = logging.getLogger(__name__)

class TrainingDataset(Dataset):
    """Custom dataset for LLM training data."""
    
    def __init__(
        self,
        data_config: Dict[str, Any],
        tokenizer: PreTrainedTokenizer,
        max_length: int = 512,
        cache_dir: Optional[str] = None
    ):
        """Initialize dataset."""
        self.config = data_config
        self.tokenizer = tokenizer
        self.max_length = max_length
        self.cache_dir = cache_dir or 'cache'
        
        # Load and prepare data
        self.data = self._load_data()
        self.processed_data = self._preprocess_data()
    
    def _load_data(self) -> HFDataset:
        """Load data from various sources."""
        data_source = self.config.get('source', {})
        source_type = data_source.get('type', 'local')
        
        try:
            if source_type == 'huggingface':
                # Load from Hugging Face datasets
                return load_dataset(
                    data_source['name'],
                    data_source.get('config', None),
                    cache_dir=self.cache_dir
                )
            elif source_type == 'local':
                # Load from local files
                return self._load_local_data(data_source['path'])
            elif source_type == 'custom':
                # Load from custom data source
                return self._load_custom_data(data_source)
            else:
                raise ValueError(f"Unsupported data source type: {source_type}")
                
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def _load_local_data(self, path: str) -> HFDataset:
        """Load data from local files."""
        data_path = Path(path)
        
        if not data_path.exists():
            raise FileNotFoundError(f"Data path not found: {path}")
        
        if data_path.is_file():
            # Single file loading
            if data_path.suffix == '.json':
                with open(data_path, 'r') as f:
                    data = json.load(f)
            elif data_path.suffix == '.jsonl':
                data = [json.loads(line) for line in open(data_path)]
            elif data_path.suffix == '.csv':
                data = pd.read_csv(data_path).to_dict('records')
            else:
                raise ValueError(f"Unsupported file format: {data_path.suffix}")
        else:
            # Directory loading
            data = []
            for file_path in data_path.glob('**/*'):
                if file_path.suffix in ['.json', '.jsonl', '.csv']:
                    data.extend(self._load_local_data(str(file_path)))
        
        return HFDataset.from_dict({k: [d[k] for d in data] for k in data[0].keys()})
    
    def _load_custom_data(self, source_config: Dict[str, Any]) -> HFDataset:
        """Load data from custom source."""
        # Implement custom data loading logic
        raise NotImplementedError("Custom data loading not implemented")
    
    def _preprocess_data(self) -> List[Dict[str, torch.Tensor]]:
        """Preprocess and tokenize data."""
        processed = []
        
        # Get column mappings from config
        column_map = self.config.get('column_mapping', {
            'input': 'text',
            'target': 'text'
        })
        
        for item in self.data:
            try:
                # Prepare input text
                input_text = item[column_map['input']]
                if isinstance(input_text, (list, tuple)):
                    input_text = ' '.join(input_text)
                
                # Prepare target text
                target_text = item.get(column_map.get('target'), input_text)
                if isinstance(target_text, (list, tuple)):
                    target_text = ' '.join(target_text)
                
                # Tokenize
                tokenized = self._tokenize_text(input_text, target_text)
                if tokenized:
                    processed.append(tokenized)
                
            except Exception as e:
                logger.warning(f"Error processing item: {e}")
                continue
        
        return processed
    
    def _tokenize_text(
        self,
        input_text: str,
        target_text: Optional[str] = None
    ) -> Optional[Dict[str, torch.Tensor]]:
        """Tokenize input and target text."""
        try:
            # Tokenize input
            inputs = self.tokenizer(
                input_text,
                max_length=self.max_length,
                padding='max_length',
                truncation=True,
                return_tensors='pt'
            )
            
            # Tokenize target if provided
            if target_text and target_text != input_text:
                labels = self.tokenizer(
                    target_text,
                    max_length=self.max_length,
                    padding='max_length',
                    truncation=True,
                    return_tensors='pt'
                )
                inputs['labels'] = labels['input_ids']
            else:
                inputs['labels'] = inputs['input_ids'].clone()
            
            # Convert to single tensors
            return {k: v.squeeze(0) for k, v in inputs.items()}
            
        except Exception as e:
            logger.warning(f"Error tokenizing text: {e}")
            return None
    
    def __len__(self) -> int:
        """Get dataset length."""
        return len(self.processed_data)
    
    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        """Get dataset item."""
        return self.processed_data[idx]

class DataPreparationPipeline:
    """Pipeline for preparing training data."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize data preparation pipeline."""
        self.config = config
        self.cache_dir = config.get('cache_dir', 'cache')
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
    
    def prepare_data(
        self,
        tokenizer: PreTrainedTokenizer,
        **kwargs
    ) -> DataLoader:
        """Prepare data for training."""
        try:
            # Create dataset
            dataset = TrainingDataset(
                self.config,
                tokenizer,
                max_length=self.config.get('max_length', 512),
                cache_dir=self.cache_dir
            )
            
            # Create data loader
            return DataLoader(
                dataset,
                batch_size=self.config.get('batch_size', 8),
                shuffle=self.config.get('shuffle', True),
                num_workers=self.config.get('num_workers', 2),
                pin_memory=self.config.get('pin_memory', True)
            )
            
        except Exception as e:
            logger.error(f"Error preparing data: {e}")
            raise
    
    def get_data_info(self) -> Dict[str, Any]:
        """Get information about the prepared data."""
        return {
            'config': self.config,
            'cache_dir': self.cache_dir,
            'timestamp': pd.Timestamp.now().isoformat()
        } 