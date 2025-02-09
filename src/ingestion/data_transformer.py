from typing import Dict, Any, List
import logging
from datetime import datetime
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class DataTransformer(ABC):
    """Abstract base class for data transformers."""
    
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform the data according to specific rules."""
        pass

class BlockchainTransformer(DataTransformer):
    """Transform blockchain data for analysis."""
    
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform blockchain data into analyzable format."""
        try:
            transformed_data = {
                'timestamp': data['timestamp'],
                'block_metrics': self._calculate_block_metrics(data['blocks']),
                'transaction_metrics': self._calculate_transaction_metrics(data['blocks']),
                'raw_data': data  # Keep original data for reference
            }
            logger.info("Successfully transformed blockchain data")
            return transformed_data
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            raise
    
    def _calculate_block_metrics(self, blocks: List[Dict]) -> Dict[str, Any]:
        """Calculate metrics from block data."""
        return {
            'total_blocks': len(blocks),
            'block_times': [
                block['timestamp'] for block in blocks
            ],
            'block_numbers': [
                block['number'] for block in blocks
            ]
        }
    
    def _calculate_transaction_metrics(self, blocks: List[Dict]) -> Dict[str, Any]:
        """Calculate metrics from transaction data."""
        all_transactions = []
        for block in blocks:
            all_transactions.extend(block['transactions'])
            
        return {
            'total_transactions': len(all_transactions),
            'transactions_per_block': len(all_transactions) / len(blocks) if blocks else 0
        } 