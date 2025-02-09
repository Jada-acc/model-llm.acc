from datetime import datetime
from typing import Dict, Any
from web3 import Web3
from src.ingestion.data_ingester import DataSource
import logging

logger = logging.getLogger(__name__)

class BlockchainDataSource(DataSource):
    """Implementation for blockchain data source."""
    
    def __init__(self, blockchain_url: str):
        self.url = blockchain_url
        self.web3 = None
        
    def connect(self) -> bool:
        """Connect to blockchain node."""
        try:
            self.web3 = Web3(Web3.HTTPProvider(self.url))
            return self.web3.is_connected()
        except Exception as e:
            logger.error(f"Failed to connect to blockchain: {str(e)}")
            return False
            
    def validate_connection(self) -> bool:
        """Check if connection is still valid."""
        return self.web3 and self.web3.is_connected()
        
    def fetch_data(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Fetch blockchain data for the given time range."""
        try:
            # Get block numbers for time range
            start_block = self._get_block_by_timestamp(start_time)
            end_block = self._get_block_by_timestamp(end_time)
            
            blocks = []
            for block_num in range(start_block, end_block + 1):
                block = self.web3.eth.get_block(block_num, full_transactions=True)
                blocks.append({
                    'number': block.number,
                    'hash': block.hash.hex(),
                    'timestamp': datetime.fromtimestamp(block.timestamp),
                    'transactions': [tx.hash.hex() for tx in block.transactions]
                })
            
            return {
                'blocks': blocks,
                'timestamp': datetime.now().isoformat(),
                'metrics': {
                    'block_count': len(blocks),
                    'start_block': start_block,
                    'end_block': end_block
                }
            }
            
        except Exception as e:
            logger.error(f"Error fetching blockchain data: {str(e)}")
            raise
            
    def _get_block_by_timestamp(self, timestamp: datetime) -> int:
        """Find the closest block number for a given timestamp."""
        timestamp_unix = int(timestamp.timestamp())
        latest_block = self.web3.eth.block_number
        
        # Binary search for block
        left, right = 1, latest_block
        while left <= right:
            mid = (left + right) // 2
            block = self.web3.eth.get_block(mid)
            
            if block.timestamp == timestamp_unix:
                return mid
            elif block.timestamp < timestamp_unix:
                left = mid + 1
            else:
                right = mid - 1
                
        return right 