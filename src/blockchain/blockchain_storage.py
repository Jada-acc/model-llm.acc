from typing import Dict, List, Optional, Union
import logging
from datetime import datetime
from .db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class BlockchainStorage:
    def __init__(self, database_url: str = "sqlite:///blockchain_data.db"):
        """
        Initialize blockchain storage with SQLAlchemy database.
        
        Args:
            database_url: Database connection URL
        """
        self.db = DatabaseManager(database_url)
        self.db.initialize_database()
        logger.info("Blockchain storage initialized with SQLAlchemy")

    def store_block_data(self, blockchain: str, block_data: dict) -> bool:
        """
        Store blockchain block data.
        
        Args:
            blockchain: Name of the blockchain ('ethereum' or 'solana')
            block_data: Dictionary containing block data
        
        Returns:
            bool: True if storage was successful, False otherwise
        """
        try:
            block = self.db.store_block(blockchain, block_data)
            if block:
                # Store TPS metric
                if 'transaction_count' in block_data:
                    self.db.store_metric(
                        block.id,
                        'tps',
                        block_data['transaction_count']  # TPS will be calculated by analyzer
                    )
                return True
            return False
        except Exception as e:
            logger.error(f"Error storing block data: {str(e)}")
            return False

    def get_latest_blocks(self, blockchain: str, limit: int = 100) -> List[Dict]:
        """
        Get the latest blocks for a blockchain.
        
        Args:
            blockchain: Name of the blockchain
            limit: Maximum number of blocks to return
        
        Returns:
            List of block data dictionaries
        """
        try:
            blocks = self.db.get_latest_blocks(blockchain, limit)
            return [
                {
                    'block_number': block.block_number,
                    'timestamp': block.timestamp,
                    'transaction_count': block.transaction_count,
                    'block_hash': block.block_hash,
                    'gas_used': block.gas_used,
                    'gas_limit': block.gas_limit,
                    'slot': block.slot
                }
                for block in blocks
            ]
        except Exception as e:
            logger.error(f"Error retrieving latest blocks: {str(e)}")
            return []

    def get_metrics(self, metric_type: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """
        Get metrics for a specific time range.
        
        Args:
            metric_type: Type of metric to retrieve
            start_time: Start of time range
            end_time: End of time range
        
        Returns:
            List of metric data dictionaries
        """
        try:
            metrics = self.db.get_metrics(metric_type, start_time, end_time)
            return [
                {
                    'timestamp': metric.timestamp,
                    'value': metric.value,
                    'block_id': metric.block_id
                }
                for metric in metrics
            ]
        except Exception as e:
            logger.error(f"Error retrieving metrics: {str(e)}")
            return []