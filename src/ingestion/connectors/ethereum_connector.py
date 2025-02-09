from web3 import Web3
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from .base_connector import BaseConnector

logger = logging.getLogger(__name__)

class EthereumConnector(BaseConnector):
    """Connector for Ethereum blockchain data."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.provider_url = config['provider_url']
        self.web3 = None
        self.start_block = config.get('start_block', 'latest')
        self.batch_size = config.get('batch_size', 100)
    
    def connect(self) -> bool:
        """Establish connection to Ethereum node."""
        try:
            self.web3 = Web3(Web3.HTTPProvider(self.provider_url))
            connected = self.web3.is_connected()
            if connected:
                logger.info("Successfully connected to Ethereum node")
            else:
                logger.error("Failed to connect to Ethereum node")
            return connected
        except Exception as e:
            logger.error(f"Error connecting to Ethereum node: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """Close connection to Ethereum node."""
        try:
            self.web3 = None
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from Ethereum node: {str(e)}")
            return False
    
    def fetch_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch blockchain data based on query parameters."""
        try:
            if not self.web3:
                raise ConnectionError("Not connected to Ethereum node")
            
            # Parse query parameters
            start_block = query.get('start_block', self.start_block)
            end_block = query.get('end_block', 'latest')
            include_transactions = query.get('include_transactions', True)
            
            # Get block range
            if end_block == 'latest':
                end_block = self.web3.eth.block_number
            if start_block == 'latest':
                start_block = end_block
            
            blocks = []
            for block_num in range(start_block, end_block + 1):
                block = self.web3.eth.get_block(block_num, include_transactions)
                blocks.append(self._format_block(block))
            
            logger.info(f"Fetched {len(blocks)} blocks from Ethereum")
            return blocks
            
        except Exception as e:
            logger.error(f"Error fetching Ethereum data: {str(e)}")
            raise
    
    def validate_connection(self) -> bool:
        """Validate connection to Ethereum node."""
        try:
            return self.web3.is_connected() if self.web3 else False
        except Exception:
            return False
    
    def _format_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Format block data for storage."""
        return {
            'number': block.number,
            'hash': block.hash.hex(),
            'parent_hash': block.parentHash.hex(),
            'timestamp': datetime.fromtimestamp(block.timestamp),
            'transactions': [
                self._format_transaction(tx) if isinstance(tx, dict) else tx.hex()
                for tx in block.transactions
            ],
            'gas_used': block.gasUsed,
            'gas_limit': block.gasLimit,
            'base_fee_per_gas': getattr(block, 'baseFeePerGas', None),
            'extra_data': block.extraData.hex()
        }
    
    def _format_transaction(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        """Format transaction data for storage."""
        return {
            'hash': tx['hash'].hex(),
            'from': tx['from'],
            'to': tx['to'],
            'value': tx['value'],
            'gas': tx['gas'],
            'gas_price': tx['gasPrice'],
            'nonce': tx['nonce']
        } 