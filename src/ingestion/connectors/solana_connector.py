from solana.rpc.api import Client
from typing import Dict, Any, List
import logging
from datetime import datetime
from .base_connector import BaseConnector

logger = logging.getLogger(__name__)

class SolanaConnector(BaseConnector):
    """Connector for Solana blockchain data."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.endpoint = config['endpoint']
        self.client = None
        self.commitment = config.get('commitment', 'confirmed')
    
    def connect(self) -> bool:
        """Establish connection to Solana node."""
        try:
            # Create Solana client
            self.client = Client(self.endpoint)
            
            # Test connection with version check
            try:
                response = self.client.get_version()
                if not isinstance(response, dict):
                    logger.error("Invalid response type from Solana node")
                    return False
                    
                if 'result' not in response:
                    logger.error("Invalid response format from Solana node")
                    return False
                    
                logger.info("Successfully connected to Solana node")
                return True
                
            except Exception as e:
                logger.error(f"Error testing connection: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Solana node: {e}")
            return False
    
    def disconnect(self) -> bool:
        """Close connection to Solana node."""
        try:
            self.client = None
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from Solana node: {str(e)}")
            return False
    
    def fetch_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch blockchain data based on query parameters."""
        try:
            if not self.client:
                raise ConnectionError("Not connected to Solana node")
            
            slot = query.get('slot')
            until_slot = query.get('until_slot', slot)
            
            blocks = []
            for current_slot in range(slot, until_slot + 1):
                response = self.client.get_block(
                    current_slot,
                    encoding='json',
                    commitment=self.commitment
                )
                if 'result' in response:
                    blocks.append(self._format_block(response['result']))
            
            return blocks
            
        except Exception as e:
            logger.error(f"Error fetching Solana data: {str(e)}")
            raise
    
    def validate_connection(self) -> bool:
        """Validate connection to Solana node."""
        try:
            response = self.client.get_version()
            return 'result' in response if self.client else False
        except Exception:
            return False
    
    def _format_block(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Format block data for storage."""
        return {
            'slot': block['parentSlot'] + 1,
            'parent_slot': block['parentSlot'],
            'blockhash': block['blockhash'],
            'previous_blockhash': block['previousBlockhash'],
            'timestamp': datetime.fromtimestamp(block['blockTime']) if block.get('blockTime') else datetime.now(),
            'transactions': [
                self._format_transaction(tx) for tx in block.get('transactions', [])
            ],
            'rewards': block.get('rewards', []),
            'block_height': block.get('blockHeight'),
            'block_time': block.get('blockTime')
        }
    
    def _format_transaction(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        """Format transaction data for storage."""
        return {
            'signature': tx['transaction']['signatures'][0],
            'slot': tx['slot'],
            'error': tx.get('meta', {}).get('err'),
            'fee': tx.get('meta', {}).get('fee'),
            'status': 'success' if not tx.get('meta', {}).get('err') else 'failed'
        } 