from solana.rpc.api import Client
from typing import Dict, Any, List, Optional
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
        self.batch_size = config.get('batch_size', 100)
    
    def connect(self) -> bool:
        """Establish connection to Solana node."""
        try:
            self.client = Client(self.endpoint)
            # Test connection
            response = self.client.get_version()
            connected = 'result' in response
            if connected:
                logger.info("Successfully connected to Solana node")
            else:
                logger.error("Failed to connect to Solana node")
            return connected
        except Exception as e:
            logger.error(f"Error connecting to Solana node: {str(e)}")
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
            
            # Parse query parameters
            slot = query.get('slot')
            until_slot = query.get('until_slot')
            include_transactions = query.get('include_transactions', True)
            
            blocks = []
            if slot and until_slot:
                for current_slot in range(slot, until_slot + 1):
                    block = self._get_block(current_slot, include_transactions)
                    if block:
                        blocks.append(block)
            else:
                # Get latest block
                block = self._get_block(None, include_transactions)
                if block:
                    blocks.append(block)
            
            logger.info(f"Fetched {len(blocks)} blocks from Solana")
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
    
    def _get_block(self, slot: Optional[int], include_transactions: bool) -> Optional[Dict[str, Any]]:
        """Get block data for a specific slot."""
        try:
            response = self.client.get_block(
                slot,
                encoding='json',
                commitment=self.commitment,
                max_supported_transaction_version=0
            )
            
            if 'result' not in response:
                return None
            
            block_data = response['result']
            return self._format_block(block_data, include_transactions)
            
        except Exception as e:
            logger.error(f"Error getting block data: {str(e)}")
            return None
    
    def _format_block(self, block: Dict[str, Any], include_transactions: bool) -> Dict[str, Any]:
        """Format block data for storage."""
        return {
            'slot': block['parentSlot'] + 1,
            'parent_slot': block['parentSlot'],
            'blockhash': block['blockhash'],
            'previous_blockhash': block['previousBlockhash'],
            'timestamp': datetime.now().isoformat(),  # Solana doesn't provide block timestamps
            'transactions': [
                self._format_transaction(tx) for tx in block.get('transactions', [])
            ] if include_transactions else [],
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