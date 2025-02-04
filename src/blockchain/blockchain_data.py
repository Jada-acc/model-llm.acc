from typing import Dict, Any, List, Optional, Union
import logging
import time
from web3 import Web3
from solana.rpc.api import Client as SolanaClient
from solana.rpc.commitment import Commitment
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class BlockchainDataSource:
    def __init__(self, use_mock: bool = True):
        """
        Initialize blockchain data source.
        
        Args:
            use_mock: Whether to use mock data for testing
        """
        self.use_mock = use_mock
        if not use_mock:
            self.eth_web3 = Web3(Web3.HTTPProvider('https://eth-mainnet.g.alchemy.com/v2/demo'))
            self.solana_client = SolanaClient(
                'https://api.mainnet-beta.solana.com',
                commitment=Commitment("finalized"),
                timeout=30
            )
        self._initialize_logging()

    def _initialize_logging(self):
        """Initialize logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def fetch_ethereum_data(self, block_number: Optional[int] = None, include_transactions: bool = False) -> Dict:
        """
        Fetch Ethereum block data with enhanced features.
        
        Args:
            block_number: Optional specific block number to fetch
            include_transactions: Whether to include full transaction details
        """
        if self.use_mock:
            # Create more realistic varying transaction count with randomness
            base_tx = 15
            time_variation = 5 * (0.5 + 0.5 * np.sin(time.time() / 10))
            random_variation = np.random.uniform(-2, 2)
            tx_count = max(1, base_tx + time_variation + random_variation)
            
            return {
                'block_height': int(1000000 + time.time() % 1000),
                'timestamp': int(time.time()),
                'hash': '0x...',
                'transaction_count': tx_count,
                'gas_used': 1000000,
                'gas_limit': 2000000,
                'datetime': datetime.now().isoformat(),
            }
            
        try:
            if block_number is None:
                block_number = self.eth_web3.eth.block_number
            
            block = self.eth_web3.eth.get_block(block_number, full_transactions=include_transactions)
            
            data = {
                'block_height': block.number,
                'timestamp': block.timestamp,
                'hash': block.hash.hex(),
                'transaction_count': len(block.transactions),
                'gas_used': block.gasUsed,
                'gas_limit': block.gasLimit,
                'datetime': datetime.fromtimestamp(block.timestamp).isoformat(),
            }
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching Ethereum data: {str(e)}")
            raise

    def fetch_solana_data(self, slot: Optional[int] = None, include_transactions: bool = False) -> Dict:
        """
        Fetch Solana block data with enhanced features.
        
        Args:
            slot: Optional specific slot to fetch
            include_transactions: Whether to include transaction details
        """
        if self.use_mock:
            # Create more realistic varying transaction count
            base_tx = 30  # Base transactions
            time_factor = np.sin(time.time() / 15) * 10  # Slower sinusoidal variation
            random_factor = np.random.normal(0, 4)  # More random variation for Solana
            tx_count = max(1, base_tx + time_factor + random_factor)
            
            return {
                'slot': int(200000000 + time.time() % 1000),
                'transaction_count': tx_count,
                'block_time': int(time.time()),
                'block_height': int(100000000 + time.time() % 1000),
            }
            
        max_retries = 3
        retry_delay = 1
        
        try:
            if slot is None:
                slot_response = self.solana_client.get_slot()
                slot = max(0, slot_response.value - 10)

            for attempt in range(max_retries):
                try:
                    block_response = self.solana_client.get_block(
                        slot,
                        max_supported_transaction_version=0
                    )
                    
                    if block_response and hasattr(block_response, 'value'):
                        block = block_response.value
                        data = {
                            'slot': slot,
                            'blockhash': str(block.blockhash),
                            'parent_slot': block.parent_slot,
                            'transactions': len(block.transactions) if hasattr(block, 'transactions') else 0,
                            'block_time': block.block_time if hasattr(block, 'block_time') else None,
                            'block_height': block.block_height if hasattr(block, 'block_height') else None,
                            'parent_hash': str(block.previous_blockhash) if hasattr(block, 'previous_blockhash') else None,
                        }
                        
                        if include_transactions and hasattr(block, 'transactions') and block.transactions:
                            data['transaction_details'] = [
                                {
                                    'signature': str(tx.transaction.signatures[0]) if hasattr(tx, 'transaction') else None,
                                    'success': tx.meta.status.Ok if hasattr(tx, 'meta') and hasattr(tx.meta, 'status') else None,
                                }
                                for tx in block.transactions[:10]  # Limit to first 10 transactions
                            ]
                        
                        return data
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Retry {attempt + 1}/{max_retries} failed: {str(e)}")
                    slot = max(0, slot - 5)
                    time.sleep(retry_delay)
            
            raise ValueError("Failed to fetch valid block data after retries")
            
        except Exception as e:
            logger.error(f"Error fetching Solana data: {str(e)}")
            raise

    def fetch_data(self, blockchain: str = 'ethereum', include_transactions: bool = False, **kwargs) -> Dict:
        """
        Fetch data from specified blockchain with enhanced features.
        
        Args:
            blockchain: The blockchain to fetch data from ('ethereum' or 'solana')
            include_transactions: Whether to include transaction details
            **kwargs: Additional arguments for specific blockchain fetching
        """
        if blockchain.lower() == 'ethereum':
            return self.fetch_ethereum_data(
                block_number=kwargs.get('block_number'),
                include_transactions=include_transactions
            )
        elif blockchain.lower() == 'solana':
            return self.fetch_solana_data(
                slot=kwargs.get('slot'),
                include_transactions=include_transactions
            )
        else:
            raise ValueError(f"Unsupported blockchain: {blockchain}")

    def get_network_status(self) -> Dict:
        """Get status information for both networks."""
        try:
            eth_status = {
                'network': 'ethereum',
                'connected': self.eth_web3.isConnected(),
                'current_block': self.eth_web3.eth.block_number if self.eth_web3.isConnected() else None,
                'gas_price': self.eth_web3.eth.gas_price if self.eth_web3.isConnected() else None,
            }
        except Exception as e:
            logger.error(f"Error getting Ethereum status: {str(e)}")
            eth_status = {'network': 'ethereum', 'error': str(e)}

        try:
            solana_status = {
                'network': 'solana',
                'current_slot': self.solana_client.get_slot().value,
                'health': self.solana_client.get_health().value,
            }
        except Exception as e:
            logger.error(f"Error getting Solana status: {str(e)}")
            solana_status = {'network': 'solana', 'error': str(e)}

        return {
            'ethereum': eth_status,
            'solana': solana_status,
            'timestamp': datetime.utcnow().isoformat()
        }

    def get_blockchain_data(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get mock blockchain data for testing."""
        return {
            'blocks': [
                {
                    'timestamp': start_time,
                    'number': 1000,
                    'hash': '0x123...',
                    'transactions': []
                }
            ],
            'start_time': start_time,
            'end_time': end_time
        }