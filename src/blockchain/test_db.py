from blockchain.blockchain_storage import BlockchainStorage
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_database_setup():
    """Test the database setup and basic operations."""
    try:
        # Initialize storage
        storage = BlockchainStorage()
        
        # Test storing Ethereum block data
        eth_block = {
            'block_height': 1000000,
            'hash': '0x123...',
            'timestamp': datetime.now(),
            'transaction_count': 100,
            'gas_used': 2000000,
            'gas_limit': 3000000
        }
        
        success = storage.store_block_data('ethereum', eth_block)
        logger.info(f"Stored Ethereum block: {success}")
        
        # Test storing Solana block data
        sol_block = {
            'slot': 2000000,
            'blockhash': 'abc...',
            'timestamp': datetime.now(),
            'transaction_count': 200
        }
        
        success = storage.store_block_data('solana', sol_block)
        logger.info(f"Stored Solana block: {success}")
        
        # Test retrieving latest blocks
        eth_blocks = storage.get_latest_blocks('ethereum', limit=5)
        logger.info(f"Retrieved {len(eth_blocks)} Ethereum blocks")
        
        sol_blocks = storage.get_latest_blocks('solana', limit=5)
        logger.info(f"Retrieved {len(sol_blocks)} Solana blocks")
        
        # Test retrieving metrics
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        metrics = storage.get_metrics('tps', start_time, end_time)
        logger.info(f"Retrieved {len(metrics)} TPS metrics")
        
        logger.info("All database tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_database_setup() 