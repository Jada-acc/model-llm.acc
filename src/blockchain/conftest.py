import pytest
import os
from datetime import datetime, timedelta
from .blockchain_storage import BlockchainStorage
from .blockchain_analysis import BlockchainAnalyzer
from .blockchain_data import BlockchainDataSource

@pytest.fixture
def analyzer(tmp_path):
    """Create a BlockchainAnalyzer instance for testing."""
    # Create a temporary database file
    db_path = str(tmp_path / "test_blockchain.db")
    storage = BlockchainStorage(db_path)
    
    # Create trend data for the last 7 days
    now = datetime.now()
    for i in range(7):
        date = now - timedelta(days=i)
        timestamp = int(date.timestamp())
        
        # Store Ethereum data
        eth_data = {
            "block_height": 1000000 + i,
            "hash": f"0xabc{i}",
            "timestamp": timestamp,
            "gas_used": 2000000 + i * 100000,
            "gas_limit": 3000000,
            "difficulty": 1000000000 + i * 1000000,
            "total_difficulty": 5000000000 + i * 5000000,
            "size": 10000 + i * 100,
            "miner": f"0xminer{i}",
            "transaction_count": 100 + i * 10,
            "transaction_details": [
                {
                    "hash": f"0xtx{i}_{j}",
                    "from": f"0xsender{j}",
                    "to": f"0xreceiver{j}",
                    "value": 1000 + j * 100,
                    "gas": 21000 + j * 1000
                } for j in range(5)
            ]
        }
        storage.store_ethereum_data(eth_data)
        
        # Store Solana data
        sol_data = {
            "slot": 100000000 + i * 1000,
            "blockhash": f"sol_hash_{i}",
            "parent_slot": 100000000 + (i-1) * 1000 if i > 0 else 99999000,
            "block_time": timestamp,
            "block_height": 90000000 + i * 1000,
            "parent_hash": f"sol_hash_{i-1}" if i > 0 else "genesis",
            "transaction_count": 200 + i * 20,
            "transaction_details": [
                {
                    "signature": f"sol_tx_{i}_{j}",
                    "slot": 100000000 + i * 1000,
                    "success": True
                } for j in range(5)
            ]
        }
        storage.store_solana_data(sol_data)
        
        # Store trend data
        eth_trends = {
            "daily_transactions": {date.date().isoformat(): 1000 + i * 100},
            "daily_avg_gas": {date.date().isoformat(): 50000 + i * 5000}
        }
        storage.store_ethereum_trends(eth_trends)
        
        sol_trends = {
            "daily_transactions": {date.date().isoformat(): 2000 + i * 200},
            "daily_slots_processed": {date.date().isoformat(): 100000 + i * 10000}
        }
        storage.store_solana_trends(sol_trends)
    
    # Create and return analyzer instance
    return BlockchainAnalyzer(storage) 