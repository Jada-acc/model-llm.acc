import pytest
import os
from src.blockchain.blockchain_storage import BlockchainStorage
from src.blockchain.blockchain_data import BlockchainDataSource

@pytest.fixture
def storage():
    """Create a temporary storage instance for testing."""
    db_path = "test_blockchain_data.db"
    storage = BlockchainStorage(db_path)
    yield storage
    # Cleanup after tests
    if os.path.exists(db_path):
        os.remove(db_path)

def test_storage_initialization(storage):
    """Test storage initialization."""
    assert os.path.exists(storage.db_path)

def test_ethereum_data_storage(storage):
    """Test storing and retrieving Ethereum data."""
    # Fetch some real Ethereum data
    data_source = BlockchainDataSource()
    eth_data = data_source.fetch_ethereum_data(include_transactions=True)
    
    # Store the data
    success = storage.store_ethereum_data(eth_data)
    assert success is True
    
    # Retrieve the latest block
    latest_block = storage.get_latest_ethereum_block()
    assert latest_block is not None
    assert latest_block['block_height'] == eth_data['block_height']

def test_solana_data_storage(storage):
    """Test storing and retrieving Solana data."""
    # Fetch some real Solana data
    data_source = BlockchainDataSource()
    solana_data = data_source.fetch_solana_data(include_transactions=True)
    
    # Store the data
    success = storage.store_solana_data(solana_data)
    assert success is True
    
    # Retrieve the latest slot
    latest_slot = storage.get_latest_solana_slot()
    assert latest_slot is not None
    assert latest_slot['slot'] == solana_data['slot']

def test_block_range_retrieval(storage):
    """Test retrieving a range of blocks."""
    data_source = BlockchainDataSource()
    
    # Store multiple Ethereum blocks
    for i in range(3):
        eth_data = data_source.fetch_ethereum_data()
        storage.store_ethereum_data(eth_data)
    
    # Get block range
    blocks = storage.get_block_range('ethereum', 0, 999999999)
    assert len(blocks) > 0

def test_error_handling(storage):
    """Test error handling for invalid data."""
    # Try to store invalid data
    invalid_data = {}
    success = storage.store_ethereum_data(invalid_data)
    assert success is False