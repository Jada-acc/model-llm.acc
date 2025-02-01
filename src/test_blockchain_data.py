import pytest
from src.blockchain.blockchain_data import BlockchainDataSource

def test_ethereum_data_format():
    """Test Ethereum data format structure."""
    blockchain_source = BlockchainDataSource()
    eth_data = blockchain_source.fetch_ethereum_data()
    
    assert isinstance(eth_data, dict)
    assert 'block_height' in eth_data
    assert 'timestamp' in eth_data
    assert 'hash' in eth_data
    assert 'transactions' in eth_data
    assert isinstance(eth_data['block_height'], int)
    assert isinstance(eth_data['timestamp'], int)
    assert isinstance(eth_data['hash'], str)
    assert isinstance(eth_data['transactions'], int)

def test_solana_data_format():
    """Test Solana data format structure."""
    blockchain_source = BlockchainDataSource()
    solana_data = blockchain_source.fetch_solana_data()
    
    assert isinstance(solana_data, dict)
    assert 'slot' in solana_data
    assert 'blockhash' in solana_data
    assert 'parent_slot' in solana_data
    assert 'transactions' in solana_data
    assert isinstance(solana_data['slot'], int)
    assert isinstance(solana_data['blockhash'], str)
    assert isinstance(solana_data['parent_slot'], int)
    assert isinstance(solana_data['transactions'], int)

def test_live_ethereum_fetch():
    """Test fetching live Ethereum data."""
    blockchain_source = BlockchainDataSource()
    eth_data = blockchain_source.fetch_ethereum_data()
    assert eth_data['block_height'] > 0
    assert eth_data['timestamp'] > 0
    assert len(eth_data['hash']) > 0

def test_live_solana_fetch():
    """Test fetching live Solana data."""
    blockchain_source = BlockchainDataSource()
    solana_data = blockchain_source.fetch_solana_data()
    assert solana_data['slot'] > 0
    assert len(solana_data['blockhash']) > 0
    assert solana_data['parent_slot'] >= 0

# New test cases
def test_ethereum_specific_block():
    """Test fetching a specific Ethereum block."""
    blockchain_source = BlockchainDataSource()
    # Get current block number first
    current_block = blockchain_source.fetch_ethereum_data()
    # Then fetch a specific previous block
    specific_block = blockchain_source.fetch_ethereum_data(
        block_number=current_block['block_height'] - 1
    )
    assert specific_block['block_height'] == current_block['block_height'] - 1

def test_solana_specific_slot():
    """Test fetching a specific Solana slot."""
    blockchain_source = BlockchainDataSource()
    # Get current slot first
    current_data = blockchain_source.fetch_solana_data()
    # Then fetch a specific previous slot
    specific_slot = blockchain_source.fetch_solana_data(
        slot=current_data['slot'] - 5
    )
    assert specific_slot['slot'] < current_data['slot']

def test_error_handling():
    """Test error handling for invalid inputs."""
    blockchain_source = BlockchainDataSource()
    with pytest.raises(ValueError):
        blockchain_source.fetch_data(blockchain='invalid_chain')

def test_consecutive_fetches():
    """Test multiple consecutive data fetches."""
    blockchain_source = BlockchainDataSource()
    # Test multiple Ethereum fetches
    eth_data_1 = blockchain_source.fetch_ethereum_data()
    eth_data_2 = blockchain_source.fetch_ethereum_data()
    assert eth_data_2['block_height'] >= eth_data_1['block_height']
    
    # Test multiple Solana fetches
    solana_data_1 = blockchain_source.fetch_solana_data()
    solana_data_2 = blockchain_source.fetch_solana_data()
    assert solana_data_2['slot'] >= solana_data_1['slot']

def test_blockchain_data_source_initialization():
    """Test BlockchainDataSource initialization."""
    blockchain_source = BlockchainDataSource()
    assert blockchain_source.eth_web3 is not None
    assert blockchain_source.solana_client is not None