import pytest
import os
from datetime import datetime
import matplotlib.pyplot as plt
from .blockchain_storage import BlockchainStorage
from .blockchain_analysis import BlockchainAnalyzer
from .blockchain_visualizer import BlockchainVisualizer

@pytest.fixture
def analyzer():
    """Create a temporary analyzer instance for testing."""
    db_path = "test_blockchain_data.db"
    storage = BlockchainStorage(db_path)
    
    # Store mock Ethereum data
    for i in range(3):
        eth_data = create_mock_ethereum_data()
        eth_data['block_height'] += i  # Ensure unique block heights
        eth_data['timestamp'] += i * 3600  # One hour difference between blocks
        if not storage.store_ethereum_data(eth_data):
            raise ValueError("Failed to store mock Ethereum data")
    
    # Store mock Solana data
    for i in range(3):
        solana_data = create_mock_solana_data()
        solana_data['slot'] += i  # Ensure unique slots
        solana_data['block_time'] += i * 3600  # One hour difference between blocks
        if not storage.store_solana_data(solana_data):
            raise ValueError("Failed to store mock Solana data")
    
    analyzer = BlockchainAnalyzer(storage)
    yield analyzer
    
    # Cleanup after tests
    if os.path.exists(db_path):
        os.remove(db_path)

def create_mock_ethereum_data():
    """Create mock Ethereum data for testing."""
    current_time = int(datetime.now().timestamp())
    return {
        'block_height': 1000000,
        'timestamp': current_time,
        'hash': '0x1234567890abcdef',
        'transaction_count': 100,
        'gas_used': 1000000,
        'gas_limit': 2000000,
        'difficulty': 1000,
        'total_difficulty': 10000,
        'size': 5000,
        'miner': '0xabcdef1234567890',
        'transaction_details': [
            {
                'hash': f'0xtx{i}',
                'from': f'0xsender{i}',
                'to': f'0xreceiver{i}',
                'value': 1000 * i,
                'gas': 21000
            }
            for i in range(10)
        ]
    }

def create_mock_solana_data():
    """Create mock Solana data for testing."""
    current_time = int(datetime.now().timestamp())
    return {
        'slot': 100000,
        'blockhash': 'solana_blockhash_123',
        'parent_slot': 99999,
        'block_time': current_time,
        'block_height': 100000,
        'parent_hash': 'solana_parent_hash_123',
        'transaction_count': 50,
        'transaction_details': [
            {
                'signature': f'solana_tx_{i}',
                'slot': 100000,
                'success': i % 2 == 0  # Alternate between success and failure
            }
            for i in range(10)
        ]
    }

@pytest.fixture
def visualizer(analyzer):
    """Create a BlockchainVisualizer instance for testing."""
    return BlockchainVisualizer(analyzer)

def test_plot_transaction_trends(visualizer, tmp_path):
    """Test transaction trends visualization."""
    save_path = tmp_path / "transaction_trends.png"
    visualizer.plot_transaction_trends(days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_plot_gas_usage(visualizer, tmp_path):
    """Test gas usage visualization."""
    save_path = tmp_path / "gas_usage.png"
    visualizer.plot_gas_usage(days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_plot_blockchain_comparison(visualizer, tmp_path):
    """Test blockchain comparison visualization."""
    save_path = tmp_path / "blockchain_comparison.png"
    visualizer.plot_blockchain_comparison(days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_plot_ethereum_transaction_patterns(visualizer, tmp_path):
    """Test Ethereum transaction patterns visualization."""
    save_path = tmp_path / "eth_transaction_patterns.png"
    visualizer.plot_transaction_patterns("ethereum", days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_plot_solana_transaction_patterns(visualizer, tmp_path):
    """Test Solana transaction patterns visualization."""
    save_path = tmp_path / "sol_transaction_patterns.png"
    visualizer.plot_transaction_patterns("solana", days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_invalid_blockchain_visualization(visualizer):
    """Test error handling for invalid blockchain."""
    with pytest.raises(ValueError):
        visualizer.plot_transaction_patterns("invalid_chain", days=7)

def test_invalid_data_period(visualizer):
    """Test error handling for invalid time period."""
    with pytest.raises(ValueError):
        visualizer.plot_transaction_trends(days=0)

def test_visualization_style(visualizer):
    """Test if visualization style is properly set."""
    assert plt.style.available
    current_style = plt.style.available[0]  # Get current style
    assert current_style is not None 