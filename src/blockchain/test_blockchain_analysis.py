import pytest
import os
from datetime import datetime, timedelta
import pandas as pd
from src.blockchain.blockchain_storage import BlockchainStorage
from src.blockchain.blockchain_data import BlockchainDataSource
from src.blockchain.blockchain_analysis import BlockchainAnalyzer

def create_mock_ethereum_data():
    """Create mock Ethereum data for testing."""
    current_time = int(datetime.now().timestamp())
    return {
        'block_height': 1000000,
        'timestamp': current_time,
        'hash': '0x1234567890abcdef',
        'transactions': 100,
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
        'transactions': 50,
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

def test_ethereum_trend_analysis(analyzer):
    """Test Ethereum trend analysis."""
    trends = analyzer.analyze_ethereum_trends(days=7)
    
    assert isinstance(trends, dict)
    assert "statistics" in trends
    assert "trends" in trends
    assert "growth_rates" in trends
    
    stats = trends["statistics"]
    assert "total_blocks" in stats
    assert "avg_block_size" in stats
    assert "total_transactions" in stats
    assert isinstance(stats["total_blocks"], int)
    assert isinstance(stats["avg_block_size"], float)
    assert stats["total_transactions"] > 0  # Verify we have transactions

def test_solana_trend_analysis(analyzer):
    """Test Solana trend analysis."""
    trends = analyzer.analyze_solana_trends(days=7)
    
    assert isinstance(trends, dict)
    assert "statistics" in trends
    assert "trends" in trends
    assert "growth_rates" in trends
    
    stats = trends["statistics"]
    assert "total_slots" in stats
    assert "total_transactions" in stats
    assert isinstance(stats["total_slots"], int)
    assert isinstance(stats["total_transactions"], int)
    assert stats["total_transactions"] > 0  # Verify we have transactions

def test_transaction_pattern_analysis(analyzer):
    """Test transaction pattern analysis."""
    # Test Ethereum patterns
    eth_patterns = analyzer.get_transaction_patterns("ethereum", days=7)
    assert isinstance(eth_patterns, dict)
    assert "value_statistics" in eth_patterns
    assert "gas_statistics" in eth_patterns
    assert "address_statistics" in eth_patterns
    
    # Verify value statistics
    value_stats = eth_patterns["value_statistics"]
    assert isinstance(value_stats["avg_transaction_value"], float)
    assert isinstance(value_stats["median_transaction_value"], float)
    assert isinstance(value_stats["max_transaction_value"], float)
    
    # Verify gas statistics
    gas_stats = eth_patterns["gas_statistics"]
    assert isinstance(gas_stats["avg_gas"], float)
    assert isinstance(gas_stats["median_gas"], float)
    assert isinstance(gas_stats["max_gas"], float)
    
    # Test Solana patterns
    sol_patterns = analyzer.get_transaction_patterns("solana", days=7)
    assert isinstance(sol_patterns, dict)
    assert "transaction_statistics" in sol_patterns
    
    # Verify transaction statistics
    tx_stats = sol_patterns["transaction_statistics"]
    assert isinstance(tx_stats["success_rate"], float)
    assert isinstance(tx_stats["total_transactions"], int)
    assert isinstance(tx_stats["successful_transactions"], int)
    assert isinstance(tx_stats["failed_transactions"], int)
    assert tx_stats["total_transactions"] > 0  # Verify we have transactions

def test_chain_comparison(analyzer):
    """Test chain comparison functionality."""
    comparison = analyzer.compare_chains(days=7)
    
    assert isinstance(comparison, dict)
    assert "ethereum" in comparison
    assert "solana" in comparison
    assert "transaction_volume_ratio" in comparison
    assert "relative_growth" in comparison
    
    eth_stats = comparison["ethereum"]
    assert "daily_tx_average" in eth_stats
    assert "tx_growth_rate" in eth_stats
    assert isinstance(eth_stats["daily_tx_average"], float)
    assert isinstance(eth_stats["tx_growth_rate"], float)
    assert eth_stats["daily_tx_average"] > 0  # Verify we have transactions
    
    sol_stats = comparison["solana"]
    assert "daily_tx_average" in sol_stats
    assert "tx_growth_rate" in sol_stats
    assert isinstance(sol_stats["daily_tx_average"], float)
    assert isinstance(sol_stats["tx_growth_rate"], float)
    assert sol_stats["daily_tx_average"] > 0  # Verify we have transactions

def test_error_handling(analyzer):
    """Test error handling for invalid inputs."""
    # Test with invalid blockchain
    with pytest.raises(ValueError):
        analyzer.get_transaction_patterns("invalid_chain")
    
    # Test with invalid time period for Ethereum
    trends = analyzer.analyze_ethereum_trends(days=0)
    assert "error" in trends
    assert trends["error"] == "Days parameter must be greater than 0"
    
    # Test with invalid time period for Solana
    trends = analyzer.analyze_solana_trends(days=0)
    assert "error" in trends
    assert trends["error"] == "Days parameter must be greater than 0"
    
    # Test with negative time period
    trends = analyzer.analyze_ethereum_trends(days=-1)
    assert "error" in trends
    assert trends["error"] == "Days parameter must be greater than 0"
    
    # Test chain comparison with invalid time period
    comparison = analyzer.compare_chains(days=0)
    assert "error" in comparison 