"""Test configuration and fixtures."""
import os
import pytest
from typing import Dict, Any

# Test configurations
TEST_CONFIGS = {
    'API': {
        'base_url': os.getenv('TEST_API_URL', 'https://api.example.com'),
        'headers': {
            'Authorization': f"Bearer {os.getenv('TEST_API_TOKEN', 'test-token')}"
        },
        'timeout': 10,
        'verify_ssl': True
    },
    'Database': {
        'connection_string': os.getenv(
            'TEST_DB_URL', 
            'sqlite:///test.db'
        ),
        'batch_size': 1000
    },
    'Ethereum': {
        'provider_url': os.getenv(
            'TEST_ETH_URL', 
            'http://localhost:8545'
        ),
        'start_block': 'latest'
    },
    'Solana': {
        'endpoint': os.getenv(
            'TEST_SOL_URL', 
            'http://localhost:8899'
        ),
        'commitment': 'confirmed'
    }
}

@pytest.fixture
def test_configs() -> Dict[str, Dict[str, Any]]:
    """Provide test configurations."""
    return TEST_CONFIGS 