from typing import Dict, Any
from datetime import datetime
import random

class TestBlockchainDataSource:
    """Mock blockchain data source for testing purposes."""
    
    def __init__(self, eth_tps: float = 0.5, sol_tps: float = 1.0, should_fail: bool = False):
        """
        Initialize test data source.
        
        Args:
            eth_tps: Mock Ethereum transactions per second
            sol_tps: Mock Solana transactions per second
            should_fail: Whether the data source should simulate failures
        """
        self.eth_tps = eth_tps
        self.sol_tps = sol_tps
        self.should_fail = should_fail
        self.call_count = 0

    def fetch_ethereum_data(self) -> Dict[str, Any]:
        """Mock Ethereum data fetch."""
        self.call_count += 1
        
        if self.should_fail and self.call_count % 3 == 0:
            raise Exception("Simulated Ethereum data fetch failure")
            
        return {
            'timestamp': datetime.now(),
            'transaction_count': self.eth_tps * 60,  # Transactions per minute
            'gas_price': random.uniform(50, 100),
            'block_number': 1000000 + self.call_count
        }

    def fetch_solana_data(self) -> Dict[str, Any]:
        """Mock Solana data fetch."""
        self.call_count += 1
        
        if self.should_fail and self.call_count % 3 == 0:
            raise Exception("Simulated Solana data fetch failure")
            
        return {
            'timestamp': datetime.now(),
            'transaction_count': self.sol_tps * 60,  # Transactions per minute
            'slot': 2000000 + self.call_count,
            'network_status': 'active'
        }

    def reset(self) -> None:
        """Reset the call counter."""
        self.call_count = 0