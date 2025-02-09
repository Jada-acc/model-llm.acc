import logging
import sys
from typing import Dict, Any
from src.ingestion.connectors.api_connector import APIConnector
from src.ingestion.connectors.db_connector import DatabaseConnector
from src.ingestion.connectors.ethereum_connector import EthereumConnector
from src.ingestion.connectors.solana_connector import SolanaConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_connector(name: str, connector, config: Dict[str, Any]) -> bool:
    """Verify a single connector."""
    logger.info(f"\nTesting {name} connector...")
    try:
        # Initialize connector
        instance = connector(config)
        
        # Test connection
        if not instance.connect():
            logger.error(f"❌ {name}: Failed to connect")
            return False
        
        # Test health check
        health = instance.health_check()
        if health['status'] != 'healthy':
            logger.error(f"❌ {name}: Health check failed - {health}")
            return False
        
        # Test disconnection
        if not instance.disconnect():
            logger.error(f"❌ {name}: Failed to disconnect")
            return False
        
        logger.info(f"✅ {name}: All checks passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ {name}: Error during verification - {str(e)}")
        return False

def main():
    """Verify all connector configurations."""
    # Load configurations (you might want to load these from a config file)
    configs = {
        'API': {
            'base_url': 'https://api.example.com',
            'headers': {'Authorization': 'Bearer YOUR_TOKEN'},
            'timeout': 10,
            'verify_ssl': True
        },
        'Database': {
            'connection_string': 'sqlite:///test.db',
            'batch_size': 1000
        },
        'Ethereum': {
            'provider_url': 'http://localhost:8545',
            'start_block': 'latest'
        },
        'Solana': {
            'endpoint': 'http://localhost:8899',
            'commitment': 'confirmed'
        }
    }
    
    # Map of connectors to test
    connectors = {
        'API': APIConnector,
        'Database': DatabaseConnector,
        'Ethereum': EthereumConnector,
        'Solana': SolanaConnector
    }
    
    # Run verifications
    results = []
    for name, connector in connectors.items():
        config = configs.get(name, {})
        results.append(verify_connector(name, connector, config))
    
    # Print summary
    logger.info("\n=== Connection Verification Summary ===")
    total = len(results)
    passed = sum(results)
    logger.info(f"Total Connectors: {total}")
    logger.info(f"Passed: {passed}")
    logger.info(f"Failed: {total - passed}")
    
    # Return non-zero exit code if any failures
    return 0 if all(results) else 1

if __name__ == '__main__':
    sys.exit(main()) 