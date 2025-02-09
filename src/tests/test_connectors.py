import unittest
from unittest.mock import Mock, patch
import logging
from datetime import datetime
import json
import requests
from sqlalchemy import create_engine, text
from src.ingestion.connectors.api_connector import APIConnector
from src.ingestion.connectors.db_connector import DatabaseConnector
from src.ingestion.connectors.ethereum_connector import EthereumConnector
from src.ingestion.connectors.solana_connector import SolanaConnector

logger = logging.getLogger(__name__)

class TestConnectors(unittest.TestCase):
    """Test suite for data connectors."""
    
    def setUp(self):
        """Set up test environment."""
        # API Connector config
        self.api_config = {
            'base_url': 'https://api.test.com',
            'headers': {'Authorization': 'Bearer test'},
            'timeout': 5,
            'verify_ssl': False
        }
        
        # Database Connector config
        self.db_config = {
            'connection_string': 'sqlite:///test.db',
            'batch_size': 100
        }
        
        # Ethereum Connector config
        self.eth_config = {
            'provider_url': 'http://localhost:8545',
            'start_block': 'latest'
        }
        
        # Solana Connector config
        self.sol_config = {
            'endpoint': 'http://localhost:8899',
            'commitment': 'confirmed'
        }
    
    def tearDown(self):
        """Clean up test environment."""
        import os
        if os.path.exists('test.db'):
            os.remove('test.db')
    
    @patch('requests.Session')
    def test_api_connector(self, mock_session):
        """Test API connector functionality."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': [{'id': 1, 'name': 'test'}]}
        mock_session.return_value.get.return_value = mock_response
        mock_session.return_value.request.return_value = mock_response
        
        connector = APIConnector(self.api_config)
        
        # Test connection
        self.assertTrue(connector.connect())
        
        # Test data fetching
        data = connector.fetch_data({
            'endpoint': 'users',
            'method': 'GET',
            'params': {'page': 1}
        })
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['name'], 'test')
        
        # Test disconnection
        self.assertTrue(connector.disconnect())
    
    def test_db_connector(self):
        """Test database connector functionality."""
        connector = DatabaseConnector(self.db_config)
        
        # Test connection
        self.assertTrue(connector.connect())
        
        # Create test table and data
        with connector.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table 
                (id INTEGER PRIMARY KEY, name TEXT)
            """))
            conn.execute(text("""
                INSERT INTO test_table (id, name) VALUES 
                (1, 'test1'), (2, 'test2')
            """))
            conn.commit()
        
        # Test data fetching
        data = connector.fetch_data({
            'sql': 'SELECT * FROM test_table WHERE id > :min_id',
            'params': {'min_id': 1}
        })
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['name'], 'test2')
        
        # Test chunked fetching
        data = connector.fetch_data({
            'sql': 'SELECT * FROM test_table',
            'use_chunks': True,
            'chunk_size': 1
        })
        self.assertEqual(len(data), 2)
        
        # Test disconnection
        self.assertTrue(connector.disconnect())
    
    @patch('web3.Web3')
    def test_ethereum_connector(self, mock_web3):
        """Test Ethereum connector functionality."""
        # Mock Web3 responses
        mock_web3.HTTPProvider.return_value = Mock()
        mock_web3.return_value.is_connected.return_value = True
        
        mock_block = Mock()
        mock_block.number = 1000
        mock_block.hash.hex.return_value = '0x123'
        mock_block.parentHash.hex.return_value = '0x456'
        mock_block.timestamp = int(datetime.now().timestamp())
        mock_block.transactions = []
        mock_block.gasUsed = 1000
        mock_block.gasLimit = 2000
        mock_block.extraData.hex.return_value = '0x789'
        
        mock_web3.return_value.eth.get_block.return_value = mock_block
        mock_web3.return_value.eth.block_number = 1000
        
        connector = EthereumConnector(self.eth_config)
        
        # Test connection
        self.assertTrue(connector.connect())
        
        # Test data fetching
        data = connector.fetch_data({
            'start_block': 1000,
            'end_block': 1000
        })
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['number'], 1000)
        
        # Test disconnection
        self.assertTrue(connector.disconnect())
    
    @patch('solana.rpc.api.Client')
    def test_solana_connector(self, mock_client):
        """Test Solana connector functionality."""
        # Mock Solana client responses
        mock_client.return_value.get_version.return_value = {'result': {'version': '1.0'}}
        
        mock_block = {
            'parentSlot': 99,
            'blockhash': 'hash123',
            'previousBlockhash': 'hash122',
            'transactions': [],
            'rewards': [],
            'blockHeight': 100,
            'blockTime': int(datetime.now().timestamp())
        }
        
        mock_client.return_value.get_block.return_value = {'result': mock_block}
        
        connector = SolanaConnector(self.sol_config)
        
        # Test connection
        self.assertTrue(connector.connect())
        
        # Test data fetching
        data = connector.fetch_data({
            'slot': 100,
            'until_slot': 100
        })
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['slot'], 100)
        
        # Test disconnection
        self.assertTrue(connector.disconnect())

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main() 