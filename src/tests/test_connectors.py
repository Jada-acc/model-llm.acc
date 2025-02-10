import unittest
from unittest.mock import Mock, patch
import logging
from datetime import datetime
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
            'connection_string': 'sqlite:///:memory:',  # Use in-memory SQLite for testing
            'batch_size': 100
        }
        
        # Ethereum Connector config
        self.eth_config = {
            'provider_url': 'http://localhost:8545',
            'start_block': 'latest',
            'retry_count': 3,
            'retry_delay': 1
        }
        
        # Solana Connector config
        self.sol_config = {
            'endpoint': 'http://localhost:8899',
            'commitment': 'confirmed',
            'retry_count': 3,
            'retry_delay': 1
        }
    
    def tearDown(self):
        """Clean up test environment."""
        pass
    
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
    
    @patch('web3.Web3')
    def test_ethereum_connector(self, MockWeb3):
        """Test Ethereum connector functionality."""
        # Create mock provider
        mock_provider = Mock(name='HTTPProvider')
        MockWeb3.HTTPProvider = Mock(return_value=mock_provider)
        
        # Create mock eth object
        mock_eth = Mock(name='eth')
        mock_eth.block_number = 1000
        mock_eth.get_block.return_value = {
            'number': 1000,
            'hash': '0x123',
            'parentHash': '0x456',
            'timestamp': int(datetime.now().timestamp()),
            'transactions': [],
            'gasUsed': 1000,
            'gasLimit': 2000,
            'extraData': '0x789',
            'baseFeePerGas': None
        }
        
        # Create mock web3 instance
        mock_web3 = Mock(name='web3')
        mock_web3.eth = mock_eth
        mock_web3.is_connected.return_value = True
        
        # Set up the Web3 class mock
        MockWeb3.side_effect = lambda _: mock_web3
        
        # Create and test connector
        connector = EthereumConnector(self.eth_config)
        self.assertTrue(connector.connect())
        
        # Test data fetching
        data = connector.fetch_data({
            'start_block': 1000,
            'end_block': 1000
        })
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['number'], 1000)
    
    @patch('solana.rpc.api.Client')
    def test_solana_connector(self, MockClient):
        """Test Solana connector functionality."""
        # Create mock client with responses
        mock_client = Mock(name='SolanaClient')
        
        # Mock version response
        mock_client.get_version = Mock(return_value={
            'jsonrpc': '2.0',
            'result': {'version': '1.0'},
            'id': 1
        })
        
        # Mock block data
        mock_block = {
            'parentSlot': 99,
            'blockhash': 'hash123',
            'previousBlockhash': 'hash122',
            'transactions': [],
            'rewards': [],
            'blockHeight': 100,
            'blockTime': int(datetime.now().timestamp())
        }
        
        # Mock get_block response
        mock_client.get_block = Mock(return_value={
            'jsonrpc': '2.0',
            'result': mock_block,
            'id': 1
        })
        
        # Set up the Client class mock
        MockClient.side_effect = lambda endpoint: mock_client
        
        # Create and test connector
        connector = SolanaConnector(self.sol_config)
        self.assertTrue(connector.connect())
        
        # Test data fetching
        data = connector.fetch_data({
            'slot': 100,
            'until_slot': 100
        })
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['slot'], 100)

if __name__ == '__main__':
    unittest.main() 