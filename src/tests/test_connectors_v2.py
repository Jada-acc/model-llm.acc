import pytest
from unittest.mock import Mock, patch
import logging
from datetime import datetime
from src.ingestion.connectors.api_connector import APIConnector
from src.ingestion.connectors.db_connector import DatabaseConnector
from src.ingestion.connectors.ethereum_connector import EthereumConnector
from src.ingestion.connectors.solana_connector import SolanaConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def mock_api_response():
    return {'data': [{'id': 1, 'name': 'test'}]}

@pytest.fixture
def mock_db_data():
    return [{'id': 1, 'value': 'test'}]

@pytest.fixture
def mock_eth_block():
    block = Mock()
    block.number = 1000
    block.hash.hex.return_value = '0x123'
    block.parentHash.hex.return_value = '0x456'
    block.timestamp = int(datetime.now().timestamp())
    block.transactions = []
    block.gasUsed = 1000
    block.gasLimit = 2000
    block.extraData.hex.return_value = '0x789'
    return block

@pytest.mark.asyncio
async def test_api_connector(mock_api_response):
    """Test API connector functionality."""
    config = {
        'base_url': 'https://api.test.com',
        'headers': {'Authorization': 'Bearer test'},
        'timeout': 5
    }
    
    with patch('requests.Session') as mock_session:
        mock_session.return_value.get.return_value.json.return_value = mock_api_response
        mock_session.return_value.get.return_value.status_code = 200
        
        connector = APIConnector(config)
        assert await connector.connect()
        
        data = await connector.fetch_data({'endpoint': 'test'})
        assert len(data) == 1
        assert data[0]['name'] == 'test'

@pytest.mark.asyncio
async def test_db_connector(mock_db_data):
    """Test database connector functionality."""
    config = {
        'connection_string': 'sqlite:///:memory:',
        'batch_size': 100
    }
    
    with patch('sqlalchemy.create_engine') as mock_engine:
        mock_engine.return_value.connect.return_value.__enter__.return_value.execute.return_value = mock_db_data
        
        connector = DatabaseConnector(config)
        assert await connector.connect()
        
        data = await connector.fetch_data({'sql': 'SELECT * FROM test'})
        assert len(data) == 1
        assert data[0]['value'] == 'test'

@pytest.mark.asyncio
async def test_ethereum_connector(mock_eth_block):
    """Test Ethereum connector functionality."""
    config = {
        'provider_url': 'http://localhost:8545',
        'start_block': 'latest'
    }
    
    with patch('web3.Web3') as mock_web3:
        mock_web3.return_value.eth.get_block.return_value = mock_eth_block
        mock_web3.return_value.eth.block_number = 1000
        
        connector = EthereumConnector(config)
        assert await connector.connect()
        
        data = await connector.fetch_data({'start_block': 1000, 'end_block': 1000})
        assert len(data) == 1
        assert data[0]['number'] == 1000

@pytest.mark.asyncio
async def test_solana_connector():
    """Test Solana connector functionality."""
    config = {
        'endpoint': 'http://localhost:8899',
        'commitment': 'confirmed'
    }
    
    mock_block = {
        'parentSlot': 99,
        'blockhash': 'hash123',
        'previousBlockhash': 'hash122',
        'transactions': [],
        'rewards': [],
        'blockHeight': 100,
        'blockTime': int(datetime.now().timestamp())
    }
    
    with patch('solana.rpc.api.Client') as mock_client:
        mock_client.return_value.get_block.return_value = {'result': mock_block}
        
        connector = SolanaConnector(config)
        assert await connector.connect()
        
        data = await connector.fetch_data({'slot': 100})
        assert len(data) == 1
        assert data[0]['slot'] == 100 