import pytest
from datetime import datetime, timedelta
import redis
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float
from src.storage.storage_optimizer import StorageOptimizer, BlockchainBlock, Transaction, BlockMetrics
from sqlalchemy.sql import text

def is_redis_available():
    """Check if Redis is available."""
    try:
        client = redis.Redis(host='localhost', port=6379, db=0)
        client.ping()
        return True
    except redis.ConnectionError:
        return False

@pytest.fixture(scope="session")
def test_db():
    """Create test database and tables."""
    db_url = "sqlite:///test_blockchain_data.db"
    engine = create_engine(db_url)
    metadata = MetaData()

    # Create test tables
    blockchain_blocks = Table(
        'blockchain_blocks', metadata,
        Column('id', Integer, primary_key=True),
        Column('blockchain', String),
        Column('timestamp', DateTime),
        Column('block_number', Integer)
    )

    transactions = Table(
        'transactions', metadata,
        Column('id', Integer, primary_key=True),
        Column('block_id', Integer),
        Column('hash', String)
    )

    block_metrics = Table(
        'block_metrics', metadata,
        Column('id', Integer, primary_key=True),
        Column('block_id', Integer),
        Column('timestamp', DateTime),
        Column('value', Float)
    )

    metadata.create_all(engine)
    return db_url

@pytest.fixture
def optimizer(test_db):
    """Create a StorageOptimizer instance for testing."""
    redis_url = "redis://localhost:6379/0" if is_redis_available() else None
    return StorageOptimizer(
        db_url=test_db,
        redis_url=redis_url
    )

def test_table_optimization(optimizer):
    """Test database table optimization."""
    assert optimizer.optimize_tables() is True

def test_data_partitioning(optimizer):
    """Test data partitioning functionality."""
    result = optimizer.partition_data(
        table_name="blockchain_blocks",
        partition_key="timestamp",
        interval="month"
    )
    assert result is True

def test_caching(optimizer):
    """Test data caching functionality."""
    if not is_redis_available():
        pytest.skip("Redis not available")
        
    test_data = {"key": "value"}
    
    # Test cache setting
    assert optimizer.cache_frequent_data("test_key", test_data) is True
    
    # Test cache retrieval
    cached_data = optimizer.get_cached_data("test_key")
    assert cached_data is not None
    assert "value" in str(cached_data)

def test_storage_analysis(optimizer):
    """Test storage analysis functionality."""
    analysis = optimizer.analyze_storage_efficiency()
    assert isinstance(analysis, dict)
    assert "table_sizes" in analysis

def test_invalid_cache_key(optimizer):
    """Test handling of invalid cache keys."""
    assert optimizer.get_cached_data("nonexistent_key") is None

def test_cache_expiration(optimizer):
    """Test cache expiration functionality."""
    if not is_redis_available():
        pytest.skip("Redis not available")
        
    test_data = {"temporary": "data"}
    
    # Test cache setting with short expiration
    assert optimizer.cache_frequent_data("temp_key", test_data, expire_seconds=1) is True
    
    # Wait for expiration
    import time
    time.sleep(2)
    
    # Verify data is expired
    assert optimizer.get_cached_data("temp_key") is None

def test_error_handling(optimizer):
    """Test error handling in storage operations."""
    # Test with invalid table name
    result = optimizer.partition_data("nonexistent_table")
    assert result is False
    
    # Test with invalid database connection
    invalid_optimizer = StorageOptimizer("sqlite:///nonexistent.db")
    assert invalid_optimizer.optimize_tables() is False

def test_redis_connection_failure(optimizer):
    """Test handling of Redis connection failures."""
    # Create optimizer with invalid Redis URL
    failed_optimizer = StorageOptimizer(
        db_url="sqlite:///test_blockchain_data.db",
        redis_url="redis://nonexistent:6379/0"
    )
    
    # Verify caching operations fail gracefully
    assert failed_optimizer.cache_frequent_data("key", "value") is False
    assert failed_optimizer.get_cached_data("key") is None

def test_data_compression(optimizer):
    """Test data compression and decompression."""
    # Test data
    test_data = {
        "id": 1,
        "name": "test",
        "values": [1, 2, 3],
        "metadata": {"type": "test", "version": 1}
    }
    
    # Test compression
    compressed = optimizer.compress_data(test_data)
    assert compressed is not None
    assert isinstance(compressed, str)
    
    # Test decompression
    decompressed = optimizer.decompress_data(compressed)
    assert decompressed == test_data

def test_old_data_compression(optimizer, test_db):
    """Test compression of old data."""
    # Insert some test data first
    with optimizer.SessionFactory() as session:
        session.execute(text("""
            INSERT INTO blockchain_blocks (blockchain, timestamp, block_number)
            VALUES 
            ('ethereum', :old_date, 1000),
            ('ethereum', :recent_date, 1001)
        """), {
            "old_date": datetime.now() - timedelta(days=40),
            "recent_date": datetime.now()
        })
        session.commit()
    
    # Test compression
    assert optimizer.compress_old_data("blockchain_blocks", days_threshold=30) is True
    
    # Verify old data was compressed
    with optimizer.SessionFactory() as session:
        old_data = session.execute(text("""
            SELECT COUNT(*) as count
            FROM compressed_data
            WHERE table_name = 'blockchain_blocks'
        """)).first()
        
        assert old_data.count > 0 