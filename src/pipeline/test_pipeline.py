import pytest
from datetime import datetime, timedelta
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float
from src.pipeline.pipeline_manager import DataPipeline
from src.storage.storage_optimizer import StorageOptimizer
from src.security.auth_manager import AuthManager
from src.security.encryption import DataEncryption

@pytest.fixture(scope="session")
def test_db():
    """Create test database."""
    db_url = "sqlite:///test_pipeline.db"
    engine = create_engine(db_url)
    metadata = MetaData()

    # Create test tables
    processed_data = Table(
        'processed_data', metadata,
        Column('id', Integer, primary_key=True),
        Column('timestamp', DateTime),
        Column('data', String)
    )

    metadata.create_all(engine)
    return db_url

@pytest.fixture
def auth_manager():
    return AuthManager("test_secret_key")

@pytest.fixture
def encryption():
    return DataEncryption()

@pytest.fixture
def pipeline(test_db, auth_manager, encryption):
    """Create a test pipeline instance."""
    storage = StorageOptimizer(test_db)
    return DataPipeline(storage, auth_manager, encryption)

def test_pipeline_processing(pipeline):
    """Test basic pipeline processing."""
    def sample_processing_step(data):
        data['processed'] = True
        return data
    
    pipeline.add_processing_step(sample_processing_step)
    
    test_data = {'raw': 'data'}
    processed = pipeline.process_data(test_data)
    
    assert processed['processed'] is True

def test_pipeline_execution(pipeline):
    """Test full pipeline execution."""
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)
    
    assert pipeline.run_pipeline(start_time, end_time) is True

def test_processing_error_handling(pipeline):
    """Test error handling in processing steps."""
    def failing_step(data):
        raise ValueError("Test error")
    
    pipeline.add_processing_step(failing_step)
    
    with pytest.raises(ValueError):
        pipeline.process_data({'test': 'data'})

def test_multiple_processing_steps(pipeline):
    """Test pipeline with multiple processing steps."""
    def step1(data):
        data['step1'] = True
        return data
        
    def step2(data):
        data['step2'] = True
        return data
    
    pipeline.add_processing_step(step1)
    pipeline.add_processing_step(step2)
    
    result = pipeline.process_data({'initial': 'data'})
    
    assert result['step1'] is True
    assert result['step2'] is True

def test_authenticated_processing(pipeline, auth_manager):
    """Test processing step that requires authentication."""
    def secure_step(data):
        data['secure'] = True
        return data
        
    pipeline.add_processing_step(secure_step, requires_auth=True)
    
    # Get valid token
    token = auth_manager.generate_token("test_user")
    
    test_data = {'raw': 'data'}
    processed = pipeline.process_data(test_data, auth_token=token)
    
    assert processed['secure'] is True
    
    # Test with invalid token
    with pytest.raises(PermissionError):
        pipeline.process_data(test_data, auth_token="invalid_token")