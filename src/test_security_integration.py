import logging
from datetime import datetime, timedelta
from security.auth_manager import AuthManager
from security.encryption import DataEncryption
from storage.storage_optimizer import StorageOptimizer
from storage.data_retriever import DataRetriever
from pipeline.secure_pipeline import SecurePipeline
from storage.init_db import init_database
from config.security_config import SecurityConfig

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_security_flow():
    """Test the complete security flow."""
    
    # 1. Initialize components
    print("\n1. Initializing components...")
    config = SecurityConfig()
    auth_manager = AuthManager(config.config['jwt']['secret_key'])
    encryption = DataEncryption()
    
    # 2. Set up database
    print("\n2. Setting up database...")
    db_url = "sqlite:///test_secure_pipeline.db"
    init_database(db_url)
    storage = StorageOptimizer(db_url)
    
    # 3. Create secure pipeline
    print("\n3. Creating secure pipeline...")
    pipeline = SecurePipeline(storage, auth_manager, encryption)
    
    # 4. Generate authentication token
    print("\n4. Generating auth token...")
    token = auth_manager.generate_token("test_user")
    print(f"Generated token: {token[:20]}...")
    
    # 5. Add secure processing steps
    def process_sensitive_data(data):
        print("Processing sensitive data...")
        data['processed'] = True
        return data
    
    pipeline.add_processing_step(process_sensitive_data, requires_auth=True)
    
    # 6. Run pipeline with authentication
    print("\n6. Running secure pipeline...")
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)
    
    print(f"Test time range: {start_time} to {end_time}")
    
    # Create test blockchain data
    test_data = {
        'blocks': [{
            'number': 1000,
            'hash': '0x123...',
            'timestamp': start_time,
            'transactions': []
        }],
        'sensitive_info': 'test123',
        'timestamp': datetime.now().isoformat(),
        'metrics': {
            'value': 100,
            'type': 'test'
        }
    }
    
    # First, try with valid token
    print("\nTrying with valid token:")
    # Pass test_data to run_pipeline
    success = pipeline.run_pipeline(start_time, end_time, token, test_data)
    print(f"Pipeline execution: {'Success' if success else 'Failed'}")
    
    # Try with invalid token
    print("\nTrying with invalid token:")
    try:
        pipeline.run_pipeline(start_time, end_time, "invalid_token")
    except PermissionError as e:
        print(f"Expected error: {str(e)}")
    
    # 7. Retrieve and decrypt data
    print("\n7. Retrieving processed data...")
    retriever = DataRetriever(storage, encryption, auth_manager)
    decrypted_data = retriever.get_processed_data(start_time, end_time, token)
    
    if decrypted_data:
        print("Successfully retrieved and decrypted data:")
        print(decrypted_data)
    else:
        print("No data retrieved")
    
    # 8. Test rate limiting
    print("\n8. Testing rate limiting...")
    for i in range(config.config['auth']['max_failed_attempts'] + 1):
        try:
            pipeline.run_pipeline(start_time, end_time, "invalid_token")
        except PermissionError as e:
            print(f"Attempt {i+1}: {str(e)}")

if __name__ == "__main__":
    test_security_flow() 