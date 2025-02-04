import logging
from datetime import datetime, timedelta
from pipeline.pipeline_manager import DataPipeline
from storage.storage_optimizer import StorageOptimizer
from storage.init_db import init_database
from security.auth_manager import AuthManager
from security.encryption import DataEncryption

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    # Initialize database
    db_url = "sqlite:///blockchain_data.db"
    init_database(db_url)
    
    # Initialize security components
    auth_manager = AuthManager(secret_key="your-secret-key")
    encryption = DataEncryption()
    
    # Get authentication token
    auth_token = auth_manager.generate_token("pipeline_user")
    
    # Initialize components
    storage = StorageOptimizer(db_url=db_url)
    pipeline = DataPipeline(storage, auth_manager, encryption)
    
    # Add some processing steps
    def clean_data(data):
        print("Cleaning data...")
        return data
    
    def transform_sensitive_data(data):
        print("Transforming sensitive data...")
        return data
    
    # Add steps to pipeline (mark sensitive operations as requiring auth)
    pipeline.add_processing_step(clean_data)
    pipeline.add_processing_step(transform_sensitive_data, requires_auth=True)
    
    # Run pipeline for last hour
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)
    
    print(f"Running pipeline from {start_time} to {end_time}")
    success = pipeline.run_pipeline(start_time, end_time, auth_token)
    
    print(f"Pipeline execution {'successful' if success else 'failed'}")

if __name__ == "__main__":
    main() 