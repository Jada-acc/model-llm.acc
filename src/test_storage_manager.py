import os
import logging
import json
from pathlib import Path
from cloud.cloud_manager import CloudManager
from data_ingestion.storage_manager import StorageManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_storage_manager():
    """Test storage manager functionality."""
    
    # Test configuration
    config = {
        'provider': 'aws',
        'region': 'us-west-2',
        'cloud_path': f"s3://{input('Enter your S3 bucket name: ')}/data"
    }
    
    try:
        # Initialize components
        cloud_manager = CloudManager(config)
        storage_manager = StorageManager(config, cloud_manager)
        logger.info("Successfully initialized storage manager")
        
        # Create test data directory
        Path("data/test").mkdir(parents=True, exist_ok=True)
        
        # Create test files for different categories
        test_files = {
            # Blockchain data
            ("raw", "blockchain"): {
                "filename": "blockchain_data.json",
                "content": {"block_height": 12345, "transactions": []}
            },
            # AI model output
            ("raw", "ai_models"): {
                "filename": "model_predictions.json",
                "content": {"model": "gpt-4", "predictions": [1, 2, 3]}
            },
            # Processed features
            ("processed", "features"): {
                "filename": "extracted_features.json",
                "content": {"features": ["f1", "f2", "f3"]}
            },
            # Model artifact
            ("models", "artifacts"): {
                "filename": "model_config.json",
                "content": {"layers": [64, 32, 16], "activation": "relu"}
            }
        }
        
        # Test storing different types of data
        for (category, subcategory), file_info in test_files.items():
            # Create test file
            file_path = f"data/test/{file_info['filename']}"
            with open(file_path, 'w') as f:
                json.dump(file_info['content'], f)
            
            # Store file with metadata
            storage_path = storage_manager.store_data(
                file_path=file_path,
                category=category,
                subcategory=subcategory,
                metadata={
                    "description": f"Test {category}/{subcategory} data",
                    "version": "1.0",
                    "timestamp": "2024-03-20T12:00:00"
                }
            )
            logger.info(f"Stored {category}/{subcategory} data at: {storage_path}")
            
            # Test retrieval
            retrieved_path = storage_manager.get_data(storage_path)
            logger.info(f"Retrieved data to: {retrieved_path}")
            
            # Test metadata retrieval
            metadata = storage_manager.get_metadata(storage_path)
            logger.info(f"Retrieved metadata for {storage_path}: {metadata}")
            
            # Cleanup local files
            os.remove(file_path)
            if os.path.exists(retrieved_path):
                os.remove(retrieved_path)
        
        # Test listing files
        for category in storage_manager.storage_structure:
            files = storage_manager.list_data(category)
            logger.info(f"Files in {category}: {files}")
            
            for subcategory in storage_manager.storage_structure[category]:
                subfiles = storage_manager.list_data(category, subcategory)
                logger.info(f"Files in {category}/{subcategory}: {subfiles}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing storage manager: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_storage_manager()
    if success:
        print("Storage manager test completed successfully!")
    else:
        print("Storage manager test failed. Check the logs for details.")