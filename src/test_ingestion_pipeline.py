import os
import logging
import json
import pandas as pd
from pathlib import Path
from cloud.cloud_manager import CloudManager
from data_ingestion.storage_manager import StorageManager
from data_ingestion.ingestion_pipeline import (
    IngestionPipeline,
    FileDataSource,
    APIDataSource
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_ingestion_pipeline():
    """Test ingestion pipeline functionality."""
    
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
        ingestion_pipeline = IngestionPipeline(storage_manager)
        logger.info("Successfully initialized ingestion pipeline")
        
        # Create test data directory
        Path("data/test").mkdir(parents=True, exist_ok=True)
        
        # Test 1a: Ethereum blockchain data (JSON)
        ethereum_data = {
            "block_height": 12345,
            "transactions": [
                {"tx_id": "0x123", "value": 1.23},
                {"tx_id": "0x456", "value": 4.56}
            ]
        }
        eth_file = Path("data/test/ethereum_data.json")
        with open(eth_file, 'w') as f:
            json.dump(ethereum_data, f)
        
        # Test Ethereum data ingestion
        eth_source = FileDataSource(str(eth_file))
        eth_path = ingestion_pipeline.ingest_data(
            data_source=eth_source,
            source_type="blockchain",
            metadata={"chain": "ethereum", "network": "mainnet", "version": "1.0"}
        )
        logger.info(f"Ingested Ethereum data to: {eth_path}")

        # Test 1b: Solana blockchain data (JSON)
        solana_data = {
            "slot": 123456789,
            "blockhash": "CkY4xZJZJ9VFgfWGYxGKqzX5RB1AjZFwYcUXztmHDfNA",
            "transactions": [
                {
                    "signature": "5KtPn1LGuxhFiwjxEqHXVcRLqQJk8rYtAjkDwJDgx8G9NPYjQkuV3NKn9FgQE8RbxPVtSwGHGwjbxEGHJqUbMPTz",
                    "value": 0.01
                }
            ]
        }
        sol_file = Path("data/test/solana_data.json")
        with open(sol_file, 'w') as f:
            json.dump(solana_data, f)
        
        # Test Solana data ingestion
        sol_source = FileDataSource(str(sol_file))
        sol_path = ingestion_pipeline.ingest_data(
            data_source=sol_source,
            source_type="blockchain",
            metadata={"chain": "solana", "network": "mainnet-beta", "version": "1.0"}
        )
        logger.info(f"Ingested Solana data to: {sol_path}")
        
        # Test 2: AI model data (CSV)
        model_data = pd.DataFrame({
            'model': ['gpt-4'] * 3,
            'predictions': [0.1, 0.2, 0.3]
        })
        model_file = Path("data/test/model_predictions.csv")
        model_data.to_csv(model_file, index=False)
        
        # Test AI model data ingestion
        model_source = FileDataSource(str(model_file))
        model_path = ingestion_pipeline.ingest_data(
            data_source=model_source,
            source_type="ai_models",
            metadata={"model_version": "1.0", "timestamp": "2024-03-20T12:00:00"}
        )
        logger.info(f"Ingested AI model data to: {model_path}")
        
        # Test 3: Sensor data (JSON)
        sensor_data = {
            "sensor_id": "SENSOR001",
            "timestamp": "2024-03-20T12:00:00",
            "readings": {
                "temperature": 25.6,
                "humidity": 65.4
            }
        }
        sensor_file = Path("data/test/sensor_data.json")
        with open(sensor_file, 'w') as f:
            json.dump(sensor_data, f)
        
        # Test sensor data ingestion
        sensor_source = FileDataSource(str(sensor_file))
        sensor_path = ingestion_pipeline.ingest_data(
            data_source=sensor_source,
            source_type="sensors",
            metadata={"location": "test_facility", "sensor_type": "environmental"}
        )
        logger.info(f"Ingested sensor data to: {sensor_path}")
        
        # Cleanup test files
        eth_file.unlink()
        sol_file.unlink()
        model_file.unlink()
        sensor_file.unlink()
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing ingestion pipeline: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_ingestion_pipeline()
    if success:
        print("Ingestion pipeline test completed successfully!")
    else:
        print("Ingestion pipeline test failed. Check the logs for details.") 