from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from src.storage.storage_optimizer import StorageOptimizer
from src.blockchain.blockchain_data import BlockchainDataSource
from src.security.encryption import DataEncryption
from src.security.auth_manager import AuthManager

logger = logging.getLogger(__name__)

class DataPipeline:
    """Manages data flow from blockchain sources to storage."""
    
    def __init__(self, storage_optimizer: StorageOptimizer, auth_manager: AuthManager, encryption: DataEncryption):
        self.storage = storage_optimizer
        self.data_source = BlockchainDataSource()
        self.processing_steps = []
        self.auth_manager = auth_manager
        self.encryption = encryption
    
    def add_processing_step(self, step_func: callable, requires_auth: bool = False):
        """Add a processing step to the pipeline."""
        self.processing_steps.append({
            'func': step_func,
            'requires_auth': requires_auth
        })
        logger.info(f"Added processing step: {step_func.__name__}")
    
    def process_data(self, data: Dict[str, Any], auth_token: Optional[str] = None) -> Dict[str, Any]:
        """Run data through all processing steps with authentication."""
        processed_data = data
        
        for step in self.processing_steps:
            try:
                # Check authentication if required
                if step['requires_auth']:
                    if not auth_token or not self.auth_manager.validate_token(auth_token):
                        raise PermissionError("Authentication required for this processing step")
                
                processed_data = step['func'](processed_data)
                logger.debug(f"Completed step: {step['func'].__name__}")
            except Exception as e:
                logger.error(f"Error in processing step {step['func'].__name__}: {str(e)}")
                raise
                
        return processed_data
    
    def run_pipeline(self, start_time: datetime, end_time: datetime, auth_token: Optional[str] = None, test_data: Optional[Dict] = None) -> bool:
        """Execute the complete data pipeline with security."""
        try:
            # 1. Data Collection
            raw_data = test_data if test_data else self.data_source.get_blockchain_data(start_time, end_time)
            
            # 2. Data Processing with authentication
            processed_data = self.process_data(raw_data, auth_token)
            
            # Add timestamps to processed data
            processed_data.update({
                'start_time': start_time,
                'end_time': end_time,
                'processing_time': datetime.now()
            })
            
            # 3. Encrypt sensitive data
            encrypted_data = self.encryption.encrypt_data(processed_data)
            if not encrypted_data:
                raise ValueError("Failed to encrypt data")
            
            # 4. Store encrypted data with consistent timestamp
            storage_data = {
                'timestamp': start_time,  # Use start_time for consistency
                'data': str(processed_data),
                'encrypted_data': encrypted_data
            }
            
            logger.debug(f"Storing data with timestamp: {storage_data['timestamp']}")
            if not self.storage.store_processed_data(storage_data):
                raise ValueError("Failed to store data")
            
            # 5. Optimize storage (compress old data)
            self.storage.compress_old_data("processed_data")
            
            # 6. Cache frequently accessed data (encrypted)
            if self.should_cache(processed_data):
                self.storage.cache_frequent_data(
                    f"processed_data_{end_time.isoformat()}",
                    encrypted_data
                )
            
            logger.info("Pipeline execution completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            return False
    
    def should_cache(self, data: Dict[str, Any]) -> bool:
        """Determine if data should be cached based on access patterns."""
        # Implement caching decision logic
        return True  # Placeholder