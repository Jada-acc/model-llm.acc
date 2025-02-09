from typing import Dict, Any
import logging
from datetime import datetime
from sqlalchemy import text
from src.storage.storage_optimizer import StorageOptimizer

logger = logging.getLogger(__name__)

class DataLoader:
    """Load transformed data into storage."""
    
    def __init__(self, storage: StorageOptimizer):
        self.storage = storage
        
    def load_data(self, data: Dict[str, Any], data_type: str) -> bool:
        """Load data into appropriate storage."""
        try:
            # Add metadata
            data_with_metadata = self._add_metadata(data, data_type)
            
            # Store in database
            success = self._store_in_db(data_with_metadata)
            
            if success:
                logger.info(f"Successfully loaded {data_type} data")
                # Trigger storage optimization
                self.storage.compress_old_data(f"{data_type}_data")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return False
    
    def _add_metadata(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Add metadata to the data before storage."""
        return {
            'data': data,
            'metadata': {
                'data_type': data_type,
                'processed_at': datetime.now().isoformat(),
                'version': '1.0'
            }
        }
    
    def _store_in_db(self, data_with_metadata: Dict[str, Any]) -> bool:
        """Store data in database."""
        try:
            with self.storage.SessionFactory() as session:
                session.execute(
                    text("""
                        INSERT INTO processed_data 
                        (timestamp, data, metadata, version) 
                        VALUES (:timestamp, :data, :metadata, :version)
                    """),
                    {
                        'timestamp': datetime.now(),
                        'data': str(data_with_metadata['data']),
                        'metadata': str(data_with_metadata['metadata']),
                        'version': '1.0'
                    }
                )
                session.commit()
                return True
        except Exception as e:
            logger.error(f"Database storage error: {str(e)}")
            return False 