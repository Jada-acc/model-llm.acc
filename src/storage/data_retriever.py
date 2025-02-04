from typing import Dict, Any, Optional
import logging
from datetime import datetime
from sqlalchemy import text
from src.security.encryption import DataEncryption
from src.security.auth_manager import AuthManager

logger = logging.getLogger(__name__)

class DataRetriever:
    """Handle secure data retrieval and decryption."""
    
    def __init__(self, storage_optimizer, encryption: DataEncryption, auth_manager: AuthManager):
        self.storage = storage_optimizer
        self.encryption = encryption
        self.auth_manager = auth_manager
    
    def get_processed_data(self, start_time: datetime, end_time: datetime, auth_token: str) -> Optional[Dict[str, Any]]:
        """Retrieve and decrypt processed data."""
        try:
            # Validate authentication
            if not self.auth_manager.validate_token(auth_token):
                raise PermissionError("Invalid authentication token")
            
            logger.debug(f"Querying data between {start_time} and {end_time}")
            
            # Get encrypted data from storage
            with self.storage.SessionFactory() as session:
                # First, check what data exists
                all_data = session.execute(text("""
                    SELECT timestamp, encrypted_data 
                    FROM processed_data 
                    ORDER BY timestamp DESC
                """)).fetchall()
                
                logger.debug(f"Found {len(all_data)} total records")
                for record in all_data:
                    logger.debug(f"Record timestamp: {record.timestamp}")
                
                # Get data for our time range
                result = session.execute(text("""
                    SELECT encrypted_data, timestamp
                    FROM processed_data
                    WHERE timestamp BETWEEN :start AND :end
                    ORDER BY timestamp DESC
                    LIMIT 1
                """), {
                    "start": start_time,
                    "end": end_time
                }).first()
                
                if not result:
                    logger.warning(f"No data found between {start_time} and {end_time}")
                    return None
                
                if not result.encrypted_data:
                    logger.warning(f"Found record at {result.timestamp} but no encrypted data")
                    return None
                
                logger.debug(f"Found data from {result.timestamp}")
                
                # Decrypt data
                decrypted_data = self.encryption.decrypt_data(result.encrypted_data)
                if decrypted_data:
                    logger.info(f"Successfully retrieved and decrypted data from {result.timestamp}")
                    logger.debug(f"Decrypted data: {decrypted_data}")
                else:
                    logger.error("Failed to decrypt data")
                return decrypted_data
                
        except Exception as e:
            logger.error(f"Error retrieving data: {str(e)}")
            logger.exception("Detailed error:")
            return None 