from cryptography.fernet import Fernet
import base64
import logging
from typing import Optional, Any
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class DataEncryption:
    """Handle data encryption and decryption."""
    
    def __init__(self, encryption_key: Optional[str] = None):
        """Initialize encryption with optional key or generate new one."""
        self.key = encryption_key or Fernet.generate_key()
        self.cipher_suite = Fernet(self.key)
    
    def _serialize_datetime(self, obj):
        """Handle datetime serialization."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    def encrypt_data(self, data: Any) -> Optional[str]:
        """Encrypt data and return base64 encoded string."""
        try:
            # Convert data to JSON string with datetime handling
            data_str = json.dumps(data, default=self._serialize_datetime)
            # Encrypt
            encrypted_data = self.cipher_suite.encrypt(data_str.encode())
            # Encode as base64
            return base64.b64encode(encrypted_data).decode()
        except Exception as e:
            logger.error(f"Error encrypting data: {str(e)}")
            return None
    
    def decrypt_data(self, encrypted_data: str) -> Optional[Any]:
        """Decrypt base64 encoded encrypted data."""
        try:
            # Decode base64
            encrypted_bytes = base64.b64decode(encrypted_data)
            # Decrypt
            decrypted_data = self.cipher_suite.decrypt(encrypted_bytes)
            # Parse JSON
            return json.loads(decrypted_data.decode())
        except Exception as e:
            logger.error(f"Error decrypting data: {str(e)}")
            return None
    
    def rotate_key(self) -> bool:
        """Generate new encryption key and re-encrypt data if needed."""
        try:
            new_key = Fernet.generate_key()
            new_cipher = Fernet(new_key)
            self.key = new_key
            self.cipher_suite = new_cipher
            return True
        except Exception as e:
            logger.error(f"Error rotating encryption key: {str(e)}")
            return False 