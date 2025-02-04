from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
from src.pipeline.pipeline_manager import DataPipeline
from src.security.encryption import DataEncryption
from src.security.auth_manager import AuthManager
from src.config.security_config import SecurityConfig

logger = logging.getLogger(__name__)

class SecurePipeline(DataPipeline):
    """Enhanced pipeline with additional security features."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.security_config = SecurityConfig()
        self.failed_attempts = {}
    
    def _check_rate_limit(self, user_id: str) -> bool:
        """Check if user has exceeded failed attempt limit."""
        if user_id in self.failed_attempts:
            attempts = self.failed_attempts[user_id]
            if attempts >= self.security_config.config['auth']['max_failed_attempts']:
                return False
        return True
    
    def run_pipeline(self, start_time: datetime, end_time: datetime, auth_token: Optional[str] = None, test_data: Optional[Dict] = None) -> bool:
        """Execute pipeline with enhanced security checks."""
        try:
            # Validate token and get user
            payload = self.auth_manager.validate_token(auth_token)
            if not payload:
                raise PermissionError("Invalid authentication token")
                
            user_id = payload['user_id']
            
            # Check rate limiting
            if not self._check_rate_limit(user_id):
                raise PermissionError("Too many failed attempts")
            
            # Run standard pipeline with test data
            success = super().run_pipeline(start_time, end_time, auth_token, test_data)
            
            if not success:
                # Track failed attempts
                self.failed_attempts[user_id] = self.failed_attempts.get(user_id, 0) + 1
            else:
                # Clear failed attempts on success
                self.failed_attempts.pop(user_id, None)
                
            return success
            
        except Exception as e:
            logger.error(f"Secure pipeline execution failed: {str(e)}")
            return False 