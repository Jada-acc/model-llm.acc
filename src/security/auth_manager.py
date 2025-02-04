from typing import Optional, Dict
import jwt
from datetime import datetime, timedelta
import logging
import hashlib
import secrets

logger = logging.getLogger(__name__)

class AuthManager:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
        self._user_tokens = {}  # Store active tokens
    
    def generate_token(self, user_id: str, expires_in: int = 3600) -> Optional[str]:
        """Generate JWT token for API access."""
        try:
            payload = {
                'user_id': user_id,
                'exp': datetime.utcnow() + timedelta(seconds=expires_in),
                'iat': datetime.utcnow(),
                'jti': secrets.token_hex(16)  # Unique token ID
            }
            token = jwt.encode(payload, self.secret_key, algorithm='HS256')
            self._user_tokens[user_id] = token
            return token
        except Exception as e:
            logger.error(f"Error generating token: {str(e)}")
            return None
    
    def validate_token(self, token: str) -> Optional[Dict]:
        """Validate JWT token and return payload if valid."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            user_id = payload.get('user_id')
            
            # Check if token is still active
            if self._user_tokens.get(user_id) != token:
                raise jwt.InvalidTokenError("Token has been revoked")
                
            return payload
        except jwt.ExpiredSignatureError:
            logger.error("Token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.error(f"Invalid token: {str(e)}")
            return None
    
    def revoke_token(self, user_id: str) -> bool:
        """Revoke user's active token."""
        try:
            self._user_tokens.pop(user_id, None)
            return True
        except Exception as e:
            logger.error(f"Error revoking token: {str(e)}")
            return False
    
    def hash_password(self, password: str) -> str:
        """Hash password using SHA-256."""
        return hashlib.sha256(password.encode()).hexdigest() 