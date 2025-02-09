from typing import Dict, Any, Optional
import logging
import jwt
import hashlib
import secrets
from datetime import datetime, timedelta
from functools import wraps
import redis

logger = logging.getLogger(__name__)

class AuthManager:
    """Handle authentication and authorization for LLM infrastructure."""
    
    def __init__(self, config: Dict[str, Any], redis_client: Optional[redis.Redis] = None):
        self.config = config
        self.redis_client = redis_client
        self.secret_key = config.get('secret_key', secrets.token_hex(32))
        self.token_expiry = config.get('token_expiry', 3600)  # 1 hour default
        
        # Role-based access control
        self.roles = {
            'admin': {'can_train', 'can_deploy', 'can_monitor', 'can_manage_users'},
            'engineer': {'can_train', 'can_monitor'},
            'analyst': {'can_monitor'},
            'user': {'can_use_models'}
        }
    
    def authenticate(self, username: str, password: str) -> Optional[str]:
        """Authenticate user and return JWT token."""
        try:
            # Hash password
            password_hash = self._hash_password(password)
            
            # Verify credentials (implement your user storage)
            user = self._verify_credentials(username, password_hash)
            if not user:
                logger.warning(f"Authentication failed for user: {username}")
                return None
            
            # Generate JWT token
            token = self._generate_token(user)
            
            # Store in Redis if available
            if self.redis_client:
                self.redis_client.setex(
                    f"token:{token}",
                    self.token_expiry,
                    username
                )
            
            logger.info(f"Successfully authenticated user: {username}")
            return token
            
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return None
    
    def authorize(self, token: str, required_permission: str) -> bool:
        """Check if token has required permission."""
        try:
            # Verify token
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            
            # Check if token is blacklisted
            if self.redis_client and self.redis_client.get(f"blacklist:{token}"):
                logger.warning("Token is blacklisted")
                return False
            
            # Check permissions
            user_role = payload.get('role', 'user')
            if user_role not in self.roles:
                logger.warning(f"Invalid role: {user_role}")
                return False
            
            has_permission = required_permission in self.roles[user_role]
            if not has_permission:
                logger.warning(
                    f"User {payload['sub']} with role {user_role} "
                    f"denied access to {required_permission}"
                )
            
            return has_permission
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired")
            return False
        except jwt.InvalidTokenError:
            logger.warning("Invalid token")
            return False
        except Exception as e:
            logger.error(f"Authorization error: {str(e)}")
            return False
    
    def require_permission(self, permission: str):
        """Decorator for requiring specific permissions."""
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                token = kwargs.get('token') or getattr(args[0], 'token', None)
                if not token:
                    raise ValueError("No token provided")
                
                if not self.authorize(token, permission):
                    raise PermissionError(f"Missing required permission: {permission}")
                
                return f(*args, **kwargs)
            return decorated_function
        return decorator
    
    def revoke_token(self, token: str) -> bool:
        """Revoke a token."""
        try:
            if self.redis_client:
                # Add to blacklist with same expiry as original token
                payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
                exp = datetime.fromtimestamp(payload['exp'])
                ttl = (exp - datetime.now()).total_seconds()
                
                if ttl > 0:
                    self.redis_client.setex(f"blacklist:{token}", int(ttl), '1')
                    logger.info(f"Token revoked for user: {payload['sub']}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error revoking token: {str(e)}")
            return False
    
    def _generate_token(self, user: Dict[str, Any]) -> str:
        """Generate JWT token for user."""
        now = datetime.utcnow()
        payload = {
            'sub': user['username'],
            'role': user['role'],
            'iat': now,
            'exp': now + timedelta(seconds=self.token_expiry)
        }
        return jwt.encode(payload, self.secret_key, algorithm='HS256')
    
    def _hash_password(self, password: str) -> str:
        """Hash password using SHA-256."""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def _verify_credentials(self, username: str, password_hash: str) -> Optional[Dict[str, Any]]:
        """Verify user credentials (implement your user storage)."""
        # TODO: Implement actual user storage
        # This is a mock implementation
        mock_users = {
            'admin': {
                'username': 'admin',
                'password_hash': self._hash_password('admin_password'),
                'role': 'admin'
            }
        }
        
        user = mock_users.get(username)
        if user and user['password_hash'] == password_hash:
            return user
        return None 