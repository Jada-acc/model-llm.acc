import os
from typing import Dict, Any
import yaml
import secrets

class SecurityConfig:
    """Manage security configuration settings."""
    
    def __init__(self, config_path: str = "config/security.yml"):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load security configuration from YAML."""
        if not os.path.exists(self.config_path):
            self._generate_default_config()
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _generate_default_config(self):
        """Generate default security configuration."""
        default_config = {
            'jwt': {
                'secret_key': secrets.token_hex(32),
                'token_expiry': 3600,  # 1 hour
                'algorithm': 'HS256'
            },
            'encryption': {
                'key_rotation_interval': 86400,  # 24 hours
                'min_key_length': 32
            },
            'auth': {
                'max_failed_attempts': 3,
                'lockout_duration': 300  # 5 minutes
            }
        }
        
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w') as f:
            yaml.dump(default_config, f) 