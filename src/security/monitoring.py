import logging
from datetime import datetime
from typing import Dict, Any
import json

class SecurityMonitor:
    """Monitor and log security events."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
    
    def setup_logging(self):
        """Configure security logging."""
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # File handler for security events
        fh = logging.FileHandler('security_events.log')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        
        # Add handlers
        self.logger.addHandler(fh)
    
    def log_security_event(self, event_type: str, details: Dict[str, Any]):
        """Log security-related events."""
        event = {
            'timestamp': datetime.now().isoformat(),
            'type': event_type,
            'details': details
        }
        self.logger.info(json.dumps(event)) 