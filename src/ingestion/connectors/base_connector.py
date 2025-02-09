from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """Base class for all data connectors."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.retry_count = config.get('retry_count', 3)
        self.retry_delay = config.get('retry_delay', 5)
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """Close connection to the data source."""
        pass
    
    @abstractmethod
    def fetch_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch data from the source."""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate connection is active and healthy."""
        pass
    
    def health_check(self) -> Dict[str, Any]:
        """Check connector health status."""
        try:
            is_connected = self.validate_connection()
            return {
                'status': 'healthy' if is_connected else 'unhealthy',
                'connection': 'active' if is_connected else 'inactive',
                'last_check': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'last_check': datetime.now().isoformat()
            } 