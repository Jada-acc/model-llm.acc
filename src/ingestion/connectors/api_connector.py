import requests
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from .base_connector import BaseConnector
import json
import backoff

logger = logging.getLogger(__name__)

class APIConnector(BaseConnector):
    """Generic connector for REST APIs."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config['base_url']
        self.headers = config.get('headers', {})
        self.auth = config.get('auth')
        self.session = None
        self.timeout = config.get('timeout', 30)
        self.verify_ssl = config.get('verify_ssl', True)
    
    def connect(self) -> bool:
        """Establish connection session."""
        try:
            self.session = requests.Session()
            self.session.headers.update(self.headers)
            if self.auth:
                self.session.auth = tuple(self.auth)
            
            # Test connection
            response = self.session.get(
                self.base_url,
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            response.raise_for_status()
            
            logger.info("Successfully established API connection")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to API: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """Close connection session."""
        try:
            if self.session:
                self.session.close()
                self.session = None
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from API: {str(e)}")
            return False
    
    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=3)
    def fetch_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch data from API endpoint."""
        try:
            if not self.session:
                raise ConnectionError("Not connected to API")
            
            # Parse query parameters
            endpoint = query.get('endpoint', '')
            method = query.get('method', 'GET')
            params = query.get('params', {})
            data = query.get('data')
            
            url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
            
            # Make request
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data if data else None,
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            response.raise_for_status()
            
            # Parse response
            result = response.json()
            if isinstance(result, list):
                return result
            elif isinstance(result, dict):
                # Handle paginated results
                if 'data' in result:
                    return result['data']
                return [result]
            else:
                raise ValueError(f"Unexpected response format: {type(result)}")
            
        except Exception as e:
            logger.error(f"Error fetching API data: {str(e)}")
            raise
    
    def validate_connection(self) -> bool:
        """Validate API connection is active."""
        try:
            if not self.session:
                return False
            
            response = self.session.get(
                self.base_url,
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            return response.status_code == 200
            
        except Exception:
            return False 