from sqlalchemy import create_engine, text
from typing import Dict, Any, List
import logging
from .base_connector import BaseConnector

logger = logging.getLogger(__name__)

class DatabaseConnector(BaseConnector):
    """Connector for SQL databases."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection_string = config['connection_string']
        self.engine = None
        self.batch_size = config.get('batch_size', 1000)
    
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.engine = create_engine(self.connection_string)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to database")
            return True
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """Close database connection."""
        try:
            if self.engine:
                self.engine.dispose()
                self.engine = None
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from database: {str(e)}")
            return False
    
    def fetch_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch data from database."""
        try:
            if not self.engine:
                raise ConnectionError("Not connected to database")
            
            sql = query.get('sql')
            params = query.get('params', {})
            
            if not sql:
                raise ValueError("SQL query is required")
            
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params)
                return [dict(zip(result.keys(), row)) for row in result]
                
        except Exception as e:
            logger.error(f"Error fetching database data: {str(e)}")
            raise
    
    def validate_connection(self) -> bool:
        """Validate database connection is active."""
        try:
            if not self.engine:
                return False
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False 