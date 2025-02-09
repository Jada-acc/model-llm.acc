from sqlalchemy import create_engine, text
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from .base_connector import BaseConnector
import pandas as pd

logger = logging.getLogger(__name__)

class DatabaseConnector(BaseConnector):
    """Generic connector for SQL databases."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection_string = config['connection_string']
        self.engine = None
        self.batch_size = config.get('batch_size', 1000)
        self.schema = config.get('schema')
    
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
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
            
            # Parse query parameters
            sql = query.get('sql')
            params = query.get('params', {})
            chunk_size = query.get('chunk_size', self.batch_size)
            
            if not sql:
                raise ValueError("SQL query is required")
            
            # Handle large datasets with chunking
            if query.get('use_chunks', False):
                return self._fetch_in_chunks(sql, params, chunk_size)
            
            # Regular query execution
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params)
                return [dict(row) for row in result]
            
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
    
    def _fetch_in_chunks(self, sql: str, params: Dict[str, Any], chunk_size: int) -> List[Dict[str, Any]]:
        """Fetch data in chunks for memory efficiency."""
        try:
            chunks = []
            for chunk_df in pd.read_sql(
                sql,
                self.engine,
                params=params,
                chunksize=chunk_size
            ):
                chunks.extend(chunk_df.to_dict('records'))
            return chunks
            
        except Exception as e:
            logger.error(f"Error fetching data in chunks: {str(e)}")
            raise 