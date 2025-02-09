from typing import Dict, Any, List
import logging
from datetime import datetime
from abc import ABC, abstractmethod
from src.pipeline.data_validator import DataValidator

logger = logging.getLogger(__name__)

class DataSource(ABC):
    """Abstract base class for data sources."""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    def fetch_data(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Fetch data from the source."""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate source connection is active."""
        pass

class DataIngestionManager:
    """Manage data ingestion from multiple sources."""
    
    def __init__(self):
        self.sources: Dict[str, DataSource] = {}
        self.validator = DataValidator()
        self.retry_limit = 3
        
    def register_source(self, source_name: str, source: DataSource) -> bool:
        """Register a new data source."""
        try:
            if source.connect():
                self.sources[source_name] = source
                logger.info(f"Successfully registered source: {source_name}")
                return True
            else:
                logger.error(f"Failed to connect to source: {source_name}")
                return False
        except Exception as e:
            logger.error(f"Error registering source {source_name}: {str(e)}")
            return False
    
    def ingest_data(self, source_name: str, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Ingest data from a specific source with retry logic."""
        if source_name not in self.sources:
            raise ValueError(f"Source {source_name} not registered")
            
        source = self.sources[source_name]
        attempts = 0
        
        while attempts < self.retry_limit:
            try:
                # Validate connection before fetching
                if not source.validate_connection():
                    source.connect()
                
                # Fetch data
                data = source.fetch_data(start_time, end_time)
                
                # Validate data structure
                if self.validator.validate_data(data, 'blockchain_data'):
                    logger.info(f"Successfully ingested data from {source_name}")
                    return data
                else:
                    logger.error(f"Data validation failed for {source_name}")
                    
            except Exception as e:
                logger.error(f"Attempt {attempts + 1} failed: {str(e)}")
                attempts += 1
                
        raise RuntimeError(f"Failed to ingest data from {source_name} after {self.retry_limit} attempts") 