import logging
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod

from .storage_manager import StorageManager

logger = logging.getLogger(__name__)

class DataSource(ABC):
    """Abstract base class for data sources."""
    
    @abstractmethod
    def read_data(self) -> Union[pd.DataFrame, Dict, List]:
        """Read data from the source."""
        pass
    
    @abstractmethod
    def validate_data(self, data: Union[pd.DataFrame, Dict, List]) -> bool:
        """Validate the data format and content."""
        pass

class FileDataSource(DataSource):
    """Handles data from file sources (CSV, JSON, etc.)."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.supported_formats = {
            '.csv': self._read_csv,
            '.json': self._read_json,
            '.parquet': self._read_parquet
        }
    
    def read_data(self) -> Union[pd.DataFrame, Dict, List]:
        """Read data from file."""
        file_ext = Path(self.file_path).suffix.lower()
        if file_ext not in self.supported_formats:
            raise ValueError(f"Unsupported file format: {file_ext}")
        
        return self.supported_formats[file_ext]()
    
    def _read_csv(self) -> pd.DataFrame:
        """Read CSV file."""
        return pd.read_csv(self.file_path)
    
    def _read_json(self) -> Union[Dict, List]:
        """Read JSON file."""
        with open(self.file_path, 'r') as f:
            return json.load(f)
    
    def _read_parquet(self) -> pd.DataFrame:
        """Read Parquet file."""
        return pd.read_parquet(self.file_path)
    
    def validate_data(self, data: Union[pd.DataFrame, Dict, List]) -> bool:
        """Validate file data."""
        try:
            if isinstance(data, pd.DataFrame):
                return not data.empty
            elif isinstance(data, (dict, list)):
                return bool(data)
            return False
        except Exception as e:
            logger.error(f"Data validation error: {str(e)}")
            return False

class APIDataSource(DataSource):
    """Handles data from API endpoints."""
    
    def __init__(self, endpoint: str, headers: Optional[Dict] = None):
        self.endpoint = endpoint
        self.headers = headers or {}
    
    def read_data(self) -> Union[Dict, List]:
        """Read data from API endpoint."""
        import requests
        
        try:
            response = requests.get(self.endpoint, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error reading from API: {str(e)}")
            raise
    
    def validate_data(self, data: Union[Dict, List]) -> bool:
        """Validate API response data."""
        return bool(data)

class IngestionPipeline:
    """Manages data ingestion from various sources."""
    
    def __init__(self, storage_manager: StorageManager):
        """Initialize ingestion pipeline.
        
        Args:
            storage_manager: Initialized storage manager instance
        """
        self.storage_manager = storage_manager
        self.validators = {
            'blockchain': self._validate_blockchain_data,
            'ai_models': self._validate_ai_model_data,
            'sensors': self._validate_sensor_data,
            'external': self._validate_external_data
        }
    
    def ingest_data(self, 
                    data_source: DataSource,
                    source_type: str,
                    metadata: Optional[Dict] = None) -> str:
        """Ingest data from a source.
        
        Args:
            data_source: Data source instance
            source_type: Type of data (blockchain/ai_models/sensors/external)
            metadata: Optional metadata about the data
            
        Returns:
            Storage path where data is stored
        """
        try:
            # Read data from source
            data = data_source.read_data()
            
            # Validate data
            if not data_source.validate_data(data):
                raise ValueError("Invalid data format or empty data")
            
            if source_type in self.validators:
                if not self.validators[source_type](data):
                    raise ValueError(f"Data validation failed for {source_type}")
            
            # Convert data to JSON if it's a DataFrame
            if isinstance(data, pd.DataFrame):
                temp_file = Path('data/temp/temp_data.json')
                temp_file.parent.mkdir(parents=True, exist_ok=True)
                data.to_json(temp_file, orient='records')
                file_path = str(temp_file)
            else:
                # Save data to temporary file
                temp_file = Path('data/temp/temp_data.json')
                temp_file.parent.mkdir(parents=True, exist_ok=True)
                with open(temp_file, 'w') as f:
                    json.dump(data, f)
                file_path = str(temp_file)
            
            # Store data
            storage_path = self.storage_manager.store_data(
                file_path=file_path,
                category='raw',
                subcategory=source_type,
                metadata=metadata
            )
            
            # Cleanup
            temp_file.unlink()
            
            return storage_path
            
        except Exception as e:
            logger.error(f"Error ingesting data: {str(e)}")
            raise
    
    def _validate_blockchain_data(self, data: Union[Dict, List]) -> bool:
        """Validate blockchain data format for both Ethereum and Solana."""
        try:
            if isinstance(data, dict):
                # Check for Ethereum format
                if all(field in data for field in {'block_height', 'transactions'}):
                    return True
                    
                # Check for Solana format
                if all(field in data for field in {'slot', 'blockhash', 'transactions'}):
                    return True
                    
                return False
                
            elif isinstance(data, list):
                # Check if all items are valid blockchain data
                return all(
                    isinstance(item, dict) and (
                        # Ethereum format
                        all(field in item for field in ['block_height', 'transactions']) or
                        # Solana format
                        all(field in item for field in ['slot', 'blockhash', 'transactions'])
                    )
                    for item in data
                )
            return False
        except Exception:
            return False
    
    def _validate_ai_model_data(self, data: Union[Dict, List]) -> bool:
        """Validate AI model data format."""
        try:
            if isinstance(data, dict):
                required_fields = {'model', 'predictions'}
                return all(field in data for field in required_fields)
            elif isinstance(data, list):
                return all(isinstance(item, dict) and 
                         'model' in item and 
                         'predictions' in item 
                         for item in data)
            return False
        except Exception:
            return False
    
    def _validate_sensor_data(self, data: Union[Dict, List]) -> bool:
        """Validate sensor data format."""
        try:
            if isinstance(data, dict):
                required_fields = {'sensor_id', 'timestamp', 'readings'}
                return all(field in data for field in required_fields)
            elif isinstance(data, list):
                return all(isinstance(item, dict) and 
                         all(field in item for field in ['sensor_id', 'timestamp', 'readings'])
                         for item in data)
            return False
        except Exception:
            return False
    
    def _validate_external_data(self, data: Union[Dict, List]) -> bool:
        """Validate external data format."""
        # Basic validation for external data
        return bool(data) 