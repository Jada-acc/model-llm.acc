import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
import json
import tempfile
import os

from cloud.cloud_manager import CloudManager

logger = logging.getLogger(__name__)

class StorageManager:
    """Manages hierarchical data storage structure."""
    
    def __init__(self, config: Dict[str, Any], cloud_manager: CloudManager):
        """Initialize storage manager.
        
        Args:
            config: Configuration dictionary
            cloud_manager: Initialized cloud manager instance
        """
        self.config = config
        self.cloud_manager = cloud_manager
        self.base_path = config.get('cloud_path', 's3://your-bucket/data')
        self.storage_structure = {
            'raw': {
                'blockchain': {},  # Blockchain data
                'ai_models': {},   # AI model outputs
                'sensors': {},     # IoT/sensor data
                'external': {}     # External data sources
            },
            'processed': {
                'features': {},    # Processed features
                'analytics': {},   # Analytics results
                'metrics': {}      # Performance metrics
            },
            'models': {
                'checkpoints': {}, # Model checkpoints
                'artifacts': {},   # Model artifacts
                'evaluations': {}  # Model evaluation results
            },
            'metadata': {
                'schemas': {},     # Data schemas
                'lineage': {},     # Data lineage
                'audit': {}        # Audit logs
            }
        }
        self._initialize_structure()
    
    def _initialize_structure(self):
        """Initialize the storage directory structure."""
        try:
            # Create a temporary .keep file
            temp_dir = tempfile.mkdtemp()
            keep_file = os.path.join(temp_dir, '.keep')
            with open(keep_file, 'w') as f:
                f.write('')
            
            for category in self.storage_structure:
                for subcategory in self.storage_structure[category]:
                    path = f"{self.base_path}/{category}/{subcategory}"
                    logger.info(f"Initializing directory: {path}")
                    # Create directory marker
                    self.cloud_manager.upload_file(
                        local_path=keep_file,
                        remote_path=f"{category}/{subcategory}/.keep",
                        bucket=self._get_bucket_name()
                    )
            
            # Clean up
            os.remove(keep_file)
            os.rmdir(temp_dir)
            logger.info("Storage structure initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing storage structure: {str(e)}")
            raise
    
    def _get_bucket_name(self) -> str:
        """Extract bucket name from base path."""
        return self.base_path.split('/')[2]
    
    def store_data(self, 
                   file_path: str, 
                   category: str,
                   subcategory: str,
                   metadata: Optional[Dict] = None) -> str:
        """Store data in the appropriate location with metadata.
        
        Args:
            file_path: Path to local data file
            category: Main category (raw/processed/models/metadata)
            subcategory: Subcategory within main category
            metadata: Optional metadata about the data
            
        Returns:
            Cloud storage path where data is stored
        """
        if category not in self.storage_structure:
            raise ValueError(f"Invalid category: {category}")
        if subcategory not in self.storage_structure[category]:
            raise ValueError(f"Invalid subcategory: {subcategory}")
        
        try:
            # Generate storage path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = Path(file_path).name
            storage_path = f"{category}/{subcategory}/{timestamp}_{filename}"
            
            # Upload data
            success = self.cloud_manager.upload_file(
                local_path=file_path,
                remote_path=storage_path,
                bucket=self._get_bucket_name()
            )
            
            if success and metadata:
                # Store metadata
                metadata_path = f"metadata/lineage/{timestamp}_{filename}.meta.json"
                self._store_metadata(metadata_path, metadata)
            
            return storage_path
            
        except Exception as e:
            logger.error(f"Error storing data: {str(e)}")
            raise
    
    def _store_metadata(self, path: str, metadata: Dict):
        """Store metadata for a data file."""
        try:
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
                json.dump(metadata, f, indent=2)
                temp_path = f.name
            
            self.cloud_manager.upload_file(
                local_path=temp_path,
                remote_path=path,
                bucket=self._get_bucket_name()
            )
            
            Path(temp_path).unlink()
            
        except Exception as e:
            logger.error(f"Error storing metadata: {str(e)}")
            raise
    
    def list_data(self, 
                  category: str, 
                  subcategory: Optional[str] = None) -> List[str]:
        """List data files in a category/subcategory.
        
        Args:
            category: Main category to list
            subcategory: Optional subcategory to filter
            
        Returns:
            List of file paths
        """
        if category not in self.storage_structure:
            raise ValueError(f"Invalid category: {category}")
            
        prefix = f"{category}/"
        if subcategory:
            if subcategory not in self.storage_structure[category]:
                raise ValueError(f"Invalid subcategory: {subcategory}")
            prefix = f"{category}/{subcategory}/"
            
        return self.cloud_manager.list_files(
            prefix=prefix,
            bucket=self._get_bucket_name()
        )
    
    def get_data(self, 
                 storage_path: str, 
                 local_path: Optional[str] = None) -> str:
        """Retrieve data from storage.
        
        Args:
            storage_path: Path in cloud storage
            local_path: Optional local path to save file
            
        Returns:
            Path to local file
        """
        if not local_path:
            local_path = f"data/temp/{Path(storage_path).name}"
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.cloud_manager.download_file(
            remote_path=storage_path,
            local_path=local_path,
            bucket=self._get_bucket_name()
        )
        
        return local_path
    
    def get_metadata(self, storage_path: str) -> Optional[Dict]:
        """Get metadata for a data file."""
        try:
            filename = Path(storage_path).name
            metadata_path = f"metadata/lineage/{filename}.meta.json"
            local_path = f"data/temp/{Path(metadata_path).name}"
            
            self.cloud_manager.download_file(
                remote_path=metadata_path,
                local_path=local_path,
                bucket=self._get_bucket_name()
            )
            
            with open(local_path, 'r') as f:
                metadata = json.load(f)
            
            Path(local_path).unlink()
            return metadata
            
        except Exception as e:
            logger.error(f"Error retrieving metadata: {str(e)}")
            return None 