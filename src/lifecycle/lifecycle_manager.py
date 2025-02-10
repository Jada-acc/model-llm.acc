from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta
import os
import json
import shutil
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class DataLifecycleManager:
    """Manage data lifecycle including retention, archival and cleanup."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize lifecycle manager with configuration."""
        self.config = config
        self.data_dir = config.get('data_dir', 'data')
        self.archive_dir = config.get('archive_dir', 'archive')
        
        # Lifecycle policies
        self.policies = config.get('policies', {
            'hot_storage': {
                'retention_days': 30,
                'storage_class': 'local'
            },
            'warm_storage': {
                'retention_days': 90,
                'storage_class': 'archive'
            },
            'cold_storage': {
                'retention_days': 365,
                'storage_class': 's3_glacier'
            }
        })
        
        # Cloud storage settings
        self.use_cloud = config.get('use_cloud', False)
        if self.use_cloud:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=config.get('aws_access_key_id'),
                aws_secret_access_key=config.get('aws_secret_access_key'),
                region_name=config.get('aws_region')
            )
            self.bucket_name = config.get('bucket_name')
        
        # Create necessary directories
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)
    
    def apply_lifecycle_policies(self) -> bool:
        """Apply configured lifecycle policies to data."""
        try:
            # Get all data items with their metadata
            data_items = self._scan_data_directory()
            
            for item in data_items:
                age_days = self._get_age_days(item['created_at'])
                
                # Determine appropriate policy
                policy = self._get_applicable_policy(age_days)
                if policy:
                    self._apply_policy(item, policy)
            
            return True
            
        except Exception as e:
            logger.error(f"Error applying lifecycle policies: {e}")
            return False
    
    def _scan_data_directory(self) -> List[Dict[str, Any]]:
        """Scan data directory and collect metadata."""
        items = []
        try:
            for root, _, files in os.walk(self.data_dir):
                for file in files:
                    if file.endswith('.metadata'):
                        continue
                        
                    file_path = os.path.join(root, file)
                    metadata = self._get_metadata(file_path)
                    
                    items.append({
                        'path': file_path,
                        'name': file,
                        'size': os.path.getsize(file_path),
                        'created_at': metadata.get('created_at', 
                            datetime.fromtimestamp(os.path.getctime(file_path)).isoformat()
                        ),
                        'last_accessed': metadata.get('last_accessed',
                            datetime.fromtimestamp(os.path.getatime(file_path)).isoformat()
                        ),
                        'storage_class': metadata.get('storage_class', 'local'),
                        'metadata': metadata
                    })
            
            return items
            
        except Exception as e:
            logger.error(f"Error scanning data directory: {e}")
            return []
    
    def _get_metadata(self, file_path: str) -> Dict[str, Any]:
        """Get metadata for a data file."""
        try:
            metadata_path = f"{file_path}.metadata"
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    return json.load(f)
            return {}
            
        except Exception as e:
            logger.error(f"Error reading metadata for {file_path}: {e}")
            return {}
    
    def _update_metadata(self, file_path: str, metadata: Dict[str, Any]) -> bool:
        """Update metadata for a data file."""
        try:
            metadata_path = f"{file_path}.metadata"
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)
            return True
            
        except Exception as e:
            logger.error(f"Error updating metadata for {file_path}: {e}")
            return False
    
    def _get_age_days(self, timestamp: str) -> int:
        """Calculate age in days from ISO timestamp."""
        try:
            created_date = datetime.fromisoformat(timestamp)
            age = datetime.now() - created_date
            return age.days
        except Exception:
            return 0
    
    def _get_applicable_policy(self, age_days: int) -> Optional[Dict[str, Any]]:
        """Get the applicable policy based on data age."""
        applicable_policy = None
        max_retention = 0
        
        for policy_name, policy in self.policies.items():
            retention_days = policy['retention_days']
            if age_days > retention_days and retention_days > max_retention:
                applicable_policy = policy
                max_retention = retention_days
        
        return applicable_policy
    
    def _apply_policy(self, item: Dict[str, Any], policy: Dict[str, Any]) -> bool:
        """Apply lifecycle policy to a data item."""
        try:
            storage_class = policy['storage_class']
            
            if storage_class == item['storage_class']:
                return True
            
            if storage_class == 'archive':
                return self._move_to_archive(item)
            elif storage_class == 's3_glacier':
                return self._move_to_glacier(item)
            elif storage_class == 'delete':
                return self._delete_data(item)
            
            return False
            
        except Exception as e:
            logger.error(f"Error applying policy to {item['path']}: {e}")
            return False
    
    def _move_to_archive(self, item: Dict[str, Any]) -> bool:
        """Move data to archive storage."""
        try:
            archive_path = os.path.join(
                self.archive_dir,
                os.path.relpath(item['path'], self.data_dir)
            )
            os.makedirs(os.path.dirname(archive_path), exist_ok=True)
            
            # Move file and metadata
            shutil.move(item['path'], archive_path)
            if os.path.exists(f"{item['path']}.metadata"):
                shutil.move(
                    f"{item['path']}.metadata",
                    f"{archive_path}.metadata"
                )
            
            # Update metadata
            metadata = item['metadata']
            metadata['storage_class'] = 'archive'
            metadata['archived_at'] = datetime.now().isoformat()
            self._update_metadata(archive_path, metadata)
            
            logger.info(f"Moved {item['path']} to archive")
            return True
            
        except Exception as e:
            logger.error(f"Error moving to archive: {e}")
            return False
    
    def _move_to_glacier(self, item: Dict[str, Any]) -> bool:
        """Move data to Glacier storage."""
        if not self.use_cloud:
            logger.error("Cloud storage not configured")
            return False
            
        try:
            # Upload to S3 Glacier
            with open(item['path'], 'rb') as f:
                self.s3_client.upload_fileobj(
                    f,
                    self.bucket_name,
                    f"glacier/{item['name']}",
                    ExtraArgs={'StorageClass': 'GLACIER'}
                )
            
            # Update metadata and move to archive
            metadata = item['metadata']
            metadata['storage_class'] = 's3_glacier'
            metadata['glacier_upload_date'] = datetime.now().isoformat()
            
            # Move to archive after successful upload
            return self._move_to_archive(item)
            
        except Exception as e:
            logger.error(f"Error moving to Glacier: {e}")
            return False
    
    def _delete_data(self, item: Dict[str, Any]) -> bool:
        """Delete data and its metadata."""
        try:
            os.remove(item['path'])
            if os.path.exists(f"{item['path']}.metadata"):
                os.remove(f"{item['path']}.metadata")
            
            logger.info(f"Deleted {item['path']}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting data: {e}")
            return False 