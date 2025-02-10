from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import os
import shutil
import json
import zlib
import boto3
from botocore.exceptions import ClientError
import threading
from queue import Queue

logger = logging.getLogger(__name__)

class BackupManager:
    """Manage data backup and recovery operations."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.backup_dir = config.get('backup_dir', 'backups')
        self.retention_days = config.get('retention_days', 30)
        self.use_cloud = config.get('use_cloud', False)
        self.cloud_config = config.get('cloud_config', {})
        self.s3_bucket = config.get('s3_bucket')
        self.backup_queue = Queue()
        self.stop_event = threading.Event()
        
        # Initialize backup directory
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Initialize S3 client if configured
        self.s3_client = None
        if self.s3_bucket:
            self.s3_client = boto3.client('s3')
        
        # Initialize cloud client if needed
        self.cloud_client = None
        if self.use_cloud:
            self._init_cloud_client()
        
        # Start backup worker thread
        self.backup_thread = threading.Thread(target=self._backup_worker)
        self.backup_thread.daemon = True
        self.backup_thread.start()
    
    def _init_cloud_client(self):
        """Initialize cloud storage client."""
        try:
            self.cloud_client = boto3.client(
                's3',
                aws_access_key_id=self.cloud_config.get('aws_access_key_id'),
                aws_secret_access_key=self.cloud_config.get('aws_secret_access_key'),
                region_name=self.cloud_config.get('region_name')
            )
            logger.info("Successfully initialized cloud client")
        except Exception as e:
            logger.error(f"Failed to initialize cloud client: {e}")
            self.use_cloud = False
    
    def schedule_backup(self, data: Dict[str, Any], backup_type: str) -> bool:
        """Schedule a backup operation."""
        try:
            backup_job = {
                'data': data,
                'type': backup_type,
                'timestamp': datetime.now().isoformat()
            }
            self.backup_queue.put(backup_job)
            logger.info(f"Scheduled {backup_type} backup")
            return True
        except Exception as e:
            logger.error(f"Error scheduling backup: {str(e)}")
            return False
    
    def restore_from_backup(self, backup_id: str) -> Optional[Dict[str, Any]]:
        """Restore data from a backup."""
        try:
            # Check local backup first
            local_path = os.path.join(self.backup_dir, f"{backup_id}.json.gz")
            if os.path.exists(local_path):
                return self._restore_local(local_path)
            
            # Try S3 if configured
            if self.s3_client:
                return self._restore_s3(backup_id)
            
            logger.error(f"Backup {backup_id} not found")
            return None
            
        except Exception as e:
            logger.error(f"Error restoring from backup: {str(e)}")
            return None
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """List available backups with metadata."""
        backups = []
        try:
            for backup_name in os.listdir(self.backup_dir):
                backup_path = os.path.join(self.backup_dir, backup_name)
                if os.path.isdir(backup_path):
                    metadata = {}
                    metadata_path = os.path.join(backup_path, 'metadata.json')
                    if os.path.exists(metadata_path):
                        with open(metadata_path, 'r') as f:
                            metadata = json.load(f)
                    
                    backups.append({
                        'name': backup_name,
                        'created_at': backup_name.split('_')[1],
                        'metadata': metadata
                    })
            
            return sorted(backups, key=lambda x: x['created_at'], reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to list backups: {e}")
            return []
    
    def _backup_worker(self):
        """Background worker for processing backup jobs."""
        while not self.stop_event.is_set():
            try:
                backup_job = self.backup_queue.get(timeout=1)
                self._process_backup(backup_job)
                self.backup_queue.task_done()
            except Queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in backup worker: {str(e)}")
    
    def _process_backup(self, backup_job: Dict[str, Any]):
        """Process a backup job."""
        try:
            backup_id = f"{backup_job['type']}_{backup_job['timestamp']}"
            
            # Compress data
            compressed = self._compress_data(backup_job['data'])
            
            # Save locally
            local_path = os.path.join(self.backup_dir, f"{backup_id}.json.gz")
            with open(local_path, 'wb') as f:
                f.write(compressed)
            
            # Upload to S3 if configured
            if self.s3_client:
                self.s3_client.upload_file(
                    local_path,
                    self.s3_bucket,
                    f"backups/{backup_id}.json.gz"
                )
            
            # Upload to cloud if enabled
            if self.use_cloud:
                self._upload_to_cloud(local_path, backup_id)
            
            logger.info(f"Successfully processed backup {backup_id}")
            
        except Exception as e:
            logger.error(f"Error processing backup: {str(e)}")
    
    def _compress_data(self, data: Dict[str, Any]) -> bytes:
        """Compress data for storage."""
        json_data = json.dumps(data).encode('utf-8')
        return zlib.compress(json_data)
    
    def _decompress_data(self, compressed: bytes) -> Dict[str, Any]:
        """Decompress stored data."""
        json_data = zlib.decompress(compressed).decode('utf-8')
        return json.loads(json_data)
    
    def _restore_local(self, backup_path: str) -> Dict[str, Any]:
        """Restore from local backup."""
        with open(backup_path, 'rb') as f:
            compressed = f.read()
        return self._decompress_data(compressed)
    
    def _restore_s3(self, backup_id: str) -> Dict[str, Any]:
        """Restore from S3 backup."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=f"backups/{backup_id}.json.gz"
            )
            compressed = response['Body'].read()
            return self._decompress_data(compressed)
        except ClientError as e:
            logger.error(f"Error retrieving S3 backup: {str(e)}")
            return None
    
    def _upload_to_cloud(self, backup_path: str, backup_name: str) -> bool:
        """Upload backup to cloud storage."""
        try:
            bucket = self.cloud_config['bucket_name']
            
            # Upload all files in backup directory
            for root, _, files in os.walk(backup_path):
                for file in files:
                    local_path = os.path.join(root, file)
                    s3_path = os.path.join(
                        backup_name,
                        os.path.relpath(local_path, backup_path)
                    )
                    
                    self.cloud_client.upload_file(
                        local_path,
                        bucket,
                        s3_path
                    )
            
            logger.info(f"Successfully uploaded backup to cloud: {backup_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload backup to cloud: {e}")
            return False
    
    def cleanup(self):
        """Clean up resources."""
        self.stop_event.set()
        self.backup_thread.join()
    
    def cleanup_old_backups(self) -> bool:
        """Remove backups older than retention period."""
        try:
            current_time = datetime.now()
            for backup in self.list_backups():
                backup_time = datetime.strptime(backup['created_at'], '%Y%m%d_%H%M%S')
                age_days = (current_time - backup_time).days
                
                if age_days > self.retention_days:
                    backup_path = os.path.join(self.backup_dir, backup['name'])
                    shutil.rmtree(backup_path)
                    
                    if self.use_cloud:
                        self._delete_from_cloud(backup['name'])
                    
                    logger.info(f"Removed old backup: {backup['name']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {e}")
            return False
    
    def _delete_from_cloud(self, backup_name: str) -> bool:
        """Delete backup from cloud storage."""
        try:
            bucket = self.cloud_config['bucket_name']
            
            # List and delete all objects for this backup
            paginator = self.cloud_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(
                Bucket=bucket,
                Prefix=backup_name
            ):
                objects = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
                if objects:
                    self.cloud_client.delete_objects(
                        Bucket=bucket,
                        Delete={'Objects': objects}
                    )
            
            logger.info(f"Successfully deleted backup from cloud: {backup_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete backup from cloud: {e}")
            return False 