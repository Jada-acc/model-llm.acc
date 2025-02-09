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
        self.s3_bucket = config.get('s3_bucket')
        self.backup_queue = Queue()
        self.stop_event = threading.Event()
        
        # Initialize backup directory
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Initialize S3 client if configured
        self.s3_client = None
        if self.s3_bucket:
            self.s3_client = boto3.client('s3')
        
        # Start backup worker thread
        self.backup_thread = threading.Thread(target=self._backup_worker)
        self.backup_thread.daemon = True
        self.backup_thread.start()
    
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
        """List available backups."""
        try:
            backups = []
            
            # List local backups
            for filename in os.listdir(self.backup_dir):
                if filename.endswith('.json.gz'):
                    backup_id = filename.replace('.json.gz', '')
                    backups.append(self._get_backup_metadata(backup_id))
            
            # List S3 backups if configured
            if self.s3_client:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix='backups/'
                )
                for obj in response.get('Contents', []):
                    backup_id = obj['Key'].split('/')[-1].replace('.json.gz', '')
                    backups.append({
                        'id': backup_id,
                        'location': 's3',
                        'timestamp': obj['LastModified'].isoformat(),
                        'size': obj['Size']
                    })
            
            return sorted(backups, key=lambda x: x['timestamp'], reverse=True)
            
        except Exception as e:
            logger.error(f"Error listing backups: {str(e)}")
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
    
    def _get_backup_metadata(self, backup_id: str) -> Dict[str, Any]:
        """Get metadata for a backup."""
        backup_path = os.path.join(self.backup_dir, f"{backup_id}.json.gz")
        return {
            'id': backup_id,
            'location': 'local',
            'timestamp': datetime.fromtimestamp(
                os.path.getmtime(backup_path)
            ).isoformat(),
            'size': os.path.getsize(backup_path)
        }
    
    def cleanup(self):
        """Clean up resources."""
        self.stop_event.set()
        self.backup_thread.join() 