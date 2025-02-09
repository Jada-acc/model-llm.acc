import unittest
from unittest.mock import Mock, patch
import os
import tempfile
from datetime import datetime
from src.backup.backup_manager import BackupManager

class TestBackupManager(unittest.TestCase):
    """Test suite for backup manager."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            'backup_dir': self.temp_dir,
            's3_bucket': 'test-bucket'
        }
        self.test_data = {
            'id': 'test123',
            'timestamp': datetime.now().isoformat(),
            'data': {'key': 'value'}
        }
    
    def tearDown(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('boto3.client')
    def test_backup_local(self, mock_s3):
        """Test local backup functionality."""
        manager = BackupManager(self.config)
        
        # Test scheduling backup
        success = manager.schedule_backup(self.test_data, 'test')
        self.assertTrue(success)
        
        # Wait for backup to complete
        manager.backup_queue.join()
        
        # Verify backup file exists
        backup_files = os.listdir(self.temp_dir)
        self.assertTrue(any(f.endswith('.json.gz') for f in backup_files))
    
    @patch('boto3.client')
    def test_restore_backup(self, mock_s3):
        """Test backup restoration."""
        manager = BackupManager(self.config)
        
        # Create a backup
        manager.schedule_backup(self.test_data, 'test')
        manager.backup_queue.join()
        
        # Get backup ID
        backup_files = os.listdir(self.temp_dir)
        backup_id = backup_files[0].replace('.json.gz', '')
        
        # Test restoration
        restored_data = manager.restore_from_backup(backup_id)
        self.assertIsNotNone(restored_data)
        self.assertEqual(restored_data['id'], self.test_data['id'])
    
    @patch('boto3.client')
    def test_list_backups(self, mock_s3):
        """Test backup listing."""
        manager = BackupManager(self.config)
        
        # Create multiple backups
        for i in range(3):
            data = self.test_data.copy()
            data['id'] = f'test{i}'
            manager.schedule_backup(data, 'test')
        
        manager.backup_queue.join()
        
        # Test listing
        backups = manager.list_backups()
        self.assertEqual(len(backups), 3)
        self.assertTrue(all('timestamp' in b for b in backups))

if __name__ == '__main__':
    unittest.main() 