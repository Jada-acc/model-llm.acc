import unittest
from unittest.mock import Mock, patch
from datetime import datetime
from src.quality.quality_monitor import QualityMonitor
from src.quality.alert_manager import AlertManager

class TestQualitySystem(unittest.TestCase):
    """Test suite for quality monitoring system."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            'thresholds': {
                'accuracy': 0.95,
                'latency': 100,
                'error_rate': 0.01
            },
            'alert_channels': ['email', 'slack']
        }
        self.test_metrics = {
            'accuracy': 0.98,
            'latency': 50,
            'error_rate': 0.005,
            'timestamp': datetime.now().isoformat()
        }
    
    @patch('src.quality.alert_manager.AlertManager')
    def test_quality_monitoring(self, mock_alert_manager):
        """Test quality monitoring functionality."""
        monitor = QualityMonitor(self.config)
        
        # Test metric evaluation
        result = monitor.evaluate_metrics(self.test_metrics)
        self.assertTrue(result['passed'])
        self.assertEqual(len(result['violations']), 0)
    
    def test_alert_generation(self):
        """Test alert generation for quality violations."""
        alert_manager = AlertManager(self.config)
        
        # Create test violation
        violation = {
            'metric': 'accuracy',
            'threshold': 0.95,
            'value': 0.90,
            'timestamp': datetime.now().isoformat()
        }
        
        # Test alert creation
        alert = alert_manager.create_alert(violation)
        self.assertIsNotNone(alert)
        self.assertEqual(alert['metric'], violation['metric'])
    
    @patch('src.quality.alert_manager.AlertManager.send_alert')
    def test_alert_delivery(self, mock_send):
        """Test alert delivery to configured channels."""
        alert_manager = AlertManager(self.config)
        
        # Create and send test alert
        alert = {
            'id': 'alert123',
            'metric': 'accuracy',
            'message': 'Accuracy below threshold',
            'severity': 'high',
            'timestamp': datetime.now().isoformat()
        }
        
        alert_manager.send_alert(alert)
        
        # Verify alert was sent to all channels
        self.assertEqual(mock_send.call_count, len(self.config['alert_channels']))

if __name__ == '__main__':
    unittest.main() 