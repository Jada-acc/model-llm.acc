import unittest
from datetime import datetime
from unittest.mock import Mock, patch
import logging
from src.ingestion.data_quality import DataQualityChecker
from src.storage.quality_storage import QualityStorage
from src.monitoring.quality_alerts import QualityAlertSystem
from src.storage.storage_optimizer import StorageOptimizer
from src.storage.init_db import init_database
import tracemalloc
from sqlalchemy import create_engine, event
from sqlalchemy.engine.base import Engine
from sqlalchemy.pool import Pool
import threading
import sqlite3

tracemalloc.start()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class BaseTestCase(unittest.TestCase):
    """Base test case with common setup and teardown."""
    
    def setUp(self):
        """Set up test environment."""
        self.db_url = "sqlite:///test_quality.db"
        init_database(self.db_url)
        self._connections = set()  # Use set to avoid duplicates
        self._engines = set()  # Track engines
        
        # Define SQLite optimization function
        def optimize_sqlite(dbapi_con, con_record):
            dbapi_con.execute('PRAGMA journal_mode=MEMORY')
        
        # Store function reference
        self._optimize_sqlite = optimize_sqlite
        
        # Register SQLite connection cleanup
        event.listen(Pool, 'connect', self._optimize_sqlite)
    
    def tearDown(self):
        """Clean up test environment."""
        try:
            # Clean up storage resources
            if hasattr(self, 'storage'):
                self.storage.cleanup()
                if isinstance(self.storage.engine, Engine):
                    self._engines.add(self.storage.engine)
            
            # Close any tracked connections
            for conn, conn_type in list(self._connections):
                try:
                    if conn_type == 'Connection':  # SQLite connection
                        try:
                            conn.execute("SELECT 1")  # Test if connection is alive
                            conn.close()
                        except Exception:
                            pass  # Connection already closed
                    else:
                        # Other connection types
                        if hasattr(conn, 'closed') and not conn.closed:
                            conn.close()
                except Exception as e:
                    logger.debug(f"Error closing connection: {str(e)}")  # Downgrade to debug
            self._connections.clear()
            
            # Dispose of all engines
            for engine in self._engines:
                try:
                    # Close thread-local connections
                    for thread in threading.enumerate():
                        if hasattr(thread, '_connections'):
                            for conn in list(thread._connections):
                                try:
                                    if isinstance(conn, sqlite3.Connection):
                                        try:
                                            conn.execute("SELECT 1")
                                            conn.close()
                                        except Exception:
                                            pass
                                    elif hasattr(conn, 'closed') and not conn.closed:
                                        conn.close()
                                except Exception:
                                    pass
                    engine.dispose()
                except Exception as e:
                    logger.warning(f"Error disposing engine: {str(e)}")
            self._engines.clear()
            
            # Remove event listeners
            if hasattr(self, '_optimize_sqlite'):
                event.remove(Pool, 'connect', self._optimize_sqlite)
            
            # Remove test database
            import os
            if os.path.exists("test_quality.db"):
                os.remove("test_quality.db")
                
        except Exception as e:
            logger.error(f"Error in tearDown: {str(e)}")
            raise
    
    def track_connection(self, conn):
        """Track a connection for cleanup.
        
        This function is called both directly and through event listeners.
        When called through an event listener, additional arguments are ignored.
        """
        try:
            if hasattr(conn, 'driver_connection'):
                # Handle SQLAlchemy connection object (new API)
                conn = conn.driver_connection
            elif hasattr(conn, '_dbapi_connection'):
                # Handle raw connection (new API)
                conn = conn._dbapi_connection
            elif isinstance(conn, Engine):
                # Handle engine
                conn = conn.raw_connection()
            
            # Only track if it's a real connection
            if conn is not None:
                # Store connection type for proper cleanup
                self._connections.add((conn, type(conn).__name__))
            
            return conn
        except Exception as e:
            logger.warning(f"Error tracking connection: {str(e)}")
            return conn

class TestQualitySystem(BaseTestCase):
    """Test the complete quality monitoring system."""
    
    def setUp(self):
        """Set up test environment."""
        super().setUp()
        
        # Initialize components
        self.storage = StorageOptimizer(
            self.db_url,
            connection_tracker=getattr(self, 'track_connection', None)
        )
        self.quality_checker = DataQualityChecker()
        self.quality_storage = QualityStorage(self.storage)
        self.alert_system = QualityAlertSystem({
            'from': 'test@example.com',
            'to': 'admin@example.com',
            'smtp_server': 'localhost',
            'username': 'test',
            'password': 'test'
        })
        
        # Test data
        self.valid_data = {
            'timestamp': datetime.now().isoformat(),
            'blocks': [
                {
                    'number': 1000,
                    'hash': '0x123...',
                    'timestamp': datetime.now(),
                    'transactions': [
                        {
                            'hash': '0xtx1',
                            'from': '0x123',
                            'to': '0x456',
                            'value': 1.0
                        }
                    ]
                },
                {
                    'number': 1001,
                    'hash': '0x456...',
                    'timestamp': datetime.now(),
                    'transactions': [
                        {
                            'hash': '0xtx2',
                            'from': '0x789',
                            'to': '0xabc',
                            'value': 2.0
                        }
                    ]
                }
            ]
        }
        
        self.invalid_data = {
            'timestamp': datetime.now().isoformat(),
            'blocks': [
                {
                    'number': 1000,
                    # Missing required 'hash' field
                    'timestamp': None,  # Invalid timestamp
                    'transactions': []
                }
            ],
            'metrics': 'invalid_type'  # Should be a dict
        }
    
    def test_data_quality_checker(self):
        """Test data quality checking functionality."""
        # Test valid data
        quality_result = self.quality_checker.check_data_quality(
            self.valid_data, 'ingestion'
        )
        self.assertTrue(quality_result, "Quality check failed for valid data")
        
        # Test consistency checks
        consistency = self.quality_checker.check_data_consistency(self.valid_data)
        self.assertTrue(
            consistency['block_sequence_valid'],
            "Block sequence validation failed"
        )
        self.assertTrue(
            consistency['timestamps_valid'],
            "Timestamp validation failed"
        )
        
        # Test completeness checks
        completeness = self.quality_checker.check_data_completeness(self.valid_data)
        self.assertEqual(
            completeness['block_completeness'],
            100.0,
            "Block completeness should be 100%"
        )
        
        # Test invalid data
        quality_result = self.quality_checker.check_data_quality(
            self.invalid_data, 'ingestion'
        )
        self.assertFalse(quality_result, "Quality check passed for invalid data")
    
    def test_quality_storage(self):
        """Test quality metrics storage."""
        try:
            # Generate quality metrics with proper format
            timestamp = datetime.now().isoformat()
            metrics = {
                f"ingestion_{timestamp}": {
                    'timestamp': timestamp,
                    'stage': 'ingestion',
                    'total_records': 2,
                    'missing_values': {'percentage': 0, 'count': 0, 'total': 10},
                    'data_types': {
                        'timestamp_valid': True,
                        'blocks_valid': True,
                        'metrics_valid': True
                    },
                    'consistency': {
                        'block_sequence_valid': True,
                        'timestamps_valid': True,
                        'transaction_counts_match': True
                    }
                }
            }
            
            # Test storing metrics
            success = self.quality_storage.store_quality_metrics(metrics)
            self.assertTrue(success, "Failed to store quality metrics")
            
            # Test quality score calculation
            first_metric = next(iter(metrics.values()))
            score = self.quality_storage.calculate_quality_score(first_metric)
            self.assertGreaterEqual(score, 0)
            self.assertLessEqual(score, 100)
            
            # Test report generation
            report = self.quality_storage.generate_quality_report(first_metric)
            self.assertIn('summary', report)
            self.assertIn('issues', report)
            self.assertIn('recommendations', report)
            
        except Exception as e:
            logger.error(f"Error in quality storage test: {str(e)}")
            raise
    
    @patch('smtplib.SMTP')
    def test_quality_alerts(self, mock_smtp):
        """Test quality alerting system."""
        # Configure mock SMTP
        mock_smtp_instance = Mock()
        mock_smtp_instance.starttls.return_value = None
        mock_smtp_instance.login.return_value = None
        mock_smtp_instance.send_message.return_value = None
        
        # Configure the mock class
        mock_smtp.return_value = mock_smtp_instance
        
        # Test alert generation for poor quality
        poor_quality_metrics = {
            'stage': 'ingestion',
            'quality_score': 60.0,
            'missing_values': {'percentage': 25.0},
            'data_types': {'timestamp_valid': True},
            'consistency': {'block_sequence_valid': True}
        }
        
        alerts = self.alert_system.generate_alerts(poor_quality_metrics)
        self.assertTrue(len(alerts) > 0, "No alerts generated for poor quality")
        
        # Test alert sending
        self.alert_system.check_and_alert(poor_quality_metrics)
        mock_smtp_instance.send_message.assert_called_once()
    
    def test_complete_quality_pipeline(self):
        """Test the complete quality monitoring pipeline."""
        try:
            # 1. Check data quality
            quality_result = self.quality_checker.check_data_quality(
                self.valid_data, 'ingestion'
            )
            self.assertTrue(quality_result, "Initial quality check failed")
            
            # 2. Get quality report
            report = self.quality_checker.get_quality_report()
            self.assertIsNotNone(report, "Failed to generate quality report")
            
            # 3. Store quality metrics
            success = self.quality_storage.store_quality_metrics(
                self.quality_checker.quality_metrics
            )
            self.assertTrue(success, "Failed to store quality metrics")
            
            # 4. Check for alerts
            self.alert_system.check_and_alert(
                self.quality_storage.generate_quality_report(
                    self.quality_checker.quality_metrics
                )
            )
            
            logger.info("Successfully tested complete quality pipeline")
            
        except Exception as e:
            logger.error(f"Quality pipeline test failed: {str(e)}")
            raise

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        # Clean up any remaining resources
        import os
        
        # Close any remaining database connections
        engines = [e for e in globals().values() if isinstance(e, Engine)]
        for engine in engines:
            try:
                # Close any connections in thread local storage
                for thread in threading.enumerate():
                    if hasattr(thread, '_connections'):
                        for conn in list(thread._connections):
                            try:
                                if isinstance(conn, sqlite3.Connection):
                                    try:
                                        conn.execute("SELECT 1")
                                        conn.close()
                                    except Exception:
                                        pass
                                elif hasattr(conn, 'closed') and not conn.closed:
                                    conn.close()
                            except Exception:
                                pass
                engine.dispose()
            except Exception:
                pass
        
        # Remove test database
        if os.path.exists("test_quality.db"):
            try:
                os.remove("test_quality.db")
            except Exception as e:
                logger.error(f"Error cleaning up test database: {str(e)}") 