import unittest
from datetime import datetime
import logging
from src.pipeline.etl_pipeline import ETLPipeline
from src.storage.storage_optimizer import StorageOptimizer
from src.storage.init_db import init_database

logger = logging.getLogger(__name__)

class TestETLPipeline(unittest.TestCase):
    """Test ETL pipeline functionality."""
    
    def setUp(self):
        """Set up test environment."""
        self.db_url = "sqlite:///test_etl.db"
        init_database(self.db_url)
        
        # Initialize components
        self.storage = StorageOptimizer(self.db_url)
        self.pipeline = ETLPipeline(self.storage)
        
        # Register test transformations
        self.pipeline.register_transformation(
            'normalize_timestamps',
            lambda x: {**x, 'timestamp': datetime.fromisoformat(x['timestamp'])}
        )
        
        self.pipeline.register_transformation(
            'add_metadata',
            lambda x: {**x, 'processed_at': datetime.now().isoformat()}
        )
        
        # Test data
        self.test_data = [
            {
                'id': 1,
                'timestamp': '2024-01-01T00:00:00',
                'value': 100
            },
            {
                'id': 2,
                'timestamp': '2024-01-01T00:01:00',
                'value': 200
            }
        ]
    
    def tearDown(self):
        """Clean up test environment."""
        import os
        if os.path.exists("test_etl.db"):
            os.remove("test_etl.db")
    
    def test_extract(self):
        """Test data extraction."""
        # Store test data
        with self.storage.session_scope() as session:
            session.execute(
                "CREATE TABLE IF NOT EXISTS test_source (id INT, timestamp TEXT, value INT)"
            )
            for record in self.test_data:
                session.execute(
                    "INSERT INTO test_source (id, timestamp, value) VALUES (:id, :timestamp, :value)",
                    record
                )
        
        # Test extraction
        query = {
            'query': "SELECT * FROM test_source WHERE value > :min_value",
            'params': {'min_value': 150}
        }
        
        extracted = self.pipeline.extract('test_source', query)
        self.assertEqual(len(extracted), 1)
        self.assertEqual(extracted[0]['value'], 200)
    
    def test_transform(self):
        """Test data transformation."""
        transformed = self.pipeline.transform(
            self.test_data,
            ['normalize_timestamps', 'add_metadata']
        )
        
        self.assertEqual(len(transformed), 2)
        self.assertIsInstance(transformed[0]['timestamp'], datetime)
        self.assertIn('processed_at', transformed[0])
    
    def test_load(self):
        """Test data loading."""
        # Create target table
        with self.storage.session_scope() as session:
            session.execute(
                "CREATE TABLE IF NOT EXISTS test_target "
                "(id INT, timestamp TEXT, value INT, processed_at TEXT)"
            )
        
        # Transform and load data
        transformed = self.pipeline.transform(
            self.test_data,
            ['normalize_timestamps', 'add_metadata']
        )
        
        success = self.pipeline.load(transformed, 'test_target')
        self.assertTrue(success)
        
        # Verify loaded data
        with self.storage.session_scope() as session:
            result = session.execute("SELECT COUNT(*) as count FROM test_target")
            count = result.fetchone()['count']
            self.assertEqual(count, 2)
    
    def test_complete_pipeline(self):
        """Test complete ETL pipeline."""
        # Set up source and target
        with self.storage.session_scope() as session:
            session.execute(
                "CREATE TABLE IF NOT EXISTS source_table "
                "(id INT, timestamp TEXT, value INT)"
            )
            session.execute(
                "CREATE TABLE IF NOT EXISTS target_table "
                "(id INT, timestamp TEXT, value INT, processed_at TEXT)"
            )
            
            # Insert test data
            for record in self.test_data:
                session.execute(
                    "INSERT INTO source_table (id, timestamp, value) "
                    "VALUES (:id, :timestamp, :value)",
                    record
                )
        
        # Run complete pipeline
        success = self.pipeline.process_batch(
            source='source_table',
            target='target_table',
            query={'query': "SELECT * FROM source_table"},
            transformations=['normalize_timestamps', 'add_metadata']
        )
        
        self.assertTrue(success)
        
        # Verify results
        with self.storage.session_scope() as session:
            result = session.execute("SELECT COUNT(*) as count FROM target_table")
            count = result.fetchone()['count']
            self.assertEqual(count, 2)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main() 