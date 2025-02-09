from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import Pool
import redis
import zlib
import json
import pickle
import base64
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Temporary mock models for testing
class BlockchainBlock:
    pass

class Transaction:
    pass

class BlockMetrics:
    pass

class StorageOptimizer:
    def __init__(self, db_url: str, redis_url: Optional[str] = None, connection_tracker: Optional[callable] = None):
        """Initialize storage optimizer.
        
        Args:
            db_url: Database connection URL
            redis_url: Optional Redis connection URL for caching
            connection_tracker: Optional function to track database connections
        """
        self.engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_use_lifo=True,
            echo_pool=True if logger.level == logging.DEBUG else False
        )
        
        # Store the connection tracker
        self._connection_tracker = connection_tracker
        
        # Define event listener function
        def on_checkout(dbapi_conn, connection_record, connection_proxy):
            if self._connection_tracker:
                self._connection_tracker(dbapi_conn)
        
        # Store function reference
        self._on_checkout = on_checkout
        
        # Add connection pool listener
        if connection_tracker:
            event.listen(Pool, 'checkout', self._on_checkout)
        
        self._session_factory = sessionmaker(
            bind=self.engine,
            expire_on_commit=False
        )
        self.SessionFactory = scoped_session(self._session_factory)
        self.redis_client = redis.from_url(redis_url) if redis_url else None
        
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.SessionFactory()
        try:
            # Track connection if provided
            if self._connection_tracker:
                conn = session.connection()
                if hasattr(conn, 'driver_connection'):
                    self._connection_tracker(conn.driver_connection)
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
            self.SessionFactory.remove()
    
    def optimize_tables(self):
        """Optimize database table structure and indexes."""
        with self.SessionFactory() as session:
            try:
                # Create indexes for frequently queried columns
                session.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_blocks_timestamp 
                    ON blockchain_blocks (timestamp)
                """))
                
                session.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_blocks_blockchain 
                    ON blockchain_blocks (blockchain)
                """))
                
                session.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_transactions_block_id 
                    ON transactions (block_id)
                """))
                
                session.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_metrics_timestamp 
                    ON block_metrics (timestamp)
                """))
                
                logger.info("Successfully created database indexes")
                return True
                
            except Exception as e:
                logger.error(f"Error optimizing tables: {str(e)}")
                return False
    
    def partition_data(self, table_name: str, partition_key: str = 'timestamp', interval: str = 'month'):
        """
        Implement basic partitioning for SQLite (simplified version).
        """
        try:
            # For SQLite, we'll just create a new table for the partition
            partition_table = f"{table_name}_{datetime.now().strftime('%Y_%m')}"
            
            with self.SessionFactory() as session:
                session.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {partition_table} AS 
                    SELECT * FROM {table_name} WHERE 0
                """))
                logger.info(f"Created partition table {partition_table}")
                
            return True
            
        except Exception as e:
            logger.error(f"Error creating partition: {str(e)}")
            return False
    
    def cache_frequent_data(self, key: str, data: Any, expire_seconds: int = 3600):
        """
        Cache frequently accessed data using Redis.
        
        Args:
            key: Cache key
            data: Data to cache
            expire_seconds: Cache expiration time in seconds
        """
        if not self.redis_client:
            return False
            
        try:
            self.redis_client.setex(key, expire_seconds, str(data))
            return True
        except Exception as e:
            logger.error(f"Error caching data: {str(e)}")
            return False
    
    def get_cached_data(self, key: str) -> Optional[str]:
        """
        Retrieve cached data.
        
        Args:
            key: Cache key
        """
        if not self.redis_client:
            return None
            
        try:
            return self.redis_client.get(key)
        except Exception as e:
            logger.error(f"Error retrieving cached data: {str(e)}")
            return None
    
    def analyze_storage_efficiency(self) -> Dict[str, Any]:
        """Analyze storage efficiency and usage patterns."""
        try:
            with self.SessionFactory() as session:
                # SQLite-compatible table size query
                sizes = session.execute(text("""
                    SELECT 
                        name as table_name,
                        'unknown' as total_size
                    FROM sqlite_master 
                    WHERE type='table'
                """))
                
                return {
                    'table_sizes': {row.table_name: row.total_size for row in sizes},
                    'query_stats': []  # SQLite doesn't support pg_stat_statements
                }
                
        except Exception as e:
            logger.error(f"Error analyzing storage: {str(e)}")
            return {}
    
    def monitor_cache_patterns(self) -> Dict[str, Any]:
        """Monitor cache usage patterns."""
        if not self.redis_client:
            return {}
        
        try:
            info = self.redis_client.info()
            return {
                'hits': info.get('keyspace_hits', 0),
                'misses': info.get('keyspace_misses', 0),
                'memory_used': info.get('used_memory_human', '0B'),
                'total_keys': len(self.redis_client.keys('*'))
            }
        except Exception as e:
            logger.error(f"Error monitoring cache: {str(e)}")
            return {}
    
    def cleanup_expired_cache(self) -> bool:
        """Clean up expired cache entries."""
        if not self.redis_client:
            return False
        
        try:
            # Get all keys
            all_keys = self.redis_client.keys('*')
            cleaned = 0
            
            # Check each key's TTL
            for key in all_keys:
                if self.redis_client.ttl(key) <= 0:
                    self.redis_client.delete(key)
                    cleaned += 1
                
            logger.info(f"Cleaned up {cleaned} expired cache entries")
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning cache: {str(e)}")
            return False

    def compress_data(self, data: Any) -> str:
        """
        Compress data using zlib.
        
        Args:
            data: Data to compress
        
        Returns:
            Base64 encoded compressed data string
        """
        try:
            # Convert data to bytes
            data_bytes = pickle.dumps(data)
            # Compress using zlib
            compressed = zlib.compress(data_bytes)
            # Encode as base64 for storage
            return base64.b64encode(compressed).decode('utf-8')
        except Exception as e:
            logger.error(f"Error compressing data: {str(e)}")
            return None

    def decompress_data(self, compressed_str: str) -> Any:
        """
        Decompress zlib compressed data.
        
        Args:
            compressed_str: Base64 encoded compressed data string
        
        Returns:
            Original data object
        """
        try:
            # Decode base64
            compressed = base64.b64decode(compressed_str)
            # Decompress
            data_bytes = zlib.decompress(compressed)
            # Convert back to original object
            return pickle.loads(data_bytes)
        except Exception as e:
            logger.error(f"Error decompressing data: {str(e)}")
            return None

    def compress_old_data(self, table_name: str) -> bool:
        """Compress data older than the retention period."""
        try:
            retention_date = datetime.now() - timedelta(days=30)  # 30 days retention
            
            with self.SessionFactory() as session:
                # Get old data
                result = session.execute(
                    text(f"""
                    SELECT *
                    FROM {table_name}
                    WHERE timestamp < :retention_date
                    AND (compressed IS NULL OR compressed = 0)
                    """),
                    {"retention_date": retention_date.isoformat()}  # Use ISO format string
                )
                
                old_data = result.fetchall()
                if not old_data:
                    logger.info(f"No old data to compress in {table_name}")
                    return True
                    
                # Compress and store
                compressed_data = self.compress_data(old_data)
                session.execute(
                    text("""
                    INSERT INTO compressed_data 
                    (original_table, start_timestamp, end_timestamp, compressed_data)
                    VALUES (:table, :start, :end, :data)
                    """),
                    {
                        "table": table_name,
                        "start": old_data[0].timestamp.isoformat(),
                        "end": old_data[-1].timestamp.isoformat(),
                        "data": compressed_data
                    }
                )
                
                # Mark original data as compressed
                session.execute(
                    text(f"""
                    UPDATE {table_name}
                    SET compressed = 1
                    WHERE timestamp < :retention_date
                    """),
                    {"retention_date": retention_date.isoformat()}
                )
                
                session.commit()
                logger.info(f"Successfully compressed old data from {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error compressing old data: {str(e)}")
            return False

    def restore_compressed_data(self, table_name: str, start_date: datetime, end_date: datetime) -> bool:
        """
        Restore compressed data for a specific time range.
        
        Args:
            table_name: Name of the table to restore data to
            start_date: Start of date range
            end_date: End of date range
        """
        try:
            with self.SessionFactory() as session:
                # Get compressed data
                compressed_rows = session.execute(text("""
                    SELECT compressed_data
                    FROM compressed_data
                    WHERE table_name = :table
                    AND compression_date BETWEEN :start AND :end
                """), {
                    "table": table_name,
                    "start": start_date,
                    "end": end_date
                })
                
                # Restore each row
                for row in compressed_rows:
                    decompressed_data = self.decompress_data(row.compressed_data)
                    if decompressed_data:
                        # Convert dict keys to string for SQL
                        columns = ', '.join(decompressed_data.keys())
                        placeholders = ', '.join(f':{k}' for k in decompressed_data.keys())
                        
                        # Restore to original table
                        session.execute(text(f"""
                            INSERT INTO {table_name} ({columns})
                            VALUES ({placeholders})
                        """), decompressed_data)
                
                session.commit()
                logger.info(f"Successfully restored compressed data to {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error restoring compressed data: {str(e)}")
            return False

    def store_processed_data(self, data: Dict[str, Any]) -> bool:
        """Store processed data in the database."""
        try:
            timestamp = data.get('timestamp', datetime.now())
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
            
            with self.SessionFactory() as session:
                session.execute(text("""
                    INSERT INTO processed_data 
                    (timestamp, data, metadata, encrypted_data, version)
                    VALUES (:timestamp, :data, :metadata, :encrypted_data, :version)
                """), {
                    'timestamp': timestamp,
                    'data': str(data),
                    'metadata': data.get('metadata'),
                    'encrypted_data': data.get('encrypted_data'),
                    'version': data.get('version', '1.0')
                })
                session.commit()
                logger.info("Successfully stored processed data")
                return True
        except Exception as e:
            logger.error(f"Error storing processed data: {str(e)}")
            return False

    def cleanup(self):
        """Clean up all resources."""
        try:
            # Close all sessions
            self.SessionFactory.remove()
            
            # Remove event listeners
            if hasattr(self, '_on_checkout'):
                event.remove(Pool, 'checkout', self._on_checkout)
            
            # Close all connections in the pool
            if hasattr(self.engine, 'dispose'):
                try:
                    # Get and close raw connection using new API
                    with self.engine.connect() as conn:
                        if hasattr(conn, 'driver_connection'):
                            conn.driver_connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {str(e)}")
                # Dispose of the engine
                self.engine.dispose()
            
            # Close Redis connection if exists
            if self.redis_client:
                self.redis_client.close()
            
            # Explicitly close the engine pool
            if hasattr(self.engine, 'pool'):
                self.engine.pool.dispose()
            
            logger.info("Successfully cleaned up all resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")