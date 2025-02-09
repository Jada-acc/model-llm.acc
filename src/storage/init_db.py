from sqlalchemy import create_engine, text, event
import logging
from datetime import datetime
from src.storage.db_adapters import register_adapters
import sqlite3

logger = logging.getLogger(__name__)

def init_database(db_url: str) -> None:
    """Initialize database with required tables."""
    try:
        # Register custom datetime adapters
        register_adapters()
        
        # Create engine with SQLite optimizations
        engine = create_engine(
            db_url,
            connect_args={
                "detect_types": sqlite3.PARSE_DECLTYPES,
                "isolation_level": None  # Autocommit mode
            },
            pool_pre_ping=True
        )
        
        # Add SQLite optimizations
        @event.listens_for(engine, 'connect')
        def optimize_sqlite(dbapi_connection, connection_record):
            dbapi_connection.execute('PRAGMA journal_mode=MEMORY')
            dbapi_connection.execute('PRAGMA synchronous=OFF')
            dbapi_connection.execute('PRAGMA cache_size=10000')
        
        with engine.connect() as conn:
            # Create quality_metrics table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS quality_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    stage TEXT NOT NULL,
                    metrics TEXT NOT NULL,
                    report TEXT,
                    score FLOAT
                )
            """))
            
            # Create processed_data table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS processed_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    data TEXT NOT NULL,
                    metadata TEXT,
                    encrypted_data TEXT,
                    version TEXT
                )
            """))
            
            # Create blockchain_data table for storing raw blockchain data
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS blockchain_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    block_number INTEGER NOT NULL,
                    block_hash TEXT NOT NULL,
                    data TEXT NOT NULL,
                    compressed BOOLEAN DEFAULT FALSE
                )
            """))
            
            # Create compressed_data table for storing compressed historical data
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS compressed_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_table TEXT NOT NULL,
                    start_timestamp DATETIME NOT NULL,
                    end_timestamp DATETIME NOT NULL,
                    compressed_data TEXT NOT NULL,
                    compression_date DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.commit()
            logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    init_database("sqlite:///blockchain_data.db") 