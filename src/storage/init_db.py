from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Text
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def init_database(db_url: str = "sqlite:///blockchain_data.db"):
    """Initialize database with required tables."""
    try:
        engine = create_engine(db_url)
        metadata = MetaData()

        # Create processed_data table
        processed_data = Table(
            'processed_data', metadata,
            Column('id', Integer, primary_key=True),
            Column('timestamp', DateTime, nullable=False),
            Column('data', Text, nullable=False),
            Column('encrypted_data', Text, nullable=True)
        )

        # Create compressed_data table
        compressed_data = Table(
            'compressed_data', metadata,
            Column('id', Integer, primary_key=True),
            Column('table_name', String, nullable=False),
            Column('original_id', Integer),
            Column('compressed_data', Text, nullable=False),
            Column('compression_date', DateTime, default=datetime.now)
        )

        # Drop existing tables if they exist
        metadata.drop_all(engine)
        
        # Create all tables
        metadata.create_all(engine)
        logger.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    init_database() 