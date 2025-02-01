from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import logging
from typing import Generator, Optional
from datetime import datetime
from blockchain.models import Base, BlockchainBlock, Transaction, BlockMetrics

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self, database_url: str = "sqlite:///blockchain_data.db"):
        """
        Initialize database manager.
        
        Args:
            database_url: Database connection URL
        """
        self.engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.scoped_session = scoped_session(self.SessionFactory)
        
    def initialize_database(self) -> None:
        """Create all database tables."""
        Base.metadata.create_all(self.engine)
        logger.info("Database initialized successfully")
    
    @contextmanager
    def get_session(self) -> Generator:
        """Get a database session."""
        session = self.scoped_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            session.close()
    
    def store_block(self, blockchain: str, block_data: dict) -> Optional[BlockchainBlock]:
        """
        Store blockchain block data.
        
        Args:
            blockchain: Name of the blockchain ('ethereum' or 'solana')
            block_data: Dictionary containing block data
        """
        with self.get_session() as session:
            try:
                block = BlockchainBlock(
                    blockchain=blockchain,
                    block_number=block_data.get('block_height') or block_data.get('slot'),
                    block_hash=block_data.get('hash') or block_data.get('blockhash'),
                    timestamp=block_data['timestamp'],
                    transaction_count=block_data['transaction_count'],
                    parent_hash=block_data.get('parent_hash'),
                    gas_used=block_data.get('gas_used'),
                    gas_limit=block_data.get('gas_limit'),
                    slot=block_data.get('slot')
                )
                session.add(block)
                session.flush()  # Ensure the block gets an ID
                session.refresh(block)  # Refresh the instance
                return block
            except Exception as e:
                logger.error(f"Error storing block data: {str(e)}")
                return None
    
    def store_metric(self, block_id: int, metric_type: str, value: float) -> Optional[BlockMetrics]:
        """
        Store block metrics.
        
        Args:
            block_id: ID of the associated block
            metric_type: Type of metric (e.g., 'tps', 'gas_price')
            value: Metric value
        """
        with self.get_session() as session:
            try:
                metric = BlockMetrics(
                    block_id=block_id,
                    metric_type=metric_type,
                    value=value,
                    timestamp=datetime.utcnow()
                )
                session.add(metric)
                session.commit()
                return metric
            except Exception as e:
                logger.error(f"Error storing metric: {str(e)}")
                return None
    
    def get_latest_blocks(self, blockchain: str, limit: int = 100) -> list:
        """
        Get the latest blocks for a blockchain.
        
        Args:
            blockchain: Name of the blockchain
            limit: Maximum number of blocks to return
        """
        with self.get_session() as session:
            return session.query(BlockchainBlock)\
                .filter_by(blockchain=blockchain)\
                .order_by(BlockchainBlock.block_number.desc())\
                .limit(limit)\
                .all()
    
    def get_metrics(self, metric_type: str, start_time: datetime, end_time: datetime) -> list:
        """
        Get metrics for a specific time range.
        
        Args:
            metric_type: Type of metric to retrieve
            start_time: Start of time range
            end_time: End of time range
        """
        with self.get_session() as session:
            return session.query(BlockMetrics)\
                .filter(
                    BlockMetrics.metric_type == metric_type,
                    BlockMetrics.timestamp.between(start_time, end_time)
                )\
                .order_by(BlockMetrics.timestamp.asc())\
                .all() 