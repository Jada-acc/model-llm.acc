from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class BlockchainBlock(Base):
    """Base model for blockchain blocks."""
    __tablename__ = 'blockchain_blocks'
    
    id = Column(Integer, primary_key=True)
    blockchain = Column(String(50), nullable=False)  # 'ethereum' or 'solana'
    block_number = Column(Integer, nullable=False)
    block_hash = Column(String(255), unique=True)
    timestamp = Column(DateTime, nullable=False)
    transaction_count = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Chain-specific fields
    parent_hash = Column(String(255))
    gas_used = Column(Integer)  # For Ethereum
    gas_limit = Column(Integer)  # For Ethereum
    slot = Column(Integer)      # For Solana
    
    # Relationships
    transactions = relationship("Transaction", back_populates="block")
    metrics = relationship("BlockMetrics", back_populates="block")

class Transaction(Base):
    """Model for individual transactions."""
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    block_id = Column(Integer, ForeignKey('blockchain_blocks.id'))
    transaction_hash = Column(String(255), unique=True)
    timestamp = Column(DateTime, nullable=False)
    status = Column(String(50))  # success, failed, pending
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Chain-specific fields
    from_address = Column(String(255))
    to_address = Column(String(255))
    value = Column(Float)
    gas_price = Column(Integer)  # For Ethereum
    signature = Column(String(255))  # For Solana
    
    # Relationships
    block = relationship("BlockchainBlock", back_populates="transactions")

class BlockMetrics(Base):
    """Model for block-level metrics."""
    __tablename__ = 'block_metrics'
    
    id = Column(Integer, primary_key=True)
    block_id = Column(Integer, ForeignKey('blockchain_blocks.id'))
    metric_type = Column(String(50))  # e.g., 'tps', 'gas_price', 'latency'
    value = Column(Float)
    timestamp = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    block = relationship("BlockchainBlock", back_populates="metrics")