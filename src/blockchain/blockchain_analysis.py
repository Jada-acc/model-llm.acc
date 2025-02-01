from typing import Dict, List, Optional, Union
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from .blockchain_storage import BlockchainStorage

logger = logging.getLogger(__name__)

class BlockchainAnalyzer:
    def __init__(self, storage: BlockchainStorage):
        """
        Initialize blockchain analyzer with storage connection.
        
        Args:
            storage: BlockchainStorage instance for data access
        """
        self.storage = storage
        self._initialize_logging()

    def _initialize_logging(self):
        """Initialize logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def analyze_ethereum_trends(self, days: int = 30) -> Dict:
        """
        Analyze Ethereum blockchain trends.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary containing trend analysis
        """
        try:
            # Get block data
            blocks = self.storage.get_ethereum_blocks(days)
            if not blocks:
                return {"error": "No Ethereum data available"}
            
            # Generate daily statistics
            daily_transactions = {}
            daily_avg_gas = {}
            
            for block in blocks:
                date = datetime.fromtimestamp(block["timestamp"]).date().isoformat()
                
                # Update transaction count
                if date not in daily_transactions:
                    daily_transactions[date] = 0
                daily_transactions[date] += block["transaction_count"]
                
                # Update gas usage
                if date not in daily_avg_gas:
                    daily_avg_gas[date] = []
                daily_avg_gas[date].append(block["gas_used"])
            
            # Calculate averages
            for date in daily_avg_gas:
                daily_avg_gas[date] = sum(daily_avg_gas[date]) / len(daily_avg_gas[date])
            
            return {
                "trends": {
                    "daily_transactions": daily_transactions,
                    "daily_avg_gas": daily_avg_gas
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing Ethereum trends: {str(e)}")
            return {"error": str(e)}

    def analyze_solana_trends(self, days: int = 30) -> Dict:
        """
        Analyze Solana blockchain trends.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary containing trend analysis
        """
        try:
            # Get block data
            blocks = self.storage.get_solana_blocks(days)
            if not blocks:
                return {"error": "No Solana data available"}
            
            # Generate daily statistics
            daily_transactions = {}
            daily_slots_processed = {}
            
            for block in blocks:
                date = datetime.fromtimestamp(block["block_time"]).date().isoformat()
                
                # Update transaction count
                if date not in daily_transactions:
                    daily_transactions[date] = 0
                daily_transactions[date] += block["transaction_count"]
                
                # Update slots processed
                if date not in daily_slots_processed:
                    daily_slots_processed[date] = 0
                daily_slots_processed[date] += 1
            
            return {
                "trends": {
                    "daily_transactions": daily_transactions,
                    "daily_slots_processed": daily_slots_processed
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing Solana trends: {str(e)}")
            return {"error": str(e)}

    def _calculate_growth_rate(self, series: pd.Series) -> float:
        """Calculate the average daily growth rate for a time series."""
        if len(series) < 2:
            return 0.0
        
        # Calculate daily percentage changes
        daily_changes = series.pct_change().dropna()
        
        # Return average daily growth rate
        return float(daily_changes.mean() * 100)  # Convert to percentage

    def get_transaction_patterns(self, blockchain: str, days: int = 7) -> Dict:
        """
        Analyze transaction patterns for the specified blockchain.
        
        Args:
            blockchain: 'ethereum' or 'solana'
            days: Number of days to analyze
        Returns:
            Dictionary containing transaction pattern analysis
        """
        try:
            if blockchain.lower() == 'ethereum':
                with self.storage._get_connection() as conn:
                    # Analyze Ethereum transaction patterns
                    query = '''
                        SELECT t.*, b.timestamp
                        FROM ethereum_transactions t
                        JOIN ethereum_blocks b ON t.block_height = b.block_height
                        WHERE b.timestamp >= ?
                    '''
                    cutoff_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
                    
                    df = pd.read_sql_query(query, conn, params=(cutoff_timestamp,))
                    
                    if df.empty:
                        raise ValueError("No transaction data available")

                    # Analyze transaction values
                    value_stats = {
                        "avg_transaction_value": float(df['value'].mean()),
                        "median_transaction_value": float(df['value'].median()),
                        "max_transaction_value": float(df['value'].max()),
                    }

                    # Analyze gas usage
                    gas_stats = {
                        "avg_gas": float(df['gas'].mean()),
                        "median_gas": float(df['gas'].median()),
                        "max_gas": float(df['gas'].max()),
                    }

                    # Analyze active addresses
                    address_stats = {
                        "unique_senders": int(len(df['from_address'].unique())),
                        "unique_receivers": int(len(df['to_address'].unique())),
                        "most_active_senders": {k: int(v) for k, v in df['from_address'].value_counts().head(5).items()},
                        "most_active_receivers": {k: int(v) for k, v in df['to_address'].value_counts().head(5).items()},
                    }

                    return {
                        "period_days": days,
                        "value_statistics": value_stats,
                        "gas_statistics": gas_stats,
                        "address_statistics": address_stats,
                    }

            elif blockchain.lower() == 'solana':
                with self.storage._get_connection() as conn:
                    # Analyze Solana transaction patterns
                    query = '''
                        SELECT t.*, b.block_time
                        FROM solana_transactions t
                        JOIN solana_blocks b ON t.slot = b.slot
                        WHERE b.block_time >= ?
                    '''
                    cutoff_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
                    
                    df = pd.read_sql_query(query, conn, params=(cutoff_timestamp,))
                    
                    if df.empty:
                        raise ValueError("No transaction data available")

                    # Analyze transaction success rates
                    success_stats = {
                        "success_rate": float((df['success'].sum() / len(df)) * 100),
                        "total_transactions": int(len(df)),
                        "successful_transactions": int(df['success'].sum()),
                        "failed_transactions": int(len(df) - df['success'].sum()),
                    }

                    return {
                        "period_days": days,
                        "transaction_statistics": success_stats,
                    }

            else:
                raise ValueError(f"Unsupported blockchain: {blockchain}")

        except ValueError as e:
            logger.error(f"Error analyzing transaction patterns: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error analyzing transaction patterns: {str(e)}")
            return {"error": str(e)}

    def compare_chains(self, days: int = 7) -> Dict:
        """
        Compare trends between Ethereum and Solana.
        
        Args:
            days: Number of days to analyze
        Returns:
            Dictionary containing comparative analysis
        """
        try:
            eth_trends = self.analyze_ethereum_trends(days)
            sol_trends = self.analyze_solana_trends(days)

            if "error" in eth_trends or "error" in sol_trends:
                return {"error": "Unable to fetch complete data for comparison"}

            # Compare transaction volumes
            eth_total_tx = len(eth_trends["trends"]["daily_transactions"])
            sol_total_tx = len(sol_trends["trends"]["daily_transactions"])

            comparison = {
                "transaction_volume_ratio": float(eth_total_tx / sol_total_tx if sol_total_tx > 0 else 0),
                "ethereum": {
                    "daily_tx_average": float(eth_total_tx / days),
                    "tx_growth_rate": float(self._calculate_growth_rate(pd.Series(list(eth_trends["trends"]["daily_transactions"].values())))),
                },
                "solana": {
                    "daily_tx_average": float(sol_total_tx / days),
                    "tx_growth_rate": float(self._calculate_growth_rate(pd.Series(list(sol_trends["trends"]["daily_transactions"].values())))),
                },
                "relative_growth": {
                    "ethereum_vs_solana": float(self._calculate_growth_rate(pd.Series(list(eth_trends["trends"]["daily_transactions"].values()))) - 
                                        float(self._calculate_growth_rate(pd.Series(list(sol_trends["trends"]["daily_transactions"].values())))))
                }
            }

            return comparison

        except Exception as e:
            logger.error(f"Error comparing chains: {str(e)}")
            return {"error": str(e)} 