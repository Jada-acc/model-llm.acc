from typing import Dict, List, Optional
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from .blockchain_analysis import BlockchainAnalyzer

class BlockchainVisualizer:
    def __init__(self, analyzer: BlockchainAnalyzer):
        """
        Initialize the blockchain visualizer.
        
        Args:
            analyzer: Instance of BlockchainAnalyzer to get data from
        """
        self.analyzer = analyzer
        # Set style for all plots
        plt.style.use('seaborn-v0_8')  # Using built-in matplotlib style

    def plot_transaction_trends(self, days: int = 30, save_path: Optional[str] = None) -> None:
        """
        Plot daily transaction trends for both Ethereum and Solana.
        
        Args:
            days: Number of days to analyze
            save_path: Optional path to save the plot
        """
        if days <= 0:
            raise ValueError("Days must be a positive integer")

        eth_data = self.analyzer.analyze_ethereum_trends(days)
        sol_data = self.analyzer.analyze_solana_trends(days)

        if "error" in eth_data or "error" in sol_data:
            raise ValueError("Unable to fetch data for visualization")

        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        # Plot Ethereum transactions
        eth_dates = list(eth_data["trends"]["daily_transactions"].keys())
        eth_values = list(eth_data["trends"]["daily_transactions"].values())
        ax1.plot(eth_dates, eth_values, marker='o')
        ax1.set_title('Ethereum Daily Transactions')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Number of Transactions')
        ax1.tick_params(axis='x', rotation=45)

        # Plot Solana transactions
        sol_dates = list(sol_data["trends"]["daily_transactions"].keys())
        sol_values = list(sol_data["trends"]["daily_transactions"].values())
        ax2.plot(sol_dates, sol_values, marker='o', color='green')
        ax2.set_title('Solana Daily Transactions')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Number of Transactions')
        ax2.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        if save_path:
            plt.savefig(save_path)
        plt.close()

    def plot_gas_usage(self, days: int = 30, save_path: Optional[str] = None) -> None:
        """
        Plot Ethereum gas usage trends.
        
        Args:
            days: Number of days to analyze
            save_path: Optional path to save the plot
        """
        if days <= 0:
            raise ValueError("Days must be a positive integer")

        eth_data = self.analyzer.analyze_ethereum_trends(days)

        if "error" in eth_data:
            raise ValueError("Unable to fetch Ethereum data for visualization")

        dates = list(eth_data["trends"]["daily_avg_gas"].keys())
        values = list(eth_data["trends"]["daily_avg_gas"].values())

        plt.figure(figsize=(12, 6))
        plt.plot(dates, values, marker='o', color='orange')
        plt.title('Ethereum Daily Average Gas Usage')
        plt.xlabel('Date')
        plt.ylabel('Average Gas Used')
        plt.tick_params(axis='x', rotation=45)
        plt.grid(True)

        if save_path:
            plt.savefig(save_path)
        plt.close()

    def plot_blockchain_comparison(self, days: int = 30, save_path: Optional[str] = None) -> None:
        """
        Create a comparative visualization between Ethereum and Solana.
        
        Args:
            days: Number of days to analyze
            save_path: Optional path to save the plot
        """
        if days <= 0:
            raise ValueError("Days must be a positive integer")

        comparison = self.analyzer.compare_chains(days)

        if "error" in comparison:
            raise ValueError("Unable to fetch comparison data for visualization")

        # Prepare data for plotting
        metrics = ['daily_tx_average', 'tx_growth_rate']
        eth_values = [comparison['ethereum'][m] for m in metrics]
        sol_values = [comparison['solana'][m] for m in metrics]

        # Create bar plot
        x = np.arange(len(metrics))
        width = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(x - width/2, eth_values, width, label='Ethereum')
        ax.bar(x + width/2, sol_values, width, label='Solana')

        ax.set_ylabel('Values')
        ax.set_title('Blockchain Comparison')
        ax.set_xticks(x)
        ax.set_xticklabels(['Daily Transactions', 'Growth Rate (%)'])
        ax.legend()

        plt.tight_layout()
        if save_path:
            plt.savefig(save_path)
        plt.close()

    def plot_transaction_patterns(self, blockchain: str, days: int = 7, save_path: Optional[str] = None) -> None:
        """
        Plot transaction patterns for the specified blockchain.
        
        Args:
            blockchain: 'ethereum' or 'solana'
            days: Number of days to analyze
            save_path: Optional path to save the plot
        """
        if blockchain.lower() not in ['ethereum', 'solana']:
            raise ValueError(f"Unsupported blockchain: {blockchain}")
        
        if days <= 0:
            raise ValueError("Days must be a positive integer")

        patterns = self.analyzer.get_transaction_patterns(blockchain, days)

        if "error" in patterns:
            raise ValueError(f"Unable to fetch {blockchain} transaction patterns for visualization")

        if blockchain.lower() == 'ethereum':
            # Create subplots for different Ethereum metrics
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

            # Plot value statistics
            value_stats = patterns['value_statistics']
            metrics = ['avg_transaction_value', 'median_transaction_value', 'max_transaction_value']
            values = [value_stats[m] for m in metrics]
            ax1.bar(metrics, values)
            ax1.set_title('Transaction Value Statistics')
            ax1.tick_params(axis='x', rotation=45)

            # Plot gas statistics
            gas_stats = patterns['gas_statistics']
            metrics = ['avg_gas', 'median_gas', 'max_gas']
            values = [gas_stats[m] for m in metrics]
            ax2.bar(metrics, values, color='orange')
            ax2.set_title('Gas Usage Statistics')
            ax2.tick_params(axis='x', rotation=45)

        else:  # Solana
            # Plot success rate statistics
            plt.figure(figsize=(10, 6))
            success_stats = patterns['transaction_statistics']
            metrics = ['successful_transactions', 'failed_transactions']
            values = [success_stats[m] for m in metrics]
            
            plt.pie(values, labels=metrics, autopct='%1.1f%%', colors=['green', 'red'])
            plt.title(f'Solana Transaction Success Rate\nTotal Transactions: {success_stats["total_transactions"]}')

        plt.tight_layout()
        if save_path:
            plt.savefig(save_path)
        plt.close() 