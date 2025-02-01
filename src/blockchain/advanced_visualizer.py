from typing import Dict, List, Optional, Union
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression
from .blockchain_analysis import BlockchainAnalyzer

class AdvancedBlockchainVisualizer:
    def __init__(self, analyzer: BlockchainAnalyzer):
        """
        Initialize advanced blockchain visualizer.
        
        Args:
            analyzer: Instance of BlockchainAnalyzer to get data from
        """
        self.analyzer = analyzer
        plt.style.use('default')  # Using default matplotlib style

    def plot_network_heatmap(self, blockchain: str, days: int = 30, save_path: Optional[str] = None) -> None:
        """
        Create a heatmap showing network activity patterns over time.
        
        Args:
            blockchain: 'ethereum' or 'solana'
            days: Number of days to analyze
            save_path: Optional path to save the plot
        """
        if blockchain.lower() not in ['ethereum', 'solana']:
            raise ValueError(f"Unsupported blockchain: {blockchain}")
        
        if days <= 0:
            raise ValueError("Days must be a positive integer")

        if blockchain.lower() == 'ethereum':
            data = self.analyzer.analyze_ethereum_trends(days)
        else:
            data = self.analyzer.analyze_solana_trends(days)

        if "error" in data:
            raise ValueError(f"Unable to fetch {blockchain} data for visualization")

        # Process daily transactions into hourly activity
        daily_data = data["trends"]["daily_transactions"]
        dates = list(daily_data.keys())
        transactions = list(daily_data.values())
        
        # Create activity matrix (24 hours x 7 days)
        activity_matrix = np.array(transactions).reshape(-1, 7)[:24, :]
        
        plt.figure(figsize=(12, 8))
        plt.imshow(activity_matrix, cmap='YlOrRd', aspect='auto')
        plt.colorbar(label='Transaction Count')
        
        # Set labels
        plt.xticks(range(7), ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
        plt.yticks(range(24), range(24))
        plt.title(f'{blockchain.capitalize()} Network Activity Heatmap')
        plt.xlabel('Day of Week')
        plt.ylabel('Hour of Day')
        
        if save_path:
            plt.savefig(save_path)
        plt.close()

    def plot_gas_price_prediction(self, days: int = 30, prediction_days: int = 7, 
                                save_path: Optional[str] = None) -> None:
        """
        Create a plot showing gas price trends and predictions.
        
        Args:
            days: Number of days of historical data to use
            prediction_days: Number of days to predict forward
            save_path: Optional path to save the plot
        """
        if days <= 0:
            raise ValueError("Days must be a positive integer")
        
        if prediction_days <= 0:
            raise ValueError("Prediction days must be a positive integer")

        data = self.analyzer.analyze_ethereum_trends(days)
        
        if "error" in data:
            raise ValueError("Unable to fetch Ethereum data for visualization")

        # Get historical gas prices
        daily_gas = data["trends"]["daily_avg_gas"]
        dates = [datetime.fromisoformat(d) for d in daily_gas.keys()]
        gas_prices = list(daily_gas.values())
        
        # Prepare data for prediction
        X = np.arange(len(dates)).reshape(-1, 1)
        y = np.array(gas_prices)
        
        # Fit linear regression
        model = LinearRegression()
        model.fit(X, y)
        
        # Generate predictions
        future_days = np.arange(len(dates), len(dates) + prediction_days).reshape(-1, 1)
        predictions = model.predict(future_days)
        
        # Plot results
        plt.figure(figsize=(12, 6))
        plt.plot(dates, gas_prices, label='Historical', marker='o')
        
        # Add predicted values
        future_dates = [dates[-1] + timedelta(days=x+1) for x in range(prediction_days)]
        plt.plot(future_dates, predictions, label='Predicted', linestyle='--', color='red')
        
        plt.title('Ethereum Gas Price Prediction')
        plt.xlabel('Date')
        plt.ylabel('Average Gas Price')
        plt.legend()
        plt.grid(True)
        
        if save_path:
            plt.savefig(save_path)
        plt.close()

    def plot_smart_contract_interactions(self, days: int = 7, save_path: Optional[str] = None) -> None:
        """
        Create a network graph of smart contract interactions.
        
        Args:
            days: Number of days to analyze
            save_path: Optional path to save the plot
        """
        patterns = self.analyzer.get_transaction_patterns('ethereum', days)
        
        if "error" in patterns:
            raise ValueError("Unable to fetch transaction patterns for visualization")

        # Extract top contracts and their interactions
        top_contracts = patterns["address_statistics"]["most_active_receivers"]
        
        # Create network plot
        plt.figure(figsize=(12, 8))
        
        # Plot nodes in a circular layout
        n = len(top_contracts)
        angles = np.linspace(0, 2*np.pi, n, endpoint=False)
        
        # Plot nodes
        x = np.cos(angles)
        y = np.sin(angles)
        plt.scatter(x, y, s=200, c='blue', alpha=0.5)
        
        # Add labels
        for i, (contract, _) in enumerate(top_contracts.items()):
            plt.annotate(f"Contract {i+1}", 
                        (x[i], y[i]),
                        xytext=(10, 10),
                        textcoords='offset points')
        
        # Draw connections
        for i in range(n):
            for j in range(i+1, n):
                plt.plot([x[i], x[j]], [y[i], y[j]], 'gray', alpha=0.2)
        
        plt.title('Smart Contract Interaction Network')
        plt.axis('equal')
        
        if save_path:
            plt.savefig(save_path)
        plt.close()

    def plot_correlation_matrix(self, days: int = 30, save_path: Optional[str] = None) -> None:
        """
        Create a correlation matrix of various blockchain metrics.
        
        Args:
            days: Number of days to analyze
            save_path: Optional path to save the plot
        """
        eth_data = self.analyzer.analyze_ethereum_trends(days)
        sol_data = self.analyzer.analyze_solana_trends(days)
        
        if "error" in eth_data or "error" in sol_data:
            raise ValueError("Unable to fetch blockchain data for correlation analysis")

        # Extract metrics
        metrics = {
            'eth_transactions': list(eth_data["trends"]["daily_transactions"].values()),
            'eth_gas': list(eth_data["trends"]["daily_avg_gas"].values()),
            'sol_transactions': list(sol_data["trends"]["daily_transactions"].values()),
            'sol_slots': list(sol_data["trends"]["daily_slots_processed"].values())
        }
        
        df = pd.DataFrame(metrics)
        corr_matrix = df.corr()
        
        # Plot correlation matrix
        plt.figure(figsize=(10, 8))
        im = plt.imshow(corr_matrix, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
        plt.colorbar(im, label='Correlation')
        
        # Add correlation values as text
        for i in range(len(corr_matrix)):
            for j in range(len(corr_matrix)):
                text = plt.text(j, i, f'{corr_matrix.iloc[i, j]:.2f}',
                              ha='center', va='center')
        
        # Set labels
        labels = ['ETH Tx', 'ETH Gas', 'SOL Tx', 'SOL Slots']
        plt.xticks(range(len(labels)), labels, rotation=45)
        plt.yticks(range(len(labels)), labels)
        plt.title('Blockchain Metrics Correlation Matrix')
        
        if save_path:
            plt.savefig(save_path, bbox_inches='tight')
        plt.close()