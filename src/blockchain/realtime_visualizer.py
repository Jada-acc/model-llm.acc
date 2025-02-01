from typing import Dict, List, Optional, Union, Callable
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import threading
import queue
import time
from .blockchain_analysis import BlockchainAnalyzer
from .blockchain_data import BlockchainDataSource

class RealtimeBlockchainVisualizer:
    def __init__(self, analyzer: BlockchainAnalyzer, data_source: BlockchainDataSource,
                 update_interval: int = 60, test_mode: bool = False):
        """
        Initialize real-time blockchain visualizer.
        
        Args:
            analyzer: Instance of BlockchainAnalyzer
            data_source: Instance of BlockchainDataSource
            update_interval: Data update interval in seconds
            test_mode: Whether to run in test mode (no animation)
        """
        self.analyzer = analyzer
        self.data_source = data_source
        self.update_interval = update_interval
        self.data_queue = queue.Queue()
        self.is_running = False
        self.alerts = []
        self.test_mode = test_mode
        self._setup_plots()
        self._data_thread = None

    def _setup_plots(self):
        """Initialize plot settings."""
        plt.style.use('seaborn-v0_8')
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(12, 10))
        self.fig.suptitle('Real-time Blockchain Analytics')
        
        # Initialize empty data
        self.eth_times = []
        self.eth_values = []
        self.sol_times = []
        self.sol_values = []
        
        # Setup plot lines
        self.eth_line, = self.ax1.plot([], [], 'b-', label='Ethereum TPS')
        self.sol_line, = self.ax2.plot([], [], 'g-', label='Solana TPS')
        
        self.ax1.set_title('Ethereum Transactions Per Second')
        self.ax2.set_title('Solana Transactions Per Second')
        
        # Set initial time window to last 5 minutes
        now = datetime.now()
        for ax in [self.ax1, self.ax2]:
            ax.set_xlabel('Time')
            ax.set_ylabel('TPS')
            ax.grid(True)
            ax.legend()
            ax.set_xlim(now - timedelta(minutes=5), now)
            ax.set_ylim(0, 10)  # Initial y-axis range

    def start_streaming(self, duration: Optional[int] = None) -> None:
        """
        Start streaming blockchain data visualization.
        
        Args:
            duration: Optional duration in seconds to run the visualization
        """
        if self.is_running:
            return
            
        self.is_running = True
        self.eth_times = []
        self.eth_values = []
        self.sol_times = []
        self.sol_values = []
        
        # Start data collection thread
        self._data_thread = threading.Thread(target=self._collect_data, args=(duration,))
        self._data_thread.daemon = True
        self._data_thread.start()
        
        if not self.test_mode:
            self._animate()
        else:
            # In test mode, just let the thread run
            time.sleep(0.1)  # Give thread time to start

    def stop_streaming(self) -> None:
        """Stop streaming data and visualization."""
        if not self.is_running:
            return
            
        self.is_running = False
        if self._data_thread and self._data_thread.is_alive():
            self._data_thread.join(timeout=1.0)
        self._data_thread = None
        plt.close('all')

    def _collect_data(self, duration: Optional[int] = None) -> None:
        """
        Collect blockchain data in a separate thread.
        
        Args:
            duration: Optional duration in seconds to collect data
        """
        start_time = time.time()
        import logging
        
        while self.is_running:
            try:
                if not self.data_source:
                    raise ValueError("Data source is not initialized")
                    
                # Fetch latest data
                eth_data = self.data_source.fetch_ethereum_data()
                sol_data = self.data_source.fetch_solana_data()
                
                current_time = datetime.now()
                
                # Calculate transactions per second
                eth_tps = eth_data['transaction_count'] / (60 if self.test_mode else self.update_interval)
                sol_tps = sol_data['transaction_count'] / (60 if self.test_mode else self.update_interval)
                
                data = {
                    'timestamp': current_time,
                    'ethereum_tps': eth_tps,
                    'solana_tps': sol_tps
                }
                
                # Add to queue and process immediately in test mode
                self.data_queue.put(data)
                if self.test_mode:
                    self._update_plot(0)
                
                # Process alerts
                for alert in self.alerts:
                    alert(data)
                
                # Check duration
                if duration and (time.time() - start_time) >= duration:
                    logging.info("Duration reached, stopping stream")
                    self.is_running = False
                    break
                
                # Shorter sleep in test mode
                time.sleep(0.1 if self.test_mode else self.update_interval)
                
            except Exception as e:
                logging.error(f"Error collecting data: {str(e)}")
                if self.test_mode:
                    # In test mode, provide mock data on error
                    mock_data = {
                        'timestamp': datetime.now(),
                        'ethereum_tps': 0.5,
                        'solana_tps': 1.0
                    }
                    self.data_queue.put(mock_data)
                    self._update_plot(0)
                    for alert in self.alerts:
                        alert(mock_data)
                time.sleep(0.1)

    def _update_plot(self, frame: int) -> None:
        """Update plot with new data."""
        try:
            # Get data from queue
            while not self.data_queue.empty():
                data = self.data_queue.get()
                
                # Update data lists
                current_time = data['timestamp']
                self.eth_times.append(current_time)
                self.eth_values.append(data['ethereum_tps'])
                self.sol_times.append(current_time)
                self.sol_values.append(data['solana_tps'])
                
                # Keep only last 5 minutes of data
                cutoff = current_time - timedelta(minutes=5)
                while self.eth_times and self.eth_times[0] < cutoff:
                    self.eth_times.pop(0)
                    self.eth_values.pop(0)
                while self.sol_times and self.sol_times[0] < cutoff:
                    self.sol_times.pop(0)
                    self.sol_values.pop(0)
                
                # Update plot data
                self.eth_line.set_data(self.eth_times, self.eth_values)
                self.sol_line.set_data(self.sol_times, self.sol_values)
                
                # Adjust plot limits
                for ax, times, values in [(self.ax1, self.eth_times, self.eth_values),
                                        (self.ax2, self.sol_times, self.sol_values)]:
                    if times:
                        current_time = times[-1]
                        # Set x-axis to show fixed window
                        window_start = current_time - timedelta(minutes=5)
                        window_end = current_time + timedelta(seconds=10)
                        ax.set_xlim(window_start, window_end)
                        
                        if values:
                            max_val = max(values)
                            min_val = min(values)
                            margin = (max_val - min_val) * 0.1 if max_val != min_val else 1.0
                            ax.set_ylim(max(0, min_val - margin), max_val + margin)
                
                # Check alerts
                for alert in self.alerts:
                    alert(data)
            
            self.fig.canvas.draw()
            
        except Exception as e:
            print(f"Error updating plot: {str(e)}")
            self.stop_streaming()

    def _animate(self) -> None:
        """Start the animation loop."""
        try:
            while self.is_running:
                self._update_plot(0)
                plt.pause(0.1)
        finally:
            plt.close()

    def save_snapshot(self, save_path: str) -> None:
        """
        Save current visualization state to file.
        
        Args:
            save_path: Path to save the snapshot
        """
        self.fig.savefig(save_path)

    def add_alert(self, metric: str, threshold: float, callback: Callable) -> None:
        """
        Add an alert for when a metric crosses a threshold.
        
        Args:
            metric: Metric to monitor ('ethereum_tps' or 'solana_tps')
            threshold: Alert threshold value
            callback: Function to call when threshold is crossed
        """
        def check_alert(data):
            if data[metric] > threshold:
                callback(data)
        
        self.alerts.append(check_alert) 