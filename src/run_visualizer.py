from blockchain.blockchain_analysis import BlockchainAnalyzer
from blockchain.blockchain_data import BlockchainDataSource
from blockchain.blockchain_storage import BlockchainStorage
from blockchain.realtime_visualizer import RealtimeBlockchainVisualizer
import time

def main():
    """Run a demo of the real-time blockchain visualizer."""
    # Initialize components with mock data
    storage = BlockchainStorage()
    data_source = BlockchainDataSource(use_mock=True)  # Explicitly use mock data
    analyzer = BlockchainAnalyzer(storage=storage)
    
    # Create visualizer with longer update interval for demo
    visualizer = RealtimeBlockchainVisualizer(
        analyzer=analyzer,
        data_source=data_source,
        update_interval=5  # Use 5 second intervals for smoother updates
    )
    
    # Add a sample alert with higher threshold and debouncing
    last_alert_time = {'value': 0}  # Using dict for mutable state in closure
    
    def alert_callback(data):
        current_time = time.time()
        # Only alert if 10 seconds have passed since last alert
        if current_time - last_alert_time['value'] >= 10:
            print(f"\nHigh TPS Alert! Ethereum TPS: {data['ethereum_tps']:.2f}")
            last_alert_time['value'] = current_time
    
    visualizer.add_alert('ethereum_tps', threshold=15.0, callback=alert_callback)  # Higher threshold
    
    try:
        print("Starting real-time blockchain visualization...")
        print("Press Ctrl+C to stop")
        print("Using mock data with varying TPS values")
        print("Alerts will trigger when Ethereum TPS exceeds 15.0 (debounced to 10s intervals)")
        
        # Start streaming indefinitely
        visualizer.start_streaming()
        
        # Keep the script running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping visualization...")
    finally:
        visualizer.stop_streaming()
        print("Visualization stopped")

if __name__ == "__main__":
    main() 