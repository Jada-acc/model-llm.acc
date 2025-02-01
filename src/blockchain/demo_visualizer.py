from .blockchain_analysis import BlockchainAnalyzer
from .blockchain_data import BlockchainDataSource
from .realtime_visualizer import RealtimeBlockchainVisualizer
import time

def main():
    """Run a demo of the real-time blockchain visualizer."""
    # Initialize components
    data_source = BlockchainDataSource()
    analyzer = BlockchainAnalyzer()
    
    # Create visualizer with shorter update interval for demo
    visualizer = RealtimeBlockchainVisualizer(
        analyzer=analyzer,
        data_source=data_source,
        update_interval=5  # Update every 5 seconds
    )
    
    # Add a sample alert
    def alert_callback(data):
        print(f"High TPS Alert! Ethereum TPS: {data['ethereum_tps']:.2f}")
    
    visualizer.add_alert('ethereum_tps', threshold=1.0, callback=alert_callback)
    
    try:
        print("Starting real-time blockchain visualization...")
        print("Press Ctrl+C to stop")
        
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