import pytest
import os
import time
from datetime import datetime
import matplotlib.pyplot as plt
from .blockchain_storage import BlockchainStorage
from .blockchain_analysis import BlockchainAnalyzer
from .test_data_source import TestBlockchainDataSource
from .realtime_visualizer import RealtimeBlockchainVisualizer

@pytest.fixture
def realtime_visualizer(analyzer):
    """Create a RealtimeBlockchainVisualizer instance for testing."""
    data_source = TestBlockchainDataSource(eth_tps=0.5, sol_tps=1.0)
    return RealtimeBlockchainVisualizer(analyzer, data_source, update_interval=1, test_mode=True)

def test_initialization(realtime_visualizer):
    """Test visualizer initialization."""
    assert not realtime_visualizer.is_running
    assert realtime_visualizer.update_interval == 1
    assert realtime_visualizer.eth_times == []
    assert realtime_visualizer.eth_values == []
    assert realtime_visualizer.sol_times == []
    assert realtime_visualizer.sol_values == []

def test_start_stop_streaming(realtime_visualizer):
    """Test starting and stopping data streaming."""
    # Start streaming with a short duration
    realtime_visualizer.start_streaming(duration=2)
    assert realtime_visualizer.is_running
    
    # Wait for streaming to complete
    time.sleep(3)
    assert not realtime_visualizer.is_running

def test_data_collection(realtime_visualizer):
    """Test data collection functionality."""
    # Start streaming
    realtime_visualizer.start_streaming(duration=2)
    time.sleep(3)  # Wait for data collection
    
    # Check that data was collected
    assert len(realtime_visualizer.eth_times) > 0
    assert len(realtime_visualizer.eth_values) > 0
    assert len(realtime_visualizer.sol_times) > 0
    assert len(realtime_visualizer.sol_values) > 0

def test_save_snapshot(realtime_visualizer, tmp_path):
    """Test saving visualization snapshot."""
    # Start streaming
    realtime_visualizer.start_streaming(duration=2)
    time.sleep(3)  # Wait for data collection
    
    # Save snapshot
    save_path = tmp_path / "snapshot.png"
    realtime_visualizer.save_snapshot(str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_alert_system(realtime_visualizer):
    """Test alert system functionality."""
    alert_triggered = False
    
    def alert_callback(data):
        nonlocal alert_triggered
        alert_triggered = True
    
    # Add alert for high TPS
    realtime_visualizer.add_alert('ethereum_tps', 0.1, alert_callback)
    
    # Start streaming
    realtime_visualizer.start_streaming(duration=2)
    time.sleep(3)  # Wait for data collection
    
    # Check if alert was triggered
    assert alert_triggered

def test_data_window(realtime_visualizer):
    """Test data window management."""
    # Start streaming
    realtime_visualizer.start_streaming(duration=2)
    time.sleep(3)  # Wait for data collection
    
    # Check that data points are within the last hour
    now = datetime.now()
    for t in realtime_visualizer.eth_times:
        assert (now - t).total_seconds() <= 3600

def test_error_handling(realtime_visualizer, caplog):
    """Test error handling during data collection."""
    # Create a new data source that will simulate failures
    data_source = TestBlockchainDataSource(should_fail=True)
    realtime_visualizer.data_source = data_source
    
    # Start streaming
    realtime_visualizer.start_streaming(duration=1)
    time.sleep(2)
    
    # Check that error was logged
    assert any("Error collecting data" in record.message for record in caplog.records)

def test_plot_updates(realtime_visualizer):
    """Test plot updates with new data."""
    # Start streaming
    realtime_visualizer.start_streaming(duration=2)
    time.sleep(1)
    
    # Get initial data
    initial_eth_len = len(realtime_visualizer.eth_values)
    initial_sol_len = len(realtime_visualizer.sol_values)
    
    # Wait for more data
    time.sleep(2)
    
    # Check that new data was added
    assert len(realtime_visualizer.eth_values) > initial_eth_len
    assert len(realtime_visualizer.sol_values) > initial_sol_len 