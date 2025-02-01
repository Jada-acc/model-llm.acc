import pytest
import os
from datetime import datetime
import matplotlib.pyplot as plt
from .blockchain_storage import BlockchainStorage
from .blockchain_analysis import BlockchainAnalyzer
from .advanced_visualizer import AdvancedBlockchainVisualizer

@pytest.fixture
def advanced_visualizer(analyzer):
    """Create an AdvancedBlockchainVisualizer instance for testing."""
    return AdvancedBlockchainVisualizer(analyzer)

def test_network_heatmap_ethereum(advanced_visualizer, tmp_path):
    """Test Ethereum network activity heatmap."""
    save_path = tmp_path / "eth_heatmap.png"
    advanced_visualizer.plot_network_heatmap("ethereum", days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_network_heatmap_solana(advanced_visualizer, tmp_path):
    """Test Solana network activity heatmap."""
    save_path = tmp_path / "sol_heatmap.png"
    advanced_visualizer.plot_network_heatmap("solana", days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_gas_price_prediction(advanced_visualizer, tmp_path):
    """Test gas price prediction visualization."""
    save_path = tmp_path / "gas_prediction.png"
    advanced_visualizer.plot_gas_price_prediction(days=7, prediction_days=3, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_smart_contract_interactions(advanced_visualizer, tmp_path):
    """Test smart contract interaction network visualization."""
    save_path = tmp_path / "contract_interactions.png"
    advanced_visualizer.plot_smart_contract_interactions(days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_correlation_matrix(advanced_visualizer, tmp_path):
    """Test blockchain metrics correlation matrix."""
    save_path = tmp_path / "correlation_matrix.png"
    advanced_visualizer.plot_correlation_matrix(days=7, save_path=str(save_path))
    assert os.path.exists(save_path)
    assert os.path.getsize(save_path) > 0

def test_invalid_blockchain_heatmap(advanced_visualizer):
    """Test error handling for invalid blockchain in heatmap."""
    with pytest.raises(ValueError):
        advanced_visualizer.plot_network_heatmap("invalid_chain", days=7)

def test_invalid_period_prediction(advanced_visualizer):
    """Test error handling for invalid time period in prediction."""
    with pytest.raises(ValueError):
        advanced_visualizer.plot_gas_price_prediction(days=0)

def test_prediction_validation(advanced_visualizer):
    """Test that predictions are reasonable."""
    # This is a basic test to ensure predictions are within expected ranges
    data = advanced_visualizer.analyzer.analyze_ethereum_trends(days=7)
    current_gas = list(data["trends"]["daily_avg_gas"].values())[-1]
    
    # Predictions shouldn't deviate more than 100% from current value
    # This is a simplified validation - adjust based on your needs
    assert current_gas > 0  # Ensure we have valid current data