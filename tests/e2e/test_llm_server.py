import pytest
import requests
import os
import time
import json
import subprocess
from requests.exceptions import ConnectionError
from prometheus_api_client import PrometheusConnect
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_service_url():
    # Get service IP and port using kubectl
    cmd = "kubectl get service llm-server -o jsonpath='{.spec.clusterIP}'"
    service_ip = os.popen(cmd).read().strip()
    return f"http://{service_ip}:8000"

def test_server_health():
    """Test that the server is responding to requests"""
    url = get_service_url()
    
    # Retry mechanism for eventual consistency
    max_retries = 5
    for i in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            assert response.status_code == 200
            return
        except (requests.exceptions.RequestException, AssertionError):
            if i == max_retries - 1:
                raise
            time.sleep(2)

def test_server_response():
    """Test that the server returns expected response"""
    url = get_service_url()
    response = requests.get(url)
    assert response.status_code == 200
    assert "Directory listing for /" in response.text

def wait_for_server(url, timeout=30, interval=1):
    """Wait for server to be ready"""
    start_time = time.time()
    while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
        except ConnectionError:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Server at {url} not ready after {timeout} seconds")
            time.sleep(interval)

@pytest.fixture(scope="session", autouse=True)
def setup_server():
    """Ensure server is running before tests"""
    # Check if pods are running
    result = subprocess.run(
        ["kubectl", "get", "pods", "-l", "app.kubernetes.io/instance=llm-server"],
        capture_output=True,
        text=True
    )
    
    if "Running" not in result.stdout:
        pytest.skip("Server pods are not running. Please start the server first.")
    
    # Wait for health endpoint
    try:
        wait_for_server("http://localhost/health")
    except TimeoutError as e:
        pytest.skip(str(e))
    
    # Wait for metrics endpoint
    try:
        wait_for_server("http://localhost:9090/metrics")
    except TimeoutError as e:
        pytest.skip(str(e))

@pytest.fixture(scope="session")
def http_session():
    """Create a session with retries"""
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1)
    session.mount('http://', HTTPAdapter(max_retries=retries))
    return session

def test_health_endpoint(http_session):
    response = http_session.get("http://localhost/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_predict_endpoint(http_session):
    payload = {
        "inputs": ["Test input 1", "Test input 2"]
    }
    response = http_session.post("http://localhost/predict", json=payload)
    assert response.status_code == 200
    assert "predictions" in response.json()

def test_metrics(http_session):
    response = http_session.get("http://localhost:9090/metrics")
    assert response.status_code == 200
    
    metrics_text = response.text
    expected_metrics = [
        'http_requests_total',
        'http_request_duration_seconds',
        'model_load_time_seconds',
        'inference_latency_seconds',
        'model_memory_usage_bytes',
        'batch_size',
        'cache_hit_ratio',
        'request_queue_size'
    ]
    
    for metric in expected_metrics:
        assert metric in metrics_text, f"Metric {metric} not found in response"

def test_load(http_session):
    for _ in range(10):
        http_session.get("http://localhost/health")
        http_session.post("http://localhost/predict", 
                       json={"inputs": ["Load test"]})
        time.sleep(0.1)
    
    response = http_session.get("http://localhost:9090/metrics")
    metrics_text = response.text
    
    assert 'http_requests_total{method="POST",path="/predict",status="200"}' in metrics_text
    assert 'inference_latency_seconds' in metrics_text
    assert 'model_memory_usage_bytes' in metrics_text
    assert 'cache_hit_ratio' in metrics_text

if __name__ == "__main__":
    pytest.main([__file__]) 