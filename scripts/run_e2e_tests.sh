#!/bin/bash
set -e

# Ensure the server is running
kubectl get pods -l app.kubernetes.io/instance=llm-server | grep Running || {
    echo "LLM Server is not running"
    exit 1
}

# Wait for service to be ready
echo "Waiting for service to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=llm-server --timeout=60s

# Run the tests
echo "Running e2e tests..."
pytest tests/e2e/test_llm_server.py -v --capture=no

# Print test summary
echo "Test run complete" 