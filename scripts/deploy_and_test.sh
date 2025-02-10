#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "🚀 Deploying LLM Server..."

# Deploy/upgrade the helm chart
helm upgrade --install llm-server ./helm/llm-server || {
    echo -e "${RED}Failed to deploy helm chart${NC}"
    exit 1
}

echo "⏳ Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=llm-server --timeout=60s || {
    echo -e "${RED}Pods failed to become ready${NC}"
    exit 1
}

echo -e "${GREEN}✅ Deployment successful${NC}"

echo "🧪 Running tests..."
pytest tests/e2e/test_llm_server.py -v || {
    echo -e "${RED}Tests failed${NC}"
    exit 1
}

echo -e "${GREEN}✅ All tests passed${NC}" 