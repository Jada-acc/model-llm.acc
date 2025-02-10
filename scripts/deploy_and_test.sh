#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "🚀 Deploying LLM Server..."

# Wait for Prometheus CRDs to be ready
echo "Waiting for Prometheus CRDs..."
kubectl wait --for condition=established --timeout=120s \
  crd/prometheusrules.monitoring.coreos.com \
  crd/servicemonitors.monitoring.coreos.com || true

# Deploy/upgrade the helm chart
echo "Deploying Helm chart..."
helm upgrade --install llm-server ./helm/llm-server \
  --namespace default \
  --set monitoring.enabled=true \
  --set monitoring.prometheusOperator.enabled=true \
  --wait || {
    echo -e "${RED}Failed to deploy helm chart${NC}"
    kubectl get pods
    kubectl describe pod -l app.kubernetes.io/instance=llm-server
    exit 1
}

# Wait for pods to be ready
echo "Waiting for pods..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=llm-server --timeout=60s || {
    echo -e "${RED}Pods failed to become ready${NC}"
    kubectl get pods
    kubectl describe pod -l app.kubernetes.io/instance=llm-server
    exit 1
}

echo -e "${GREEN}✅ Deployment successful${NC}"

echo "🧪 Running tests..."
pytest tests/e2e/test_llm_server.py -v || {
    echo -e "${RED}Tests failed${NC}"
    exit 1
}

echo -e "${GREEN}✅ All tests passed${NC}" 