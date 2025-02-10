#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "🚀 Deploying LLM Server..."

# Debug info
echo "Current CRDs:"
kubectl get crd | grep monitoring.coreos.com

# Verify Prometheus CRDs are available
echo "Verifying Prometheus CRDs..."
if ! kubectl get crd prometheusrules.monitoring.coreos.com &>/dev/null; then
    echo "PrometheusRules CRD not found. Checking monitoring namespace..."
    kubectl get all -n monitoring
    exit 1
fi

# Deploy/upgrade the helm chart
echo "Deploying Helm chart..."
helm upgrade --install llm-server ./helm/llm-server \
  --namespace default \
  --set monitoring.enabled=true \
  --set monitoring.prometheusOperator.enabled=true \
  --wait --timeout 5m || {
    echo -e "${RED}Failed to deploy helm chart${NC}"
    kubectl get pods --all-namespaces
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