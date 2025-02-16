#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "🚀 Deploying LLM Server..."

# First verify cluster is ready
echo "Verifying cluster..."
kubectl cluster-info || {
    echo -e "${RED}Cluster is not ready${NC}"
    exit 1
}

# Remove any existing deployment
echo "Cleaning up any existing deployment..."
helm uninstall llm-server --namespace default || true
kubectl delete pods,services -l app.kubernetes.io/instance=llm-server --namespace default || true

# Deploy the helm chart
echo "Deploying Helm chart..."
helm install llm-server ./helm/llm-server \
  --namespace default \
  --set replicaCount=1 \
  --create-namespace || {
    echo -e "${RED}Failed to deploy helm chart${NC}"
    kubectl get pods -A
    exit 1
}

# Wait for pod to be ready
echo "Waiting for pod to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=llm-server --timeout=60s || {
    echo -e "${RED}Pod failed to become ready${NC}"
    kubectl describe pods -l app.kubernetes.io/instance=llm-server
    exit 1
}

echo -e "${GREEN}✅ Deployment successful${NC}"

# Simple test
echo "🧪 Testing service..."
SERVICE_IP=$(kubectl get service llm-server -o jsonpath='{.spec.clusterIP}')
kubectl run curl-test --image=curlimages/curl --rm -i --restart=Never -- curl -s "http://${SERVICE_IP}:8000" || {
    echo -e "${RED}Service test failed${NC}"
    exit 1
}

echo -e "${GREEN}✅ All tests passed${NC}"
