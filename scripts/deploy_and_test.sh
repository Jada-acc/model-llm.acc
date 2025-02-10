#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "🚀 Deploying LLM Server..."

# Deploy/upgrade the helm chart
echo "Deploying Helm chart..."
helm upgrade --install llm-server ./helm/llm-server \
  --namespace default \
  --set monitoring.enabled=false \
  --set service.type=ClusterIP \
  --set replicaCount=1 \
  --atomic \
  --timeout 3m || {
    echo -e "${RED}Failed to deploy helm chart${NC}"
    kubectl get pods --all-namespaces
    kubectl describe deployment llm-server
    exit 1
}

# Wait for deployment to be ready
echo "Waiting for deployment to be ready..."
kubectl rollout status deployment/llm-server --timeout=2m || {
    echo -e "${RED}Deployment failed to roll out${NC}"
    kubectl get pods
    kubectl describe deployment llm-server
    exit 1
}

# Test the service
echo "Testing service..."
SERVICE_IP=$(kubectl get service llm-server -o jsonpath='{.spec.clusterIP}')
PORT=8000

echo "Service endpoint: http://${SERVICE_IP}:${PORT}"

# Wait for service to be ready
echo "Waiting for service to be ready..."
for i in {1..30}; do
    if kubectl run curl-test --image=curlimages/curl --rm -i --restart=Never -- curl -s "http://${SERVICE_IP}:${PORT}"; then
        echo -e "${GREEN}✅ Service is responding${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Service failed to respond${NC}"
        kubectl get pods
        kubectl get svc
        exit 1
    fi
    echo "Waiting for service to respond... ($i/30)"
    sleep 2
done

echo -e "${GREEN}✅ Deployment successful${NC}"

echo "🧪 Running tests..."
pytest tests/e2e/test_llm_server.py -v || {
    echo -e "${RED}Tests failed${NC}"
    exit 1
}

echo -e "${GREEN}✅ All tests passed${NC}" 