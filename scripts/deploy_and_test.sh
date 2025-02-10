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

# Deploy/upgrade the helm chart
echo "Deploying Helm chart..."
helm upgrade --install llm-server ./helm/llm-server \
  --namespace default \
  --set monitoring.enabled=false \
  --set service.type=ClusterIP \
  --set replicaCount=1 \
  --wait \
  --timeout 5m || {
    echo -e "${RED}Failed to deploy helm chart${NC}"
    kubectl get pods --all-namespaces
    kubectl describe pods -l app.kubernetes.io/instance=llm-server
    exit 1
}

# Wait for deployment to be ready
echo "Waiting for deployment to be ready..."
kubectl wait --for=condition=available deployment/llm-server --timeout=2m || {
    echo -e "${RED}Deployment failed to be available${NC}"
    kubectl get pods
    kubectl describe deployment llm-server
    exit 1
}

# Test the service
echo "Testing service..."
SERVICE_IP=$(kubectl get service llm-server -o jsonpath='{.spec.clusterIP}')
PORT=8000

echo "Service endpoint: http://${SERVICE_IP}:${PORT}"

# Create a test pod to verify service
echo "Creating test pod..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: curl-test
  labels:
    app: curl-test
spec:
  containers:
  - name: curl
    image: curlimages/curl
    command: ["sleep", "300"]
EOF

# Wait for test pod to be ready
kubectl wait --for=condition=ready pod -l app=curl-test --timeout=60s || {
    echo -e "${RED}Test pod failed to start${NC}"
    kubectl describe pod curl-test
    exit 1
}

# Test the service using the test pod
echo "Testing service connection..."
for i in {1..30}; do
    if kubectl exec curl-test -- curl -s "http://${SERVICE_IP}:${PORT}"; then
        echo -e "${GREEN}✅ Service is responding${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Service failed to respond${NC}"
        kubectl get pods
        kubectl get svc
        kubectl logs -l app.kubernetes.io/instance=llm-server
        exit 1
    fi
    echo "Waiting for service to respond... ($i/30)"
    sleep 2
done

# Cleanup test pod
kubectl delete pod curl-test --wait=false

echo -e "${GREEN}✅ Deployment successful${NC}"

echo "🧪 Running tests..."
pytest tests/e2e/test_llm_server.py -v || {
    echo -e "${RED}Tests failed${NC}"
    kubectl logs -l app.kubernetes.io/instance=llm-server
    exit 1
}

echo -e "${GREEN}✅ All tests passed${NC}" 