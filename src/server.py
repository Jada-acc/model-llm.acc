from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import signal
import sys
import time
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# Prometheus metrics
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['path', 'method', 'status'])
REQUEST_LATENCY = Histogram('http_request_duration_seconds', 'HTTP request latency')

# Custom metrics
MODEL_LOAD_TIME = Histogram('model_load_time_seconds', 'Time to load model')
MODEL_MEMORY_USAGE = Gauge('model_memory_usage_bytes', 'Current model memory usage')
INFERENCE_LATENCY = Histogram('inference_latency_seconds', 'Model inference latency')
CACHE_HIT_RATIO = Gauge('cache_hit_ratio', 'Cache hit ratio')
QUEUE_SIZE = Gauge('request_queue_size', 'Number of requests in queue')

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        start_time = time.time()
        try:
            if self.path == '/health':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {'status': 'healthy'}
                self.wfile.write(json.dumps(response).encode())
                REQUEST_COUNT.labels(path='/health', method='GET', status='200').inc()
            else:
                self.send_response(404)
                self.end_headers()
                REQUEST_COUNT.labels(path=self.path, method='GET', status='404').inc()
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            REQUEST_COUNT.labels(path=self.path, method='GET', status='500').inc()
        finally:
            REQUEST_LATENCY.observe(time.time() - start_time)

    def do_POST(self):
        start_time = time.time()
        try:
            if self.path == '/predict':
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                request = json.loads(post_data.decode())

                # Record queue size
                QUEUE_SIZE.inc()

                # Simulate model inference
                inference_start = time.time()
                time.sleep(0.1)  # Simulate processing
                INFERENCE_LATENCY.observe(time.time() - inference_start)

                # Update metrics
                MODEL_MEMORY_USAGE.set(1000000)  # Example memory usage
                CACHE_HIT_RATIO.set(0.8)  # Example cache hit ratio
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    'prediction': 'example_output',
                    'processing_time': time.time() - start_time
                }
                self.wfile.write(json.dumps(response).encode())
                REQUEST_COUNT.labels(path='/predict', method='POST', status='200').inc()

                # Request completed, decrease queue
                QUEUE_SIZE.dec()
            else:
                self.send_response(404)
                self.end_headers()
                REQUEST_COUNT.labels(path=self.path, method='POST', status='404').inc()
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            REQUEST_COUNT.labels(path=self.path, method='POST', status='500').inc()
        finally:
            REQUEST_LATENCY.observe(time.time() - start_time)

def signal_handler(signum, frame):
    print('Received shutdown signal, exiting...')
    sys.exit(0)

def main():
    # Register signal handler
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start Prometheus metrics server
    start_http_server(9090)
    print('Metrics server running on port 9090')
    
    # Initialize metrics
    MODEL_MEMORY_USAGE.set(0)
    CACHE_HIT_RATIO.set(0)
    QUEUE_SIZE.set(0)
    
    # Start main server
    server_address = ('', 8000)
    httpd = HTTPServer(server_address, Handler)
    print('Server running on port 8000')
    httpd.serve_forever()

if __name__ == '__main__':
    main() 