.PHONY: test test-all test-connectors test-connections test-backup test-quality

# Install dependencies
install:
	pip install -r requirements.txt

# Test commands
test-all: test-connectors test-connections test-backup test-quality

test-connectors:
	PYTHONPATH=. pytest src/tests/test_connectors.py -v

test-connections:
	PYTHONPATH=. python src/tests/verify_connections.py

test-backup:
	PYTHONPATH=. pytest src/tests/test_backup.py -v

test-quality:
	PYTHONPATH=. pytest src/test_quality_system.py -v

# Development commands
lint:
	flake8 src/
	black src/ --check

format:
	black src/
	isort src/

# Clean up
clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name "*.egg" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".coverage" -exec rm -r {} + 