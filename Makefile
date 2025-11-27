.PHONY: install test lint up down clean

install:
	pip install -r requirements.txt
	pip install -e .

test:
	python -m pytest tests/ scripts/test_*.py

lint:
	flake8 src scripts tests

up:
	docker-compose -f docker/docker-compose.yml up -d

down:
	docker-compose -f docker/docker-compose.yml down

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
