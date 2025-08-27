.PHONY: help install test test-coverage lint format clean docker-build docker-up-gpu docker-up-cpu docker-down logs migrate dev

# Default target
help:
	@echo "Restaurant Opening Leads MVP - Available Commands"
	@echo "=================================================="
	@echo ""
	@echo "🔧 Development:"
	@echo "  install          Install dependencies and setup development environment"
	@echo "  dev              Start development server with auto-reload"
	@echo "  migrate          Run database migrations"
	@echo "  shell            Start interactive Python shell with app context"
	@echo ""
	@echo "🧪 Testing:"
	@echo "  test             Run all tests"
	@echo "  test-unit        Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-coverage    Run tests with coverage report"
	@echo "  test-rules       Run rules engine tests"
	@echo "  test-agents      Run agent tests"
	@echo "  test-ingestion   Run data ingestion tests"
	@echo ""
	@echo "📝 Code Quality:"
	@echo "  lint             Run linting (flake8, mypy)"
	@echo "  format           Format code (black, isort)"
	@echo "  format-check     Check code formatting without changes"
	@echo "  type-check       Run type checking only"
	@echo ""
	@echo "🐳 Docker:"
	@echo "  docker-build-gpu Build GPU Docker image"
	@echo "  docker-build-cpu Build CPU Docker image"
	@echo "  docker-up-gpu    Start GPU services (vLLM + API)"
	@echo "  docker-up-cpu    Start CPU services (Ollama + API)"
	@echo "  docker-down      Stop all services"
	@echo "  docker-clean     Clean up containers and images"
	@echo ""
	@echo "📊 Monitoring:"
	@echo "  logs             View all container logs"
	@echo "  logs-api         View API container logs"
	@echo "  logs-vllm        View vLLM container logs"
	@echo "  logs-ollama      View Ollama container logs"
	@echo "  health           Check service health"
	@echo ""
	@echo "🚀 Pipeline:"
	@echo "  run-pipeline     Run pipeline manually"
	@echo "  run-test-pipeline Run test pipeline with mock data"
	@echo "  backup-db        Backup SQLite database"
	@echo "  clean-data       Clean up old data and logs"

# Development Environment
install:
	@echo "🔧 Installing development environment..."
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	playwright install
	@echo "✅ Installation complete!"

dev:
	@echo "🚀 Starting development server..."
	uvicorn app.main:app --reload --host 0.0.0.0 --port 8080

migrate:
	@echo "🗃️ Running database migrations..."
	python -c "from app.db import init_db; init_db()"
	@echo "✅ Database initialized!"

shell:
	@echo "🐍 Starting Python shell..."
	python -c "from app.main import app; from app.db import get_session; import IPython; IPython.start_ipython(argv=[], user_ns={'app': app, 'db': get_session})"

# Testing
test:
	@echo "🧪 Running all tests..."
	pytest app/tests/ -v --tb=short

test-unit:
	@echo "🧪 Running unit tests..."
	pytest app/tests/test_rules.py app/tests/test_matching.py -v

test-integration:
	@echo "🧪 Running integration tests..."
	pytest app/tests/test_ingestion.py app/tests/test_agents.py -v

test-coverage:
	@echo "🧪 Running tests with coverage..."
	pytest app/tests/ --cov=app --cov-report=html --cov-report=term-missing
	@echo "📊 Coverage report generated in htmlcov/"

test-rules:
	@echo "🧪 Running rules engine tests..."
	pytest app/tests/test_rules.py -v

test-agents:
	@echo "🧪 Running agent tests..."
	pytest app/tests/test_agents.py -v

test-ingestion:
	@echo "🧪 Running data ingestion tests..."
	pytest app/tests/test_ingestion.py -v

# Code Quality
lint:
	@echo "📝 Running linters..."
	flake8 app/ --max-line-length=100 --ignore=E203,W503
	mypy app/ --ignore-missing-imports --no-strict-optional
	@echo "✅ Linting complete!"

format:
	@echo "📝 Formatting code..."
	black app/ --line-length=100
	isort app/ --profile=black --line-length=100
	@echo "✅ Formatting complete!"

format-check:
	@echo "📝 Checking code formatting..."
	black app/ --line-length=100 --check --diff
	isort app/ --profile=black --line-length=100 --check --diff

type-check:
	@echo "📝 Running type checking..."
	mypy app/ --ignore-missing-imports --no-strict-optional

# Docker Commands
docker-build-gpu:
	@echo "🐳 Building GPU Docker image..."
	docker build -f docker/Dockerfile.gpu -t restaurant-leads:gpu .

docker-build-cpu:
	@echo "🐳 Building CPU Docker image..."
	docker build -f docker/Dockerfile.cpu -t restaurant-leads:cpu .

docker-up-gpu:
	@echo "🐳 Starting GPU services (vLLM + API)..."
	docker-compose -f infra/docker-compose.yml up -d vllm api worker
	@echo "⏳ Waiting for model server to load (~2-3 minutes)..."
	@echo "💡 Monitor progress with: make logs-vllm"

docker-up-cpu:
	@echo "🐳 Starting CPU services (Ollama + API)..."
	docker-compose -f infra/docker-compose.yml up -d ollama api-cpu worker-cpu
	@echo "⏳ Pulling model (may take a few minutes)..."
	docker exec restaurant-leads-ollama ollama pull llama2:7b
	@echo "💡 Monitor with: make logs-ollama"

docker-down:
	@echo "🐳 Stopping all services..."
	docker-compose -f infra/docker-compose.yml down

docker-clean:
	@echo "🐳 Cleaning up Docker resources..."
	docker-compose -f infra/docker-compose.yml down -v
	docker system prune -f
	@echo "✅ Docker cleanup complete!"

# Monitoring
logs:
	@echo "📊 Viewing all container logs..."
	docker-compose -f infra/docker-compose.yml logs -f

logs-api:
	@echo "📊 Viewing API container logs..."
	docker-compose -f infra/docker-compose.yml logs -f api

logs-vllm:
	@echo "📊 Viewing vLLM container logs..."
	docker-compose -f infra/docker-compose.yml logs -f vllm

logs-ollama:
	@echo "📊 Viewing Ollama container logs..."
	docker-compose -f infra/docker-compose.yml logs -f ollama

health:
	@echo "🏥 Checking service health..."
	@echo "API Health:"
	@curl -s http://localhost:8080/health | python -m json.tool || echo "❌ API not responding"
	@echo ""
	@echo "Model Server Health:"
	@curl -s http://localhost:8000/v1/models | python -m json.tool || echo "❌ Model server not responding"

# Pipeline Operations
run-pipeline:
	@echo "🚀 Running pipeline manually..."
	curl -X POST http://localhost:8080/run_pipeline \
		-H "Content-Type: application/json" \
		-d '{"county": "Harris", "days_ahead": 90}' | python -m json.tool

run-test-pipeline:
	@echo "🧪 Running test pipeline..."
	curl -X POST http://localhost:8080/test_pipeline \
		-H "Content-Type: application/json" \
		-d '{"mock_data": true}' | python -m json.tool

backup-db:
	@echo "💾 Backing up database..."
	@mkdir -p backups
	cp leads.db backups/leads_$(shell date +%Y%m%d_%H%M%S).db
	@echo "✅ Database backed up to backups/"

clean-data:
	@echo "🧹 Cleaning up old data and logs..."
	find . -name "*.log" -mtime +7 -delete
	find . -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleanup complete!"

# Environment Setup
setup-dev:
	@echo "🏗️ Setting up development environment..."
	cp .env.example .env
	@echo "📝 Please edit .env file with your configuration"
	make install
	make migrate
	@echo "✅ Development environment ready!"

setup-prod:
	@echo "🏗️ Setting up production environment..."
	@echo "🔒 Ensure .env.production is configured"
	make docker-build-gpu
	@echo "✅ Production build ready!"

# Quick Development Workflow
quick-test:
	@echo "⚡ Quick test run (rules + matching only)..."
	pytest app/tests/test_rules.py app/tests/test_matching.py -v --tb=short

quick-lint:
	@echo "⚡ Quick lint check..."
	flake8 app/ --max-line-length=100 --ignore=E203,W503 --statistics

quick-dev: quick-lint quick-test
	@echo "⚡ Quick dev check complete!"

# Documentation
docs-serve:
	@echo "📚 Starting documentation server..."
	@echo "README: file://$(PWD)/README.md"
	@command -v mdcat >/dev/null 2>&1 && mdcat README.md || cat README.md

# Release Management
version:
	@echo "📋 Current version info:"
	@echo "Git commit: $(shell git rev-parse --short HEAD 2>/dev/null || echo 'no-git')"
	@echo "Git branch: $(shell git branch --show-current 2>/dev/null || echo 'no-git')"
	@echo "Build date: $(shell date -u +%Y-%m-%dT%H:%M:%SZ)"

tag-release:
	@echo "🏷️ Creating release tag..."
	@read -p "Enter version (e.g., v1.0.0): " version; \
	git tag -a $$version -m "Release $$version"; \
	echo "✅ Tagged $$version - push with: git push origin $$version"

# Performance Testing
load-test:
	@echo "⚡ Running basic load test..."
	@command -v ab >/dev/null 2>&1 && \
		ab -n 100 -c 10 http://localhost:8080/health || \
		echo "❌ Apache Bench (ab) not installed"

benchmark:
	@echo "⏱️ Running pipeline benchmark..."
	time curl -X POST http://localhost:8080/test_pipeline \
		-H "Content-Type: application/json" \
		-d '{"mock_data": true, "candidates_count": 100}'

# Database Operations
db-shell:
	@echo "🗃️ Starting database shell..."
	sqlite3 leads.db

db-inspect:
	@echo "🔍 Inspecting database..."
	sqlite3 leads.db ".tables"
	sqlite3 leads.db ".schema candidates"

db-reset:
	@echo "⚠️ Resetting database (ALL DATA WILL BE LOST)..."
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	rm -f leads.db
	make migrate
	@echo "✅ Database reset complete!"

# Utility Commands
check-deps:
	@echo "🔍 Checking dependencies..."
	pip check
	@echo "✅ Dependencies OK!"

update-deps:
	@echo "📦 Updating dependencies..."
	pip install --upgrade pip
	pip install --upgrade -r requirements.txt
	@echo "✅ Dependencies updated!"

clean: clean-data
	@echo "🧹 Deep cleaning..."
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	@echo "✅ Deep clean complete!"

# Environment Info
info:
	@echo "ℹ️ Environment Information:"
	@echo "Python: $(shell python --version)"
	@echo "Pip: $(shell pip --version)"
	@echo "Docker: $(shell docker --version 2>/dev/null || echo 'Not installed')"
	@echo "Docker Compose: $(shell docker-compose --version 2>/dev/null || echo 'Not installed')"
	@echo "Git: $(shell git --version 2>/dev/null || echo 'Not installed')"
	@echo "Current directory: $(PWD)"
	@echo "Virtual environment: $(shell echo $$VIRTUAL_ENV)"
