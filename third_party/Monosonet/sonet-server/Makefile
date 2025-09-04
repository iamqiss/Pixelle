# Gateway specific shortcuts
.PHONY: gateway
gateway:
	cmake -S . -B build -DBUILD_OTHER_SERVICES=ON
	cmake --build build --target sonet_gateway -j $$(nproc)

.PHONY: gateway-run
gateway-run: gateway
	./build/src/gateway/sonet_gateway
#
# Copyright (c) 2025 Neo Qiss
# All rights reserved.
# 
# This software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.
#

# Sonet Makefile for development

.PHONY: help build clean test install deps up down logs shell format lint

# Default target
help:
	@echo "Sonet Development Commands:"
	@echo "  build      - Build the project using Conan and CMake"
	@echo "  clean      - Clean build artifacts"
	@echo "  test       - Run all tests"
	@echo "  install    - Install dependencies"
	@echo "  deps       - Install Conan dependencies"
	@echo "  up         - Start all services with Docker Compose"
	@echo "  down       - Stop all services"
	@echo "  logs       - Show logs from all services"
	@echo "  shell      - Open shell in gateway container"
	@echo "  format     - Format code using clang-format"
	@echo "  lint       - Run static analysis"
	@echo "  migrate    - Run database migrations"
	@echo "  seed       - Seed database with test data"

# Build configuration
BUILD_TYPE ?= Debug
BUILD_DIR = build
CONAN_PROFILE = default

# Install system dependencies
install:
	@echo "Installing system dependencies..."
	sudo apt-get update
	sudo apt-get install -y build-essential cmake git curl python3 python3-pip pkg-config
	sudo apt-get install -y libssl-dev libpq-dev postgresql-client redis-tools
	sudo apt-get install -y clang-format cppcheck valgrind
	pip3 install conan==2.0.17 --user
	conan profile detect --force

# Install Conan dependencies
deps:
	@echo "Installing Conan dependencies..."
	mkdir -p $(BUILD_DIR)
	conan install . --output-folder=$(BUILD_DIR) --build=missing -s build_type=$(BUILD_TYPE)

# Build the project
build: deps
	@echo "Building Sonet..."
	cd $(BUILD_DIR) && \
	cmake .. -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) && \
	cmake --build . -j$$(nproc)

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	docker-compose down --volumes --remove-orphans
	docker system prune -f

# Run tests
test: build
	@echo "Running tests..."
	cd $(BUILD_DIR) && ctest --output-on-failure

# Format code
format:
	@echo "Formatting code..."
	find src tests -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs clang-format -i

# Run static analysis
lint:
	@echo "Running static analysis..."
	find src -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs cppcheck --enable=all --std=c++20

# Docker Compose commands
up:
	@echo "Starting all services..."
	docker-compose up -d

down:
	@echo "Stopping all services..."
	docker-compose down

logs:
	@echo "Showing service logs..."
	docker-compose logs -f

shell:
	@echo "Opening shell in gateway container..."
	docker-compose exec gateway /bin/bash

# Database operations
migrate:
	@echo "Running database migrations..."
	./scripts/database/migrate.sh

seed:
	@echo "Seeding database..."
	./scripts/database/seed.sh

# Development workflow
dev: clean build test
	@echo "Development build complete!"

# Production build
prod:
	$(MAKE) build BUILD_TYPE=Release

# Quick start for new developers
setup: install deps build migrate seed
	@echo "Development environment setup complete!"
	@echo "Run 'make up' to start all services"
	@echo "Services will be available at:"
	@echo "  API Gateway: http://localhost:8080"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"
	@echo "  Jaeger: http://localhost:16686"
	@echo "  Prometheus: http://localhost:9091"
