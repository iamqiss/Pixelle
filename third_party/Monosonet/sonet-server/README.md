# Sonet - Social Media Microservices Platform

A high-performance, scalable social media platform built with C++ microservices architecture.

## 🚀 Quick Start

### Prerequisites

- Ubuntu 24.04 LTS (or compatible Linux distribution)
- Docker and Docker Compose
- Python 3.8+ (for Conan package manager)
- Git

### 1. Clone and Setup

```bash
git clone <repository-url>
cd Sonet/sonet
make setup
```

This will:
- Install all system dependencies
- Set up Conan package manager
- Install C++ dependencies
- Build the project
- Set up databases

### 2. Start Development Environment

```bash
make up
```

This starts all services using Docker Compose:
- **API Gateway**: http://localhost:8080
- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Jaeger Tracing**: http://localhost:16686
- **Prometheus Metrics**: http://localhost:9091

### 3. Development Workflow

```bash
# Build the project
make build

# Run tests
make test

# Format code
make format

# Run static analysis
make lint

# View logs
make logs

# Clean build artifacts
make clean
```

## 📦 Services Architecture

- **Gateway Service**: API gateway and load balancer
- **User Service**: User management and authentication
- **Note Service**: Notes and content management
- **Media Service**: File uploads and media handling
- **Follow Service**: Social connections and relationships
- **Notification Service**: Real-time notifications
- **Follow Service**: Social graph and relationships
- **Media Service**: Image and video handling
- **Search Service**: Full-text search capabilities
- **Notification Service**: Real-time notifications
- **Analytics Service**: Metrics and analytics
- **API Gateway**: Request routing and rate limiting

## Tech Stack

- **Language**: C++17/20
- **Databases**: postgresql, Redis, Cassandra, Elasticsearch
- **Communication**: gRPC, HTTP/REST, WebSockets
- **Containerization**: Docker, Kubernetes
- **Monitoring**: Prometheus, Grafana, Jaeger

## Getting Started

```bash
# Build the project
mkdir build && cd build
cmake ..
make -j$(nproc)

# Run with Docker Compose
docker-compose up -d
```

## Services

Each service runs independently and communicates via gRPC and HTTP APIs.

