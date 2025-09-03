# Pixelle Backend

A microservices-based backend for the Pixelle social media platform built with Rust.

## Architecture

The backend is organized as a collection of microservices, each responsible for specific functionality:

### Core Services
- **API Gateway** (`:8080`) - Entry point for all client requests, handles routing and authentication
- **User Service** (`:8081`) - User management, profiles, and authentication
- **Feed Service** (`:8082`) - Content feed generation and trending posts
- **Content Service** (`:8083`) - Post and comment management
- **Auth Service** (`:8084`) - Authentication and authorization

### Infrastructure Services
- **Database Service** - maintableQL database management
- **Cache Service** - Redis caching layer
- **File Storage** - Media file storage and processing
- **Notification Service** - Push notifications and email

## Getting Started

### Prerequisites
- Rust 1.75+
- Docker and Docker Compose
- maintableQL (optional, for production)

### Development Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd Pixelle-Backend
```

2. Build the project:
```bash
cargo build
```

3. Run with Docker Compose:
```bash
docker-compose up --build
```

4. Or run individual services:
```bash
# API Gateway
cargo run --package pixelle-api-gateway

# User Service
cargo run --package pixelle-user-service

# Feed Service
cargo run --package pixelle-feed-service
```

## API Endpoints

### User Service (`/api/v1/users`)
- `POST /api/v1/users` - Create a new user
- `GET /api/v1/users/{id}` - Get user by ID
- `PUT /api/v1/users/{id}` - Update user
- `DELETE /api/v1/users/{id}` - Delete user
- `GET /api/v1/users/search?q={query}` - Search users

### Feed Service (`/api/v1/feed`)
- `GET /api/v1/feed/{user_id}` - Get user's feed
- `GET /api/v1/feed/trending` - Get trending posts

### Health Checks
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics

## Development

### Project Structure
```
Pixelle-Backend/
├── crates/                 # Shared libraries
│   ├── pixelle-core/      # Core types and traits
│   ├── pixelle-auth/      # Authentication utilities
│   ├── pixelle-database/  # Database abstractions
│   └── pixelle-monitoring/ # Monitoring and metrics
├── services/              # Microservices
│   ├── api-gateway/       # API Gateway service
│   ├── user-service/      # User management service
│   ├── feed-service/      # Feed generation service
│   └── ...               # Other services
└── tools/                # Development tools
```

### Adding a New Service

1. Create a new service directory in `services/`
2. Add the service to `Cargo.toml` workspace members
3. Implement the service with Actix Web
4. Add Docker configuration
5. Update the API Gateway routing

### Testing
```bash
# Run all tests
cargo test

# Run tests for specific service
cargo test --package pixelle-user-service

# Run integration tests
cargo test --test integration
```

## Deployment

### Docker
```bash
# Build all services
docker-compose build

# Run in production mode
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### Kubernetes
```bash
# Apply Kubernetes manifests
kubectl apply -f k8s/
```

## Monitoring

The backend includes comprehensive monitoring:

- **Metrics**: Prometheus metrics for all services
- **Tracing**: Distributed tracing with OpenTelemetry
- **Logging**: Structured logging with tracing
- **Health Checks**: Health endpoints for all services

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.
