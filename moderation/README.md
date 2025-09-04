# Sonet Moderation Service

A production-grade, high-throughput content moderation service built in Rust with support for multilingual content, ML inference, real-time signals processing, and comprehensive observability.

## 🚀 Production Features

### 🌍 Multilingual Moderation
- **Language Detection**: Automatic detection of 30+ languages using `whatlang`
- **Language-Specific Processing**: Optimized text preprocessing for CJK, RTL, and Latin languages
- **Multi-language Models**: Support for language-specific ML models
- **Unicode Support**: Full support for international character sets

### 🔍 ML Inference at Scale
- **Model Management**: Dynamic loading and versioning of ML models
- **Batch Processing**: Efficient batch inference for high throughput
- **GPU Acceleration**: Optional GPU support for faster inference
- **Model Health Monitoring**: Automatic health checks and failover
- **Caching**: Intelligent result caching with LRU eviction

### 📡 Signals and Pipelines
- **Real-time Processing**: Asynchronous signal processing with configurable pipelines
- **Rule Engine**: Flexible rule-based content analysis
- **Pipeline Stages**: Configurable processing stages (aggregation, ML enhancement, user scoring)
- **Signal Aggregation**: Intelligent grouping and correlation of signals
- **Auto-escalation**: Automatic escalation based on signal severity

### 📊 User Reports & Investigations
- **Comprehensive Reporting**: Rich report structure with evidence and metadata
- **Investigation Workflow**: Full investigation lifecycle management
- **Specialist Assignment**: Automatic and manual specialist assignment
- **Priority Management**: Intelligent priority calculation based on content type
- **Evidence Management**: Support for text, images, links, and screenshots

### 💾 Storage and Throughput
- **Hybrid Storage**: PostgreSQL + Redis + Sled for optimal performance
- **Connection Pooling**: Configurable database connection pools
- **Caching Strategy**: Multi-level caching for frequently accessed data
- **Compression**: Optional content compression for storage efficiency
- **Batch Operations**: Efficient batch processing for high-volume operations

### 🔒 Reliability & Observability
- **Distributed Tracing**: Jaeger integration for request tracing
- **Metrics Collection**: Prometheus metrics with custom business metrics
- **Health Checks**: Comprehensive health monitoring for all components
- **Alerting**: Configurable alerting with multiple severity levels
- **Structured Logging**: JSON logging with correlation IDs

### 👥 Abuse Specialists
- **Workload Management**: Intelligent workload distribution
- **Expertise Matching**: Specialist assignment based on content type
- **Performance Tracking**: Specialist performance metrics and analytics
- **Escalation Paths**: Clear escalation workflows for complex cases

### 🛠️ Operations & Monitoring
- **Auto-scaling**: Kubernetes-ready with horizontal pod autoscaling
- **Performance Monitoring**: Real-time performance metrics and alerts
- **Backup & Recovery**: Automated backup strategies
- **Deployment**: Blue-green deployment support
- **Configuration Management**: Environment-specific configuration files

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   HTTP/gRPC     │    │   Load          │    │   Rate          │
│   API Layer     │───▶│   Balancer      │───▶│   Limiting      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Request       │    │   Language      │    │   ML Model      │
│   Router        │───▶│   Detection     │───▶│   Manager       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Production    │    │   Signal        │    │   Report        │
│   Classifier    │◀───│   Processor     │◀───│   Manager       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Metrics       │    │   Storage       │    │   Observability │
│   Collector     │    │   Layer         │    │   Layer         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Rust 1.70+
- Docker & Docker Compose
- PostgreSQL 15+
- Redis 7+

### Development Setup

1. **Clone and Build**
```bash
git clone <repository>
cd moderation
cargo build --release
```

2. **Start Infrastructure**
```bash
docker-compose up -d postgres redis jaeger prometheus grafana
```

3. **Configure Environment**
```bash
cp config/default.toml config/local.toml
# Edit local.toml with your settings
```

4. **Run the Service**
```bash
cargo run
```

### Production Deployment

1. **Build Production Image**
```bash
docker build -t sonet-moderation:latest .
```

2. **Deploy with Kubernetes**
```bash
kubectl apply -f k8s/
```

3. **Configure Production Settings**
```bash
# Set environment variables
export RUST_ENV=production
export JWT_SECRET=your-secure-secret
export DATABASE_URL=your-production-db-url
```

## 📊 Configuration

### Environment Variables
- `RUST_ENV`: Environment (development/production)
- `CONFIG_PATH`: Path to configuration files
- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string
- `JAEGER_ENDPOINT`: Jaeger tracing endpoint

### Configuration Files
- `config/default.toml`: Default configuration
- `config/local.toml`: Local development overrides
- `config/production.toml`: Production optimizations

### Key Configuration Sections
```toml
[ml]
batch_size = 128                    # ML inference batch size
gpu_enabled = true                  # Enable GPU acceleration
model_cache_size = 10000            # Model result cache size

[pipeline]
workers = 32                        # Pipeline worker threads
buffer_size = 100000               # Signal buffer size
timeout_seconds = 120              # Pipeline timeout

[observability]
jaeger_endpoint = "http://jaeger:14268/api/traces"
prometheus_enabled = true
alerting_enabled = true
```

## 🔌 API Usage

### HTTP API

#### Classify Content
```bash
curl -X POST http://localhost:8080/api/v1/classify \
  -H "Content-Type: application/json" \
  -d '{
    "content_id": "123",
    "user_id": "user_456",
    "text": "Hello world",
    "content_type": "post",
    "language_hint": "en"
  }'
```

#### Create Report
```bash
curl -X POST http://localhost:8080/api/v1/reports \
  -H "Content-Type: application/json" \
  -d '{
    "reporter_id": "user_789",
    "target_id": "user_456",
    "content_id": "123",
    "report_type": "hate_speech",
    "reason": "Contains hate speech",
    "evidence": [{"type": "text", "content": "Offensive content"}]
  }'
```

### gRPC API

```protobuf
service ModerationService {
  rpc Classify (ClassifyRequest) returns (ClassifyResponse);
  rpc ClassifyStream (stream ClassifyRequest) returns (stream ClassifyResponse);
  rpc CreateReport (CreateReportRequest) returns (CreateReportResponse);
  rpc GetReport (GetReportRequest) returns (GetReportResponse);
}
```

## 📈 Monitoring & Observability

### Metrics Endpoints
- `/metrics`: Prometheus metrics
- `/health`: Health check endpoint
- `/ready`: Readiness probe

### Key Metrics
- `moderation_requests_total`: Total requests
- `moderation_classification_duration_seconds`: Classification latency
- `moderation_ml_inference_duration_seconds`: ML inference time
- `moderation_cache_hits_total`: Cache hit rate
- `moderation_signal_total`: Signal processing volume

### Grafana Dashboards
- **Performance Dashboard**: Response times, throughput, error rates
- **ML Dashboard**: Model performance, inference times, accuracy
- **Business Dashboard**: Content types, languages, violation rates
- **Infrastructure Dashboard**: CPU, memory, database connections

### Alerting
- High error rates (>5%)
- High latency (>1s)
- Low throughput (<100 req/s)
- High resource usage (>80%)
- Model degradation

## 🔧 Development

### Adding New Languages
1. Add language code to `supported_languages` in config
2. Implement language-specific preprocessing in `LanguagePreprocessor`
3. Add language-specific ML models if needed

### Adding New ML Models
1. Implement `InferenceModel` trait
2. Add model metadata
3. Configure model loading in `MlModelManager`

### Adding New Signal Types
1. Define new `SignalType` enum variant
2. Implement signal processing logic
3. Add corresponding metrics

### Testing
```bash
# Run all tests
cargo test

# Run specific test suite
cargo test --test integration_tests

# Run with coverage
cargo tarpaulin
```

## 🚀 Performance Tuning

### High Throughput
- Increase `pipeline.workers` based on CPU cores
- Optimize `ml.batch_size` for your hardware
- Use Redis clustering for high availability
- Enable connection pooling

### Low Latency
- Enable ML model caching
- Use GPU acceleration for inference
- Optimize database queries
- Enable compression

### High Availability
- Deploy multiple instances
- Use load balancers
- Implement circuit breakers
- Enable health checks

## 🔒 Security

### Authentication
- JWT-based authentication
- Role-based access control
- API key management

### Data Protection
- Content encryption at rest
- Secure communication (TLS)
- Audit logging
- Data retention policies

### Rate Limiting
- Per-user rate limits
- Per-endpoint rate limits
- IP-based rate limiting
- Burst handling

## 📚 Documentation

- [API Reference](./docs/api.md)
- [Configuration Guide](./docs/configuration.md)
- [Deployment Guide](./docs/deployment.md)
- [Performance Tuning](./docs/performance.md)
- [Troubleshooting](./docs/troubleshooting.md)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

Apache 2.0 License - see [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/sonet/moderation/issues)
- **Discussions**: [GitHub Discussions](https://github.com/sonet/moderation/discussions)
- **Documentation**: [Wiki](https://github.com/sonet/moderation/wiki)

---

**Built with ❤️ by the Sonet Team**