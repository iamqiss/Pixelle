use moderation::api::routes::create_router;
use moderation::core::{
    classifier::{ProductionClassifier, ClassifierConfig, ContentClassifier},
    languages::LanguageDetector,
    ml_models::{MlModelManager, ModelManagerConfig},
    signals::{SignalProcessor, SignalProcessorConfig},
    observability::{MetricsCollector, MetricsConfig},
    reports::ReportManager,
    compliance::ComplianceManager,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use axum::Router;
use tower_http::compression::CompressionLayer;
use tower_http::decompression::DecompressionLayer;
use tower_http::trace::TraceLayer;
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use moderation::config::AppConfig;
use moderation::storage::db::Datastores;
use axum_prometheus::PrometheusMetricLayer;
use moderation::api::middleware::RateLimitLayer;
use tonic::transport::Server as TonicServer;
use moderation::api::grpc::{ModerationGrpcService, moderation_v1::moderation_service_server::ModerationServiceServer};
use tokio::sync::broadcast;

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install signal handler");
        term.recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with enhanced configuration
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "moderation=info,tower_http=info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();

    tracing::info!("Starting Sonet Moderation Service");

    // Load configuration
    let cfg = AppConfig::load().expect("config load failed");
    tracing::info!("Configuration loaded successfully");

    // Initialize metrics collector
    let metrics_config = MetricsConfig {
        enable_prometheus: cfg.prometheus_enabled,
        enable_jaeger: cfg.jaeger_endpoint.is_some(),
        enable_structured_logging: true,
        metrics_export_interval: cfg.metrics_export_interval,
        alerting_enabled: cfg.alerting_enabled,
        alert_thresholds: Default::default(),
    };
    
    let metrics = Arc::new(MetricsCollector::new(metrics_config)?);
    metrics.start().await?;
    tracing::info!("Metrics collector started");

    // Initialize language detector
    let language_detector = Arc::new(LanguageDetector::new(
        cfg.supported_languages.clone(),
        cfg.default_language.clone(),
    ));
    tracing::info!("Language detector initialized with {} supported languages", cfg.supported_languages.len());

    // Initialize ML model manager
    let ml_config = ModelManagerConfig {
        max_concurrent_inferences: cfg.ml_batch_size,
        model_cache_size: cfg.ml_model_cache_size,
        batch_size: cfg.ml_batch_size,
        gpu_enabled: cfg.ml_gpu_enabled,
        model_path: cfg.ml_model_path.clone(),
        enable_model_auto_update: true,
        model_health_check_interval: Duration::from_secs(300),
    };
    
    let ml_models = Arc::new(MlModelManager::new(ml_config, metrics.clone()));
    ml_models.initialize().await?;
    tracing::info!("ML model manager initialized");

    // Initialize signal processor
    let signal_config = SignalProcessorConfig {
        max_signals: 10000,
        signal_ttl_seconds: 86400,
        max_pipeline_workers: cfg.pipeline_workers,
        enable_real_time_processing: true,
        enable_signal_persistence: true,
        cleanup_interval_seconds: 3600,
    };
    
    let mut signal_processor = SignalProcessor::new(signal_config, metrics.clone());
    signal_processor.start().await?;
    tracing::info!("Signal processor started");

    // Connect to datastores
    let datastores = Datastores::connect(&cfg.database_url, &cfg.redis_url).await.expect("datastores connect failed");
    datastores.ensure_moderation_schema().await.expect("ensure schema failed");
    datastores.ensure_audit_schema().await.expect("ensure audit schema failed");
    let state = Arc::new(datastores);

    // Initialize broadcast channels for streaming
    let (report_tx, _report_rx) = broadcast::channel::<serde_json::Value>(1000);
    let (signal_tx, _signal_rx) = broadcast::channel::<serde_json::Value>(1000);

    // Initialize report manager
    let report_manager = Arc::new(ReportManager::new(
        signal_processor.clone(),
        metrics.clone(),
        Default::default(),
    ).with_datastores(state.clone()).with_event_sender(report_tx.clone()));
    tracing::info!("Report manager initialized");

    // Initialize production classifier
    let classifier_config = ClassifierConfig {
        min_confidence_threshold: 0.7,
        max_processing_time_ms: cfg.ml_inference_timeout.as_millis() as u64,
        enable_language_detection: cfg.language_detection_enabled,
        enable_ml_inference: true,
        enable_rule_based: true,
        enable_signal_processing: true,
        batch_processing: true,
        cache_results: true,
    };
    
    let classifier = Arc::new(ProductionClassifier::new(
        classifier_config,
        language_detector.clone(),
        ml_models.clone(),
        signal_processor.clone(),
        metrics.clone(),
    ));
    tracing::info!("Production classifier initialized");

    // Initialize compliance manager
    let compliance_manager = Arc::new(ComplianceManager::new(crate::core::compliance::ComplianceConfig::default()));
    tracing::info!("Compliance manager initialized");

    // Create application state with all components
    let app_state = Arc::new(AppState {
        classifier,
        language_detector,
        ml_models,
        signal_processor,
        report_manager,
        metrics,
        compliance_manager,
        config: cfg.clone(),
        datastores: state.clone(),
    });

    // Initialize observability
    if let Some(jaeger_endpoint) = &cfg.jaeger_endpoint {
        init_jaeger_tracing(jaeger_endpoint).await?;
        tracing::info!("Jaeger tracing initialized");
    }

    // Create HTTP router with enhanced middleware
    let (prom_layer, metric_handle) = PrometheusMetricLayer::pair();
    let base: Router = create_router();
    let compliance_router = moderation::api::compliance::create_compliance_router();
    let app = base
        .merge(compliance_router)
        .layer(CompressionLayer::new())
        .layer(DecompressionLayer::new())
        .layer(TimeoutLayer::new(cfg.pipeline_timeout))
        .layer(ConcurrencyLimitLayer::new(cfg.pipeline_workers * 2))
        .layer(TraceLayer::new_for_http())
        .layer(prom_layer)
        .layer(RateLimitLayer { 
            redis: state.redis.clone(), 
            window: cfg.rate_limit_window, 
            limit: cfg.rate_limit_requests 
        })
        .route("/metrics", axum::routing::get(move || async move { metric_handle.render() }))
        .route("/health", axum::routing::get(moderation::core::observability::health_check))
        .with_state(app_state);

    // Bind addresses
    let http_addr = SocketAddr::from(([0, 0, 0, 0], cfg.server_port));
    let grpc_addr = SocketAddr::from(([0, 0, 0, 0], cfg.grpc_port));
    
    tracing::info!(%http_addr, %grpc_addr, "Starting moderation service (http + grpc)");
    tracing::info!("Production features enabled:");
    tracing::info!("  - Multilingual support: {} languages", cfg.supported_languages.len());
    tracing::info!("  - ML inference: {} (GPU: {})", cfg.ml_batch_size, cfg.ml_gpu_enabled);
    tracing::info!("  - Pipeline workers: {}", cfg.pipeline_workers);
    tracing::info!("  - Observability: Prometheus + {}", if cfg.jaeger_endpoint.is_some() { "Jaeger" } else { "Basic" });

    // Start HTTP server
    let http_listener = tokio::net::TcpListener::bind(http_addr).await.expect("bind http failed");
    let http_server = axum::serve(http_listener, app).with_graceful_shutdown(shutdown_signal());

    // Start gRPC server
    let grpc_service = ModerationGrpcService::new(app_state).into_server();
    let grpc_server = TonicServer::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    // Start background tasks
    start_background_tasks(app_state.clone()).await;

    // Wait for servers to complete
    tokio::join!(http_server, grpc_server);
    
    tracing::info!("Moderation service shutdown complete");
    Ok(())
}

async fn init_jaeger_tracing(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::sdk::trace::config;
    use opentelemetry::sdk::Resource;
    use opentelemetry::KeyValue;
    use tracing_opentelemetry::OpenTelemetryLayer;

    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_endpoint(endpoint)
        .with_service_name("sonet-moderation")
        .with_trace_config(config().with_resource(Resource::new(vec![
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment", std::env::var("RUST_ENV").unwrap_or_else(|_| "development".to_string())),
        ])))
        .install_simple()?;

    let _layer = tracing_opentelemetry::layer().with_tracer(tracer);
    Ok(())
}

async fn start_background_tasks(app_state: Arc<AppState>) {
    let app_state_clone = app_state.clone();
    
    // Start periodic health checks
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            
            // Check ML model health
            if let Err(e) = app_state_clone.ml_models.get_model_info().await {
                tracing::warn!("ML model health check failed: {}", e);
            }
            
            // Check signal processor health
            // Check report manager health
            // Update system metrics
        }
    });

    // Start periodic cleanup tasks
    let app_state_clone = app_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Every hour
        
        loop {
            interval.tick().await;
            
            // Clean up expired signals
            // Clean up old reports
            // Clean up ML model cache
            // Database maintenance
        }
    });

    // Start performance monitoring
    let app_state_clone = app_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60)); // Every minute
        
        loop {
            interval.tick().await;
            
            // Monitor response times
            // Monitor error rates
            // Monitor resource usage
            // Generate performance reports
        }
    });
}

// Application state structure
pub struct AppState {
    pub classifier: Arc<ProductionClassifier>,
    pub language_detector: Arc<LanguageDetector>,
    pub ml_models: Arc<MlModelManager>,
    pub signal_processor: Arc<SignalProcessor>,
    pub report_manager: Arc<ReportManager>,
    pub metrics: Arc<MetricsCollector>,
    pub compliance_manager: Arc<ComplianceManager>,
    pub config: AppConfig,
    pub datastores: Arc<Datastores>,
}

impl AppState {
    pub async fn health_check(&self) -> serde_json::Value {
        let mut health_status = serde_json::json!({
            "status": "healthy",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "version": env!("CARGO_PKG_VERSION"),
            "components": {}
        });

        // Check ML model health
        if let Ok(_) = self.ml_models.get_model_info().await {
            health_status["components"]["ml_models"] = serde_json::json!({"status": "healthy"});
        } else {
            health_status["components"]["ml_models"] = serde_json::json!({"status": "unhealthy"});
            health_status["status"] = "degraded";
        }

        // Check database health
        if let Ok(_) = self.datastores.health_check().await {
            health_status["components"]["database"] = serde_json::json!({"status": "healthy"});
        } else {
            health_status["components"]["database"] = serde_json::json!({"status": "unhealthy"});
            health_status["status"] = "degraded";
        }

        // Check Redis health
        if let Ok(_) = self.datastores.redis_health_check().await {
            health_status["components"]["redis"] = serde_json::json!({"status": "healthy"});
        } else {
            health_status["components"]["redis"] = serde_json::json!({"status": "unhealthy"});
            health_status["status"] = "degraded";
        }

        health_status
    }
}

// Add missing imports
use chrono;