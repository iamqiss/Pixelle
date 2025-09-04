use serde::{Deserialize, Serialize};
use std::time::Duration;
use config::{Config, ConfigError, Environment, File};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    // Server Configuration
    pub server_port: u16,
    pub grpc_port: u16,
    pub host: String,
    
    // Database Configuration
    pub database_url: String,
    pub redis_url: String,
    pub max_connections: u32,
    pub connection_timeout: Duration,
    
    // ML & Inference Configuration
    pub ml_model_path: String,
    pub ml_batch_size: usize,
    pub ml_inference_timeout: Duration,
    pub ml_gpu_enabled: bool,
    pub ml_model_cache_size: usize,
    
    // Multilingual Configuration
    pub supported_languages: Vec<String>,
    pub default_language: String,
    pub language_detection_enabled: bool,
    
    // Pipeline Configuration
    pub pipeline_workers: usize,
    pub pipeline_buffer_size: usize,
    pub pipeline_timeout: Duration,
    
    // Storage & Throughput
    pub storage_backend: StorageBackend,
    pub max_content_size: usize,
    pub compression_enabled: bool,
    pub cache_ttl: Duration,
    
    // Observability
    pub log_level: String,
    pub jaeger_endpoint: Option<String>,
    pub prometheus_enabled: bool,
    pub health_check_interval: Duration,
    
    // Security & Rate Limiting
    pub rate_limit_requests: u32,
    pub rate_limit_window: Duration,
    pub jwt_secret: String,
    pub cors_origins: Vec<String>,
    
    // Abuse Specialist Configuration
    pub specialist_queue_size: usize,
    pub specialist_timeout: Duration,
    pub escalation_threshold: f32,
    
    // Operations & Monitoring
    pub alerting_enabled: bool,
    pub metrics_export_interval: Duration,
    pub backup_enabled: bool,
    pub backup_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageBackend {
    Postgres,
    Redis,
    Sled,
    Hybrid,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server_port: 8080,
            grpc_port: 9090,
            host: "0.0.0.0".to_string(),
            
            database_url: "postgresql://localhost/sonet_moderation".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            max_connections: 100,
            connection_timeout: Duration::from_secs(30),
            
            ml_model_path: "./models".to_string(),
            ml_batch_size: 32,
            ml_inference_timeout: Duration::from_secs(10),
            ml_gpu_enabled: false,
            ml_model_cache_size: 1000,
            
            supported_languages: vec!["en".to_string(), "es".to_string(), "fr".to_string(), "de".to_string()],
            default_language: "en".to_string(),
            language_detection_enabled: true,
            
            pipeline_workers: 8,
            pipeline_buffer_size: 10000,
            pipeline_timeout: Duration::from_secs(60),
            
            storage_backend: StorageBackend::Hybrid,
            max_content_size: 1024 * 1024, // 1MB
            compression_enabled: true,
            cache_ttl: Duration::from_secs(3600),
            
            log_level: "info".to_string(),
            jaeger_endpoint: None,
            prometheus_enabled: true,
            health_check_interval: Duration::from_secs(30),
            
            rate_limit_requests: 1000,
            rate_limit_window: Duration::from_secs(60),
            jwt_secret: "default-secret-change-in-production".to_string(),
            cors_origins: vec!["*".to_string()],
            
            specialist_queue_size: 1000,
            specialist_timeout: Duration::from_secs(300),
            escalation_threshold: 0.8,
            
            alerting_enabled: true,
            metrics_export_interval: Duration::from_secs(60),
            backup_enabled: true,
            backup_interval: Duration::from_secs(86400), // 24 hours
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "./config".to_string());
        
        let config = Config::builder()
            .add_source(File::from(Path::new(&config_path).join("default.toml")).required(false))
            .add_source(File::from(Path::new(&config_path).join("local.toml")).required(false))
            .add_source(File::from(Path::new(&config_path).join("production.toml")).required(false))
            .add_source(Environment::with_prefix("MODERATION").separator("__"))
            .build()?;
            
        config.try_deserialize()
    }
    
    pub fn is_production(&self) -> bool {
        std::env::var("RUST_ENV").unwrap_or_else(|_| "development".to_string()) == "production"
    }
    
    pub fn get_database_pool_size(&self) -> u32 {
        if self.is_production() {
            self.max_connections
        } else {
            std::cmp::min(self.max_connections, 10)
        }
    }
}