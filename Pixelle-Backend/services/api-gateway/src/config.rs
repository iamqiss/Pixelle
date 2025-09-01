use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub user_service_url: String,
    pub feed_service_url: String,
    pub content_service_url: String,
    pub auth_service_url: String,
    pub rate_limit_requests_per_minute: u32,
    pub rate_limit_requests_per_hour: u32,
    pub cors_origins: Vec<String>,
    pub jwt_secret: String,
}

impl GatewayConfig {
    pub fn from_env() -> Self {
        Self {
            user_service_url: env::var("USER_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:8081".to_string()),
            feed_service_url: env::var("FEED_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:8082".to_string()),
            content_service_url: env::var("CONTENT_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:8083".to_string()),
            auth_service_url: env::var("AUTH_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:8084".to_string()),
            rate_limit_requests_per_minute: env::var("RATE_LIMIT_PER_MINUTE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            rate_limit_requests_per_hour: env::var("RATE_LIMIT_PER_HOUR")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            cors_origins: env::var("CORS_ORIGINS")
                .unwrap_or_else(|_| "*".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            jwt_secret: env::var("JWT_SECRET")
                .unwrap_or_else(|_| "your-secret-key-here".to_string()),
        }
    }
}
