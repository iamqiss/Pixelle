use actix_web::{HttpRequest, HttpResponse, web::Payload};
use reqwest::Client;
use crate::config::GatewayConfig;
use anyhow::Result;

pub struct ServiceRouter {
    config: GatewayConfig,
    client: Client,
}

impl ServiceRouter {
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    pub async fn route_request(&self, req: &HttpRequest, payload: Payload) -> Result<HttpResponse> {
        let path = req.path();
        let method = req.method().as_str();
        
        // Route based on path
        let target_url = if path.starts_with("/api/v1/users") {
            format!("{}{}", self.config.user_service_url, path)
        } else if path.starts_with("/api/v1/feed") {
            format!("{}{}", self.config.feed_service_url, path)
        } else if path.starts_with("/api/v1/posts") {
            format!("{}{}", self.config.content_service_url, path)
        } else if path.starts_with("/api/v1/auth") {
            format!("{}{}", self.config.auth_service_url, path)
        } else {
            return Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": "Service not found",
                "path": path
            })));
        };

        // Forward the request
        self.forward_request(&target_url, req, payload).await
    }

    async fn forward_request(&self, target_url: &str, req: &HttpRequest, payload: Payload) -> Result<HttpResponse> {
        let method = req.method().clone();
        let headers = req.headers().clone();
        
        // Build the request
        let mut request_builder = self.client
            .request(method, target_url)
            .headers(headers);

        // Add query parameters
        if let Some(query) = req.uri().query() {
            request_builder = request_builder.query(&[("", query)]);
        }

        // Execute the request
        let response = request_builder
            .body(payload)
            .send()
            .await?;

        // Convert response
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.bytes().await?;

        let mut http_response = HttpResponse::build(status);
        
        // Copy headers
        for (key, value) in headers {
            if let Some(key) = key {
                http_response.header(key, value);
            }
        }

        Ok(http_response.body(body))
    }
}
