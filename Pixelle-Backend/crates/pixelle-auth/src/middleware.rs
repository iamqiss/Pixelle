use actix_web::{
    dev::{ServiceRequest, ServiceResponse},
    Error, HttpMessage, HttpRequest, HttpResponse,
    middleware::Next,
};
use actix_web::http::header::Authorization;
use actix_web::http::HeaderValue;
use pixelle_core::{PixelleResult, UserId, PixelleError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Authentication context that can be extracted from requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthContext {
    pub user_id: UserId,
    pub username: String,
    pub email: String,
    pub is_verified: bool,
}

/// JWT authentication middleware
pub struct AuthMiddleware {
    jwt_service: Arc<crate::jwt::JwtService>,
}

impl AuthMiddleware {
    pub fn new(jwt_service: Arc<crate::jwt::JwtService>) -> Self {
        Self { jwt_service }
    }

    /// Extract authentication context from request
    pub async fn extract_auth_context(&self, req: &HttpRequest) -> PixelleResult<Option<AuthContext>> {
        // Get Authorization header
        let auth_header = req.headers().get("Authorization")
            .ok_or_else(|| PixelleError::Authentication("Missing Authorization header".to_string()))?;

        let auth_str = auth_header.to_str()
            .map_err(|_| PixelleError::Authentication("Invalid Authorization header".to_string()))?;

        // Check if it's a Bearer token
        if !auth_str.starts_with("Bearer ") {
            return Err(PixelleError::Authentication("Invalid token format".to_string()));
        }

        let token = &auth_str[7..]; // Remove "Bearer " prefix

        // Validate token
        let user_id = self.jwt_service.validate_token(token).await?
            .ok_or_else(|| PixelleError::Authentication("Invalid token".to_string()))?;

        // TODO: Get user details from database
        // For now, return a mock context
        Ok(Some(AuthContext {
            user_id,
            username: "mock_user".to_string(),
            email: "mock@example.com".to_string(),
            is_verified: true,
        }))
    }
}

/// Middleware function for JWT authentication
pub async fn jwt_auth_middleware(
    req: ServiceRequest,
    next: Next<ServiceRequest>,
) -> Result<ServiceResponse, Error> {
    // Skip auth for certain paths
    let path = req.path();
    if path.starts_with("/health") || path.starts_with("/api/v1/auth/login") || path.starts_with("/api/v1/auth/register") {
        return next.call(req).await;
    }

    // Extract JWT service from app data
    let jwt_service = req.app_data::<Arc<crate::jwt::JwtService>>()
        .ok_or_else(|| actix_web::error::ErrorInternalServerError("JWT service not configured"))?;

    let auth_middleware = AuthMiddleware::new(jwt_service.clone());

    // Extract auth context
    match auth_middleware.extract_auth_context(req.request()).await {
        Ok(Some(auth_context)) => {
            // Store auth context in request extensions
            req.request().extensions_mut().insert(auth_context);
            next.call(req).await
        }
        Ok(None) => {
            Ok(ServiceResponse::new(
                req.into_parts().0,
                HttpResponse::Unauthorized().json(serde_json::json!({
                    "success": false,
                    "error": "Authentication required",
                    "message": "Please provide a valid authentication token"
                }))
            ))
        }
        Err(e) => {
            Ok(ServiceResponse::new(
                req.into_parts().0,
                HttpResponse::Unauthorized().json(serde_json::json!({
                    "success": false,
                    "error": e.to_string(),
                    "message": "Authentication failed"
                }))
            ))
        }
    }
}

/// Rate limiting middleware
pub struct RateLimitMiddleware {
    max_requests: u32,
    window_seconds: u64,
}

impl RateLimitMiddleware {
    pub fn new(max_requests: u32, window_seconds: u64) -> Self {
        Self {
            max_requests,
            window_seconds,
        }
    }

    pub async fn rate_limit_middleware(
        req: ServiceRequest,
        next: Next<ServiceRequest>,
    ) -> Result<ServiceResponse, Error> {
        // TODO: Implement rate limiting logic
        // For now, just pass through
        next.call(req).await
    }
}

/// Input validation middleware
pub async fn validation_middleware(
    req: ServiceRequest,
    next: Next<ServiceRequest>,
) -> Result<ServiceResponse, Error> {
    // TODO: Implement input validation
    // For now, just pass through
    next.call(req).await
}