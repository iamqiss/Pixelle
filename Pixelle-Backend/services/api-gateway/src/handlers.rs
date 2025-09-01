use actix_web::{web, HttpRequest, HttpResponse, Result};
use serde_json::json;
use crate::routing::ServiceRouter;
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn proxy_request(
    req: HttpRequest,
    payload: web::Payload,
    service_router: web::Data<Arc<RwLock<ServiceRouter>>>,
) -> Result<HttpResponse> {
    let router = service_router.get_ref();
    let router_guard = router.read().await;
    
    match router_guard.route_request(&req, payload).await {
        Ok(response) => Ok(response),
        Err(e) => {
            tracing::error!("Proxy error: {}", e);
            Ok(HttpResponse::InternalServerError().json(json!({
                "error": "Internal server error",
                "message": e.to_string()
            })))
        }
    }
}

pub async fn health_check() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(json!({
        "status": "healthy",
        "service": "api-gateway",
        "timestamp": chrono::Utc::now()
    })))
}

pub async fn metrics() -> Result<HttpResponse> {
    // This would return Prometheus metrics
    Ok(HttpResponse::Ok().body("# HELP http_requests_total Total number of HTTP requests\n# TYPE http_requests_total counter\nhttp_requests_total 0"))
}
