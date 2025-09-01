use actix_web::{web, App, HttpServer, middleware};
use actix_web::middleware::Logger;
use pixelle_monitoring::init_tracing;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

mod handlers;
mod middleware;
mod config;
mod routing;

use config::GatewayConfig;
use routing::ServiceRouter;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing
    init_tracing();
    
    // Load configuration
    let config = GatewayConfig::from_env();
    
    // Get port from environment or use default
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_address = format!("0.0.0.0:{}", port);
    
    tracing::info!("Starting API Gateway on {}", bind_address);
    tracing::info!("User service URL: {}", config.user_service_url);
    
    // Create service router
    let service_router = Arc::new(RwLock::new(ServiceRouter::new(config.clone())));
    
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(middleware::cors::Cors::permissive())
            .app_data(web::Data::new(service_router.clone()))
            .service(
                web::scope("/api/v1")
                    .service(handlers::proxy_request)
            )
            .service(
                web::scope("/health")
                    .service(handlers::health_check)
            )
            .service(
                web::scope("/metrics")
                    .service(handlers::metrics)
            )
    })
    .bind(bind_address)?
    .run()
    .await
}
