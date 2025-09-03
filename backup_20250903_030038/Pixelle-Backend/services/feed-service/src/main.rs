use actix_web::{web, App, HttpServer};
use pixelle_monitoring::init_tracing;
use std::env;

mod handlers;
mod models;
mod service;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing
    init_tracing();
    
    // Get port from environment or use default
    let port = env::var("PORT").unwrap_or_else(|_| "8082".to_string());
    let bind_address = format!("0.0.0.0:{}", port);
    
    tracing::info!("Starting feed service on {}", bind_address);
    
    HttpServer::new(|| {
        App::new()
            .service(
                web::scope("/api/v1/feed")
                    .service(handlers::get_user_feed)
                    .service(handlers::get_trending_posts)
            )
            .service(
                web::scope("/health")
                    .service(handlers::health_check)
            )
    })
    .bind(bind_address)?
    .run()
    .await
}
