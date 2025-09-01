use actix_web::{web, App, HttpServer};
use pixelle_monitoring::init_tracing;
use std::env;

mod handlers;
mod models;
mod repository;
mod service;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing
    init_tracing();
    
    // Get port from environment or use default
    let port = env::var("PORT").unwrap_or_else(|_| "8081".to_string());
    let bind_address = format!("0.0.0.0:{}", port);
    
    tracing::info!("Starting user service on {}", bind_address);
    
    HttpServer::new(|| {
        App::new()
            .service(
                web::scope("/api/v1/users")
                    .service(handlers::create_user)
                    .service(handlers::get_user)
                    .service(handlers::update_user)
                    .service(handlers::delete_user)
                    .service(handlers::search_users)
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
