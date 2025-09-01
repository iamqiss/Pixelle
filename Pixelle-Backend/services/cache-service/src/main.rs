use actix_web::{web, App, HttpServer};
use pixelle_monitoring::init_tracing;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    init_tracing();
    
    HttpServer::new(|| {
        App::new()
            .service(
                web::scope("/health")
                    .service(health_check)
            )
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}

async fn health_check() -> actix_web::HttpResponse {
    actix_web::HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "cache-service"
    }))
}
