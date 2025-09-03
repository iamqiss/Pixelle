use actix_web::{web, App, HttpServer, HttpResponse};
use pixelle_monitoring::init_tracing;
use serde_json::json;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    init_tracing();
    
    HttpServer::new(|| {
        App::new()
            .service(
                web::scope("/health")
                    .route("", web::get().to(health_check))
            )
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}

async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(json!({
        "status": "healthy",
        "service": "media-processor"
    }))
}
