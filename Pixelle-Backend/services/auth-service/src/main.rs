use actix_web::{
    web, App, HttpServer, HttpResponse, Result, middleware,
    middleware::Logger,
};
use actix_web::http::header;
use pixelle_monitoring::init_tracing;
use pixelle_auth::{
    EnhancedAuthService, RegisterRequest, LoginRequest, ChangePasswordRequest,
    ResetPasswordRequest, ConfirmPasswordResetRequest
};
use pixelle_core::{ApiResponse, PixelleResult};
use pixelle_database::DatabaseRepository;
use std::sync::Arc;
use serde_json::json;

mod mock_repository;
use mock_repository::MockUserRepository;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    init_tracing();
    
    // Initialize services
    let database_repo = Arc::new(DatabaseRepository::new());
    let user_repository = Arc::new(MockUserRepository);
    let auth_service = Arc::new(EnhancedAuthService::new(
        "your-jwt-secret-key".to_string(),
        user_repository,
        database_repo,
    ));
    
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(auth_service.clone()))
            .wrap(Logger::default())
            .wrap(middleware::DefaultHeaders::new().header("X-Version", "1.0"))
            .service(
                web::scope("/api/v1/auth")
                    .service(register)
                    .service(login)
                    .service(logout)
                    .service(refresh_token)
                    .service(change_password)
                    .service(request_password_reset)
                    .service(confirm_password_reset)
                    .service(get_profile)
                    .service(update_profile)
                    .service(delete_account)
            )
            .service(
                web::scope("/health")
                    .service(health_check)
            )
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}

async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(json!({
        "status": "healthy",
        "service": "auth-service",
        "version": "1.0.0"
    }))
}

#[actix_web::post("/register")]
async fn register(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    request: web::Json<RegisterRequest>,
) -> Result<HttpResponse> {
    match auth_service.register(request.into_inner()).await {
        Ok(auth_token) => Ok(HttpResponse::Created().json(ApiResponse {
            success: true,
            data: Some(auth_token),
            error: None,
            message: Some("User registered successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::post("/login")]
async fn login(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    request: web::Json<LoginRequest>,
) -> Result<HttpResponse> {
    match auth_service.login(request.into_inner()).await {
        Ok(auth_token) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(auth_token),
            error: None,
            message: Some("Login successful".to_string()),
        })),
        Err(e) => Ok(HttpResponse::Unauthorized().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::post("/logout")]
async fn logout(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    token: web::Header<header::Authorization<header::Bearer>>,
) -> Result<HttpResponse> {
    match auth_service.logout(token.as_str()).await {
        Ok(_) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: None,
            error: None,
            message: Some("Logout successful".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::post("/refresh")]
async fn refresh_token(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    request: web::Json<serde_json::Value>,
) -> Result<HttpResponse> {
    let refresh_token = request.get("refresh_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| actix_web::error::ErrorBadRequest("Missing refresh_token"))?;

    match auth_service.refresh_token(refresh_token).await {
        Ok(auth_token) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(auth_token),
            error: None,
            message: Some("Token refreshed successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::Unauthorized().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::post("/change-password")]
async fn change_password(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    token: web::Header<header::Authorization<header::Bearer>>,
    request: web::Json<ChangePasswordRequest>,
) -> Result<HttpResponse> {
    // TODO: Extract user_id from token
    let user_id = uuid::Uuid::new_v4(); // Mock user ID

    match auth_service.change_password(user_id, request.into_inner()).await {
        Ok(_) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: None,
            error: None,
            message: Some("Password changed successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::post("/request-password-reset")]
async fn request_password_reset(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    request: web::Json<ResetPasswordRequest>,
) -> Result<HttpResponse> {
    match auth_service.request_password_reset(request.into_inner()).await {
        Ok(_) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: None,
            error: None,
            message: Some("Password reset email sent".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::post("/confirm-password-reset")]
async fn confirm_password_reset(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    request: web::Json<ConfirmPasswordResetRequest>,
) -> Result<HttpResponse> {
    match auth_service.confirm_password_reset(request.into_inner()).await {
        Ok(_) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: None,
            error: None,
            message: Some("Password reset successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::get("/profile")]
async fn get_profile(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    token: web::Header<header::Authorization<header::Bearer>>,
) -> Result<HttpResponse> {
    // TODO: Extract user_id from token
    let user_id = uuid::Uuid::new_v4(); // Mock user ID

    match auth_service.get_user_profile(user_id).await {
        Ok(profile) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(profile),
            error: None,
            message: None,
        })),
        Err(e) => Ok(HttpResponse::NotFound().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::put("/profile")]
async fn update_profile(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    token: web::Header<header::Authorization<header::Bearer>>,
    request: web::Json<pixelle_core::UserProfile>,
) -> Result<HttpResponse> {
    match auth_service.update_user_profile(request.into_inner()).await {
        Ok(profile) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(profile),
            error: None,
            message: Some("Profile updated successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

#[actix_web::delete("/account")]
async fn delete_account(
    auth_service: web::Data<Arc<EnhancedAuthService>>,
    token: web::Header<header::Authorization<header::Bearer>>,
) -> Result<HttpResponse> {
    // TODO: Extract user_id from token
    let user_id = uuid::Uuid::new_v4(); // Mock user ID

    match auth_service.delete_user(user_id).await {
        Ok(_) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: None,
            error: None,
            message: Some("Account deleted successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}
