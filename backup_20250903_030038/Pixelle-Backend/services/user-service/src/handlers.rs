use actix_web::{web, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use pixelle_core::{UserProfile, ApiResponse, PaginationParams, PaginatedResponse, PixelleResult};
use crate::service::UserService;

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub email: String,
    pub password: String,
    pub display_name: Option<String>,
    pub bio: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    pub display_name: Option<String>,
    pub bio: Option<String>,
    pub avatar_url: Option<String>,
    pub is_private: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct SearchUsersQuery {
    pub q: String,
    pub page: Option<u32>,
    pub per_page: Option<u32>,
}

pub async fn create_user(
    user_service: web::Data<UserService>,
    request: web::Json<CreateUserRequest>,
) -> Result<HttpResponse> {
    let result = user_service.create_user(&request.into_inner()).await;
    
    match result {
        Ok(user) => Ok(HttpResponse::Created().json(ApiResponse {
            success: true,
            data: Some(user),
            error: None,
            message: Some("User created successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse::<UserProfile> {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

pub async fn get_user(
    user_service: web::Data<UserService>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let user_id = path.into_inner();
    let result = user_service.get_user_by_id(&user_id).await;
    
    match result {
        Ok(Some(user)) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(user),
            error: None,
            message: None,
        })),
        Ok(None) => Ok(HttpResponse::NotFound().json(ApiResponse::<UserProfile> {
            success: false,
            data: None,
            error: Some("User not found".to_string()),
            message: None,
        })),
        Err(e) => Ok(HttpResponse::InternalServerError().json(ApiResponse::<UserProfile> {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

pub async fn update_user(
    user_service: web::Data<UserService>,
    path: web::Path<String>,
    request: web::Json<UpdateUserRequest>,
) -> Result<HttpResponse> {
    let user_id = path.into_inner();
    let result = user_service.update_user(&user_id, &request.into_inner()).await;
    
    match result {
        Ok(user) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(user),
            error: None,
            message: Some("User updated successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::BadRequest().json(ApiResponse::<UserProfile> {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

pub async fn delete_user(
    user_service: web::Data<UserService>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let user_id = path.into_inner();
    let result = user_service.delete_user(&user_id).await;
    
    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(ApiResponse::<()> {
            success: true,
            data: None,
            error: None,
            message: Some("User deleted successfully".to_string()),
        })),
        Err(e) => Ok(HttpResponse::InternalServerError().json(ApiResponse::<()> {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

pub async fn search_users(
    user_service: web::Data<UserService>,
    query: web::Query<SearchUsersQuery>,
) -> Result<HttpResponse> {
    let pagination = PaginationParams {
        page: query.page.unwrap_or(1),
        per_page: query.per_page.unwrap_or(20),
    };
    
    let result = user_service.search_users(&query.q, &pagination).await;
    
    match result {
        Ok(users) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(users),
            error: None,
            message: None,
        })),
        Err(e) => Ok(HttpResponse::InternalServerError().json(ApiResponse::<PaginatedResponse<UserProfile>> {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

pub async fn health_check() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "user-service",
        "timestamp": chrono::Utc::now()
    })))
}
