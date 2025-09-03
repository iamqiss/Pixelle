use actix_web::{web, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use pixelle_core::{ApiResponse, PaginationParams, PaginatedResponse, Post};
use crate::service::FeedService;

#[derive(Debug, Deserialize)]
pub struct FeedQuery {
    pub page: Option<u32>,
    pub per_page: Option<u32>,
}

pub async fn get_user_feed(
    feed_service: web::Data<FeedService>,
    query: web::Query<FeedQuery>,
    user_id: web::Path<String>,
) -> Result<HttpResponse> {
    let pagination = PaginationParams {
        page: query.page.unwrap_or(1),
        per_page: query.per_page.unwrap_or(20),
    };
    
    let result = feed_service.get_user_feed(&user_id, &pagination).await;
    
    match result {
        Ok(posts) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(posts),
            error: None,
            message: None,
        })),
        Err(e) => Ok(HttpResponse::InternalServerError().json(ApiResponse::<PaginatedResponse<Post>> {
            success: false,
            data: None,
            error: Some(e.to_string()),
            message: None,
        })),
    }
}

pub async fn get_trending_posts(
    feed_service: web::Data<FeedService>,
    query: web::Query<FeedQuery>,
) -> Result<HttpResponse> {
    let pagination = PaginationParams {
        page: query.page.unwrap_or(1),
        per_page: query.per_page.unwrap_or(20),
    };
    
    let result = feed_service.get_trending_posts(&pagination).await;
    
    match result {
        Ok(posts) => Ok(HttpResponse::Ok().json(ApiResponse {
            success: true,
            data: Some(posts),
            error: None,
            message: None,
        })),
        Err(e) => Ok(HttpResponse::InternalServerError().json(ApiResponse::<PaginatedResponse<Post>> {
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
        "service": "feed-service",
        "timestamp": chrono::Utc::now()
    })))
}
