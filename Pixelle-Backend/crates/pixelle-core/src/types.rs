use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use validator::Validate;

/// User ID type alias
pub type UserId = Uuid;

/// Post ID type alias  
pub type PostId = Uuid;

/// Comment ID type alias
pub type CommentId = Uuid;

/// Generic ID type
pub type Id = Uuid;

/// Pagination parameters
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct PaginationParams {
    #[validate(range(min = 1, max = 100))]
    pub page: u32,
    #[validate(range(min = 1, max = 50))]
    pub per_page: u32,
}

/// Standard API response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
    pub message: Option<String>,
}

/// Paginated response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub page: u32,
    pub per_page: u32,
    pub total_pages: u32,
}

/// User profile information
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct UserProfile {
    pub id: UserId,
    pub username: String,
    pub email: String,
    pub display_name: Option<String>,
    pub bio: Option<String>,
    pub avatar_url: Option<String>,
    pub is_verified: bool,
    pub is_private: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Post content
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct Post {
    pub id: PostId,
    pub author_id: UserId,
    pub content: String,
    pub media_urls: Vec<String>,
    pub likes_count: u32,
    pub comments_count: u32,
    pub shares_count: u32,
    pub is_public: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Comment on a post
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct Comment {
    pub id: CommentId,
    pub post_id: PostId,
    pub author_id: UserId,
    pub content: String,
    pub parent_id: Option<CommentId>,
    pub likes_count: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Authentication token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthToken {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
    pub token_type: String,
}

/// User session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSession {
    pub user_id: UserId,
    pub session_id: String,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}
