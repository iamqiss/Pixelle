use async_trait::async_trait;
use crate::types::{UserId, PostId, CommentId, UserProfile, Post, Comment, PaginationParams, PaginatedResponse};
use crate::errors::PixelleResult;

/// Repository trait for user operations
#[async_trait]
pub trait UserRepository {
    async fn create_user(&self, user: &UserProfile) -> PixelleResult<UserProfile>;
    async fn get_user_by_id(&self, user_id: UserId) -> PixelleResult<Option<UserProfile>>;
    async fn get_user_by_username(&self, username: &str) -> PixelleResult<Option<UserProfile>>;
    async fn get_user_by_email(&self, email: &str) -> PixelleResult<Option<UserProfile>>;
    async fn update_user(&self, user: &UserProfile) -> PixelleResult<UserProfile>;
    async fn delete_user(&self, user_id: UserId) -> PixelleResult<()>;
    async fn search_users(&self, query: &str, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<UserProfile>>;
}

/// Repository trait for post operations
#[async_trait]
pub trait PostRepository {
    async fn create_post(&self, post: &Post) -> PixelleResult<Post>;
    async fn get_post_by_id(&self, post_id: PostId) -> PixelleResult<Option<Post>>;
    async fn get_posts_by_user(&self, user_id: UserId, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<Post>>;
    async fn get_feed_posts(&self, user_id: UserId, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<Post>>;
    async fn update_post(&self, post: &Post) -> PixelleResult<Post>;
    async fn delete_post(&self, post_id: PostId) -> PixelleResult<()>;
    async fn like_post(&self, post_id: PostId, user_id: UserId) -> PixelleResult<()>;
    async fn unlike_post(&self, post_id: PostId, user_id: UserId) -> PixelleResult<()>;
}

/// Repository trait for comment operations
#[async_trait]
pub trait CommentRepository {
    async fn create_comment(&self, comment: &Comment) -> PixelleResult<Comment>;
    async fn get_comment_by_id(&self, comment_id: CommentId) -> PixelleResult<Option<Comment>>;
    async fn get_comments_by_post(&self, post_id: PostId, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<Comment>>;
    async fn update_comment(&self, comment: &Comment) -> PixelleResult<Comment>;
    async fn delete_comment(&self, comment_id: CommentId) -> PixelleResult<()>;
    async fn like_comment(&self, comment_id: CommentId, user_id: UserId) -> PixelleResult<()>;
    async fn unlike_comment(&self, comment_id: CommentId, user_id: UserId) -> PixelleResult<()>;
}

/// Authentication service trait
#[async_trait]
pub trait AuthService {
    async fn authenticate_user(&self, username: &str, password: &str) -> PixelleResult<Option<UserProfile>>;
    async fn create_session(&self, user_id: UserId) -> PixelleResult<String>;
    async fn validate_session(&self, session_token: &str) -> PixelleResult<Option<UserId>>;
    async fn revoke_session(&self, session_token: &str) -> PixelleResult<()>;
    async fn hash_passphrase(&self, passphrase: &str) -> PixelleResult<String>;
    async fn verify_passphrase(&self, passphrase: &str, hash: &str) -> PixelleResult<bool>;
}

/// Notification service trait
#[async_trait]
pub trait NotificationService {
    async fn send_notification(&self, user_id: UserId, message: &str, notification_type: &str) -> PixelleResult<()>;
    async fn get_user_notifications(&self, user_id: UserId, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<String>>;
    async fn mark_notification_read(&self, notification_id: &str) -> PixelleResult<()>;
}
