use async_trait::async_trait;
use pixelle_core::{
    UserRepository, UserProfile, UserId, PixelleResult, PixelleError,
    PaginationParams, PaginatedResponse
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Mock user repository for development and testing
pub struct MockUserRepository {
    users: Arc<RwLock<HashMap<UserId, UserProfile>>>,
}

impl MockUserRepository {
    pub fn new() -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl UserRepository for MockUserRepository {
    async fn create_user(&self, user: &UserProfile) -> PixelleResult<UserProfile> {
        let mut users = self.users.write().await;
        
        // Check if user already exists
        if users.contains_key(&user.id) {
            return Err(PixelleError::Conflict("User already exists".to_string()));
        }
        
        users.insert(user.id, user.clone());
        Ok(user.clone())
    }

    async fn get_user_by_id(&self, user_id: UserId) -> PixelleResult<Option<UserProfile>> {
        let users = self.users.read().await;
        Ok(users.get(&user_id).cloned())
    }

    async fn get_user_by_username(&self, username: &str) -> PixelleResult<Option<UserProfile>> {
        let users = self.users.read().await;
        Ok(users.values()
            .find(|user| user.username == username)
            .cloned())
    }

    async fn get_user_by_email(&self, email: &str) -> PixelleResult<Option<UserProfile>> {
        let users = self.users.read().await;
        Ok(users.values()
            .find(|user| user.email == email)
            .cloned())
    }

    async fn update_user(&self, user: &UserProfile) -> PixelleResult<UserProfile> {
        let mut users = self.users.write().await;
        
        if let Some(existing_user) = users.get_mut(&user.id) {
            *existing_user = user.clone();
            Ok(user.clone())
        } else {
            Err(PixelleError::NotFound("User not found".to_string()))
        }
    }

    async fn delete_user(&self, user_id: UserId) -> PixelleResult<()> {
        let mut users = self.users.write().await;
        users.remove(&user_id);
        Ok(())
    }

    async fn search_users(&self, query: &str, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<UserProfile>> {
        let users = self.users.read().await;
        let all_users: Vec<UserProfile> = users.values()
            .filter(|user| {
                user.username.contains(query) ||
                user.email.contains(query) ||
                user.display_name.as_ref().map_or(false, |name| name.contains(query))
            })
            .cloned()
            .collect();

        let total = all_users.len() as u64;
        let start = ((pagination.page - 1) * pagination.per_page) as usize;
        let end = (start + pagination.per_page as usize).min(all_users.len());
        
        let items = if start < all_users.len() {
            all_users[start..end].to_vec()
        } else {
            Vec::new()
        };

        Ok(PaginatedResponse {
            items,
            total,
            page: pagination.page,
            per_page: pagination.per_page,
            total_pages: ((total as f64) / (pagination.per_page as f64)).ceil() as u32,
        })
    }
}