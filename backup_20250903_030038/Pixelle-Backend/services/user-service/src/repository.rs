use async_trait::async_trait;
use pixelle_core::{UserProfile, PaginationParams, PaginatedResponse, PixelleResult, UserRepository, UserId};
use std::collections::HashMap;
use std::sync::Mutex;
use chrono::Utc;

pub struct UserRepositoryImpl {
    users: Mutex<HashMap<UserId, UserProfile>>,
}

impl UserRepositoryImpl {
    pub fn new() -> Self {
        Self {
            users: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl UserRepository for UserRepositoryImpl {
    async fn create_user(&self, user: &UserProfile) -> PixelleResult<UserProfile> {
        let mut users = self.users.lock().unwrap();
        
        // Check if username already exists
        for existing_user in users.values() {
            if existing_user.username == user.username {
                return Err(pixelle_core::PixelleError::Conflict("Username already exists".to_string()));
            }
            if existing_user.email == user.email {
                return Err(pixelle_core::PixelleError::Conflict("Email already exists".to_string()));
            }
        }
        
        users.insert(user.id, user.clone());
        Ok(user.clone())
    }

    async fn get_user_by_id(&self, user_id: UserId) -> PixelleResult<Option<UserProfile>> {
        let users = self.users.lock().unwrap();
        Ok(users.get(&user_id).cloned())
    }

    async fn get_user_by_username(&self, username: &str) -> PixelleResult<Option<UserProfile>> {
        let users = self.users.lock().unwrap();
        Ok(users.values().find(|u| u.username == username).cloned())
    }

    async fn get_user_by_email(&self, email: &str) -> PixelleResult<Option<UserProfile>> {
        let users = self.users.lock().unwrap();
        Ok(users.values().find(|u| u.email == email).cloned())
    }

    async fn update_user(&self, user: &UserProfile) -> PixelleResult<UserProfile> {
        let mut users = self.users.lock().unwrap();
        
        if let Some(existing_user) = users.get_mut(&user.id) {
            *existing_user = user.clone();
            Ok(user.clone())
        } else {
            Err(pixelle_core::PixelleError::NotFound("User not found".to_string()))
        }
    }

    async fn delete_user(&self, user_id: UserId) -> PixelleResult<()> {
        let mut users = self.users.lock().unwrap();
        users.remove(&user_id);
        Ok(())
    }

    async fn search_users(&self, query: &str, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<UserProfile>> {
        let users = self.users.lock().unwrap();
        
        let filtered_users: Vec<UserProfile> = users
            .values()
            .filter(|user| {
                user.username.to_lowercase().contains(&query.to_lowercase()) ||
                user.display_name.as_ref().map_or(false, |name| 
                    name.to_lowercase().contains(&query.to_lowercase())
                ) ||
                user.bio.as_ref().map_or(false, |bio| 
                    bio.to_lowercase().contains(&query.to_lowercase())
                )
            })
            .cloned()
            .collect();

        let total = filtered_users.len() as u64;
        let start = ((pagination.page - 1) * pagination.per_page) as usize;
        let end = (start + pagination.per_page as usize).min(filtered_users.len());
        
        let items = if start < filtered_users.len() {
            filtered_users[start..end].to_vec()
        } else {
            Vec::new()
        };

        let total_pages = ((total as f64) / (pagination.per_page as f64)).ceil() as u32;

        Ok(PaginatedResponse {
            items,
            total,
            page: pagination.page,
            per_page: pagination.per_page,
            total_pages,
        })
    }
}
