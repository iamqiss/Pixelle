use async_trait::async_trait;
use pixelle_core::{UserProfile, PaginationParams, PaginatedResponse, PixelleResult, UserRepository};
use crate::repository::UserRepositoryImpl;
use pixelle_auth::AuthServiceImpl;

pub struct UserService {
    repository: UserRepositoryImpl,
    auth_service: AuthServiceImpl,
}

impl UserService {
    pub fn new(repository: UserRepositoryImpl, auth_service: AuthServiceImpl) -> Self {
        Self {
            repository,
            auth_service,
        }
    }

    pub async fn create_user(&self, request: &crate::handlers::CreateUserRequest) -> PixelleResult<UserProfile> {
        // Validate input
        if request.username.len() < 3 || request.username.len() > 20 {
            return Err(pixelle_core::PixelleError::Validation("Username must be between 3 and 20 characters".to_string()));
        }

        if request.password.len() < 8 {
            return Err(pixelle_core::PixelleError::Validation("Password must be at least 8 characters".to_string()));
        }

        // Hash password
        let hashed_password = self.auth_service.hash_password(&request.password).await?;

        // Create user profile
        let user = UserProfile {
            id: pixelle_core::generate_id(),
            username: request.username.clone(),
            email: request.email.clone(),
            display_name: request.display_name.clone(),
            bio: request.bio.clone(),
            avatar_url: None,
            is_verified: false,
            is_private: false,
            created_at: pixelle_core::now(),
            updated_at: pixelle_core::now(),
        };

        // Save to repository
        self.repository.create_user(&user).await
    }

    pub async fn get_user_by_id(&self, user_id: &str) -> PixelleResult<Option<UserProfile>> {
        let user_id = user_id.parse::<pixelle_core::UserId>()
            .map_err(|_| pixelle_core::PixelleError::Validation("Invalid user ID format".to_string()))?;
        
        self.repository.get_user_by_id(user_id).await
    }

    pub async fn update_user(&self, user_id: &str, request: &crate::handlers::UpdateUserRequest) -> PixelleResult<UserProfile> {
        let user_id = user_id.parse::<pixelle_core::UserId>()
            .map_err(|_| pixelle_core::PixelleError::Validation("Invalid user ID format".to_string()))?;
        
        let mut user = self.repository.get_user_by_id(user_id).await?
            .ok_or_else(|| pixelle_core::PixelleError::NotFound("User not found".to_string()))?;

        // Update fields
        if let Some(display_name) = &request.display_name {
            user.display_name = Some(display_name.clone());
        }
        if let Some(bio) = &request.bio {
            user.bio = Some(bio.clone());
        }
        if let Some(avatar_url) = &request.avatar_url {
            user.avatar_url = Some(avatar_url.clone());
        }
        if let Some(is_private) = request.is_private {
            user.is_private = is_private;
        }

        user.updated_at = pixelle_core::now();

        self.repository.update_user(&user).await
    }

    pub async fn delete_user(&self, user_id: &str) -> PixelleResult<()> {
        let user_id = user_id.parse::<pixelle_core::UserId>()
            .map_err(|_| pixelle_core::PixelleError::Validation("Invalid user ID format".to_string()))?;
        
        self.repository.delete_user(user_id).await
    }

    pub async fn search_users(&self, query: &str, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<UserProfile>> {
        self.repository.search_users(query, pagination).await
    }
}
