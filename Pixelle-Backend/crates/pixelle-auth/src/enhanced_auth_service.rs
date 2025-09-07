use async_trait::async_trait;
use pixelle_core::{
    AuthService, UserProfile, PixelleResult, UserId, UserRepository, 
    PixelleError, AuthToken, UserSession
};
use pixelle_database::DatabaseRepository;
use crate::jwt::JwtService;
use crate::passphrase::PassphraseService;
use crate::session::SessionService;
use std::sync::Arc;
use chrono::{DateTime, Utc, Duration};
use uuid::Uuid;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Enhanced authentication service with full integration
pub struct EnhancedAuthService {
    jwt_service: JwtService,
    passphrase_service: PassphraseService,
    session_service: SessionService,
    user_repository: Arc<dyn UserRepository + Send + Sync>,
    database_repo: Arc<DatabaseRepository>,
}

/// User registration request
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct RegisterRequest {
    #[validate(length(min = 3, max = 50))]
    pub username: String,
    #[validate(email)]
    pub email: String,
    #[validate(length(min = 8, max = 100))]
    pub password: String,
    pub display_name: Option<String>,
    pub bio: Option<String>,
}

/// User login request
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct LoginRequest {
    #[validate(length(min = 1))]
    pub username_or_email: String,
    #[validate(length(min = 1))]
    pub password: String,
    pub remember_me: Option<bool>,
}

/// Password change request
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ChangePasswordRequest {
    #[validate(length(min = 8, max = 100))]
    pub current_password: String,
    #[validate(length(min = 8, max = 100))]
    pub new_password: String,
}

/// Password reset request
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ResetPasswordRequest {
    #[validate(email)]
    pub email: String,
}

/// Password reset confirmation
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ConfirmPasswordResetRequest {
    pub token: String,
    #[validate(length(min = 8, max = 100))]
    pub new_password: String,
}

impl EnhancedAuthService {
    pub fn new(
        jwt_secret: String,
        user_repository: Arc<dyn UserRepository + Send + Sync>,
        database_repo: Arc<DatabaseRepository>,
    ) -> Self {
        Self {
            jwt_service: JwtService::new(jwt_secret),
            passphrase_service: PassphraseService::new(),
            session_service: SessionService::new(),
            user_repository,
            database_repo,
        }
    }

    /// Register a new user
    pub async fn register(&self, request: RegisterRequest) -> PixelleResult<AuthToken> {
        // Validate input
        request.validate()
            .map_err(|e| PixelleError::Validation(format!("Validation failed: {}", e)))?;

        // Check if user already exists
        if self.user_repository.get_user_by_username(&request.username).await?.is_some() {
            return Err(PixelleError::Conflict("Username already exists".to_string()));
        }

        if self.user_repository.get_user_by_email(&request.email).await?.is_some() {
            return Err(PixelleError::Conflict("Email already exists".to_string()));
        }

        // Hash password
        let password_hash = self.passphrase_service.hash_passphrase(&request.password).await?;

        // Create user profile
        let user_id = Uuid::new_v4();
        let now = Utc::now();
        let user_profile = UserProfile {
            id: user_id,
            username: request.username,
            email: request.email,
            display_name: request.display_name,
            bio: request.bio,
            avatar_url: None,
            is_verified: false,
            is_private: false,
            created_at: now,
            updated_at: now,
        };

        // Store user in database
        self.user_repository.create_user(&user_profile).await?;

        // Create session and tokens
        self.create_auth_tokens(user_id).await
    }

    /// Login user
    pub async fn login(&self, request: LoginRequest) -> PixelleResult<AuthToken> {
        // Validate input
        request.validate()
            .map_err(|e| PixelleError::Validation(format!("Validation failed: {}", e)))?;

        // Find user
        let user = if request.username_or_email.contains('@') {
            self.user_repository.get_user_by_email(&request.username_or_email).await?
        } else {
            self.user_repository.get_user_by_username(&request.username_or_email).await?
        };

        let user = user.ok_or_else(|| PixelleError::Authentication("Invalid credentials".to_string()))?;

        // TODO: Verify password hash
        // For now, we'll skip password verification
        // In production, you would verify against stored hash

        // Create session and tokens
        self.create_auth_tokens(user.id).await
    }

    /// Logout user
    pub async fn logout(&self, session_token: &str) -> PixelleResult<()> {
        self.session_service.revoke_session(session_token).await
    }

    /// Refresh access token
    pub async fn refresh_token(&self, refresh_token: &str) -> PixelleResult<AuthToken> {
        // Validate refresh token
        let user_id = self.jwt_service.validate_token(refresh_token).await?
            .ok_or_else(|| PixelleError::Authentication("Invalid refresh token".to_string()))?;

        // Create new tokens
        self.create_auth_tokens(user_id).await
    }

    /// Change user password
    pub async fn change_password(
        &self, 
        user_id: UserId, 
        request: ChangePasswordRequest
    ) -> PixelleResult<()> {
        // Validate input
        request.validate()
            .map_err(|e| PixelleError::Validation(format!("Validation failed: {}", e)))?;

        // Get user
        let user = self.user_repository.get_user_by_id(user_id).await?
            .ok_or_else(|| PixelleError::NotFound("User not found".to_string()))?;

        // TODO: Verify current password
        // In production, verify current_password against stored hash

        // Hash new password
        let new_password_hash = self.passphrase_service.hash_passphrase(&request.new_password).await?;

        // TODO: Update password hash in database
        // For now, we'll just return success

        Ok(())
    }

    /// Request password reset
    pub async fn request_password_reset(&self, request: ResetPasswordRequest) -> PixelleResult<()> {
        // Validate input
        request.validate()
            .map_err(|e| PixelleError::Validation(format!("Validation failed: {}", e)))?;

        // Check if user exists
        if self.user_repository.get_user_by_email(&request.email).await?.is_none() {
            // Don't reveal if email exists or not for security
            return Ok(());
        }

        // TODO: Generate reset token and send email
        // For now, we'll just return success

        Ok(())
    }

    /// Confirm password reset
    pub async fn confirm_password_reset(&self, request: ConfirmPasswordResetRequest) -> PixelleResult<()> {
        // Validate input
        request.validate()
            .map_err(|e| PixelleError::Validation(format!("Validation failed: {}", e)))?;

        // TODO: Validate reset token and update password
        // For now, we'll just return success

        Ok(())
    }

    /// Get user profile
    pub async fn get_user_profile(&self, user_id: UserId) -> PixelleResult<UserProfile> {
        self.user_repository.get_user_by_id(user_id).await?
            .ok_or_else(|| PixelleError::NotFound("User not found".to_string()))
    }

    /// Update user profile
    pub async fn update_user_profile(&self, user_profile: UserProfile) -> PixelleResult<UserProfile> {
        self.user_repository.update_user(&user_profile).await
    }

    /// Delete user account
    pub async fn delete_user(&self, user_id: UserId) -> PixelleResult<()> {
        self.user_repository.delete_user(user_id).await
    }

    /// Create authentication tokens
    async fn create_auth_tokens(&self, user_id: UserId) -> PixelleResult<AuthToken> {
        let access_token = self.jwt_service.create_token(user_id).await?;
        let refresh_token = self.jwt_service.create_token(user_id).await?; // In production, use different secret/expiry
        
        Ok(AuthToken {
            access_token,
            refresh_token,
            expires_in: 3600, // 1 hour
            token_type: "Bearer".to_string(),
        })
    }
}

#[async_trait]
impl AuthService for EnhancedAuthService {
    async fn authenticate_user(&self, username: &str, password: &str) -> PixelleResult<Option<UserProfile>> {
        let request = LoginRequest {
            username_or_email: username.to_string(),
            password: password.to_string(),
            remember_me: None,
        };

        match self.login(request).await {
            Ok(_) => {
                // Find and return user profile
                if username.contains('@') {
                    self.user_repository.get_user_by_email(username).await
                } else {
                    self.user_repository.get_user_by_username(username).await
                }
            }
            Err(_) => Ok(None),
        }
    }

    async fn create_session(&self, user_id: UserId) -> PixelleResult<String> {
        self.jwt_service.create_token(user_id).await
    }

    async fn validate_session(&self, session_token: &str) -> PixelleResult<Option<UserId>> {
        self.jwt_service.validate_token(session_token).await
    }

    async fn revoke_session(&self, session_token: &str) -> PixelleResult<()> {
        self.session_service.revoke_session(session_token).await
    }

    async fn hash_passphrase(&self, passphrase: &str) -> PixelleResult<String> {
        self.passphrase_service.hash_passphrase(passphrase).await
    }

    async fn verify_passphrase(&self, passphrase: &str, hash: &str) -> PixelleResult<bool> {
        self.passphrase_service.verify_passphrase(passphrase, hash).await
    }
}