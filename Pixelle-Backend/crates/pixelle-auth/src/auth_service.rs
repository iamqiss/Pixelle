use async_trait::async_trait;
use pixelle_core::{AuthService, UserProfile, PixelleResult, UserId};
use crate::jwt::JwtService;
use crate::password::PasswordService;
use crate::session::SessionService;

/// Authentication service implementation
pub struct AuthServiceImpl {
    jwt_service: JwtService,
    password_service: PasswordService,
    session_service: SessionService,
}

impl AuthServiceImpl {
    pub fn new(jwt_secret: String) -> Self {
        Self {
            jwt_service: JwtService::new(jwt_secret),
            password_service: PasswordService::new(),
            session_service: SessionService::new(),
        }
    }
}

#[async_trait]
impl AuthService for AuthServiceImpl {
    async fn authenticate_user(&self, username: &str, password: &str) -> PixelleResult<Option<UserProfile>> {
        // This would typically query the database
        // For now, we'll return None to indicate authentication failure
        Ok(None)
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

    async fn hash_password(&self, password: &str) -> PixelleResult<String> {
        self.password_service.hash_password(password).await
    }

    async fn verify_password(&self, password: &str, hash: &str) -> PixelleResult<bool> {
        self.password_service.verify_password(password, hash).await
    }
}
