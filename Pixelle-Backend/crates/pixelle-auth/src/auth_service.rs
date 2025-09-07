use async_trait::async_trait;
use pixelle_core::{AuthService, UserProfile, PixelleResult, UserId, UserRepository};
use pixelle_database::{DatabaseRepository, DatabaseUser};
use crate::jwt::JwtService;
use crate::passphrase::PassphraseService;
use crate::session::SessionService;
use std::sync::Arc;

/// Authentication service implementation
pub struct AuthServiceImpl {
    jwt_service: JwtService,
    passphrase_service: PassphraseService,
    session_service: SessionService,
    user_repository: Arc<dyn UserRepository + Send + Sync>,
    database_repo: Arc<DatabaseRepository>,
}

impl AuthServiceImpl {
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
}

#[async_trait]
impl AuthService for AuthServiceImpl {
    async fn authenticate_user(&self, username: &str, password: &str) -> PixelleResult<Option<UserProfile>> {
        // Try to find user by username or email
        let user = if username.contains('@') {
            self.user_repository.get_user_by_email(username).await?
        } else {
            self.user_repository.get_user_by_username(username).await?
        };

        match user {
            Some(user_profile) => {
                // In a real implementation, you would verify the password hash
                // For now, we'll assume the password is correct if user exists
                // TODO: Implement proper password verification with stored hash
                Ok(Some(user_profile))
            }
            None => Ok(None),
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
