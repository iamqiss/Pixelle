use async_trait::async_trait;
use pixelle_core::{AuthService, UserProfile, PixelleResult, UserId};
use crate::jwt::JwtService;
use crate::passphrase::PassphraseService;
use crate::session::SessionService;

/// Authentication service implementation
pub struct AuthServiceImpl {
    jwt_service: JwtService,
    passphrase_service: PassphraseService,
    session_service: SessionService,
}

impl AuthServiceImpl {
    pub fn new(jwt_secret: String) -> Self {
        Self {
            jwt_service: JwtService::new(jwt_secret),
            passphrase_service: PassphraseService::new(),
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

    async fn hash_passphrase(&self, passphrase: &str) -> PixelleResult<String> {
        self.passphrase_service.hash_password(passphrase).await
    }

    async fn verify_passphrase(&self, passphrase: &str, hash: &str) -> PixelleResult<bool> {
        self.passphrase_service.verify_password(passphrase, hash).await
    }
}
