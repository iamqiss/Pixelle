#[cfg(test)]
mod tests {
    use super::*;
    use pixelle_core::{UserProfile, UserId, PixelleResult};
    use pixelle_database::DatabaseRepository;
    use std::sync::Arc;
    use chrono::Utc;
    use uuid::Uuid;

    // Mock user repository for testing
    struct TestUserRepository {
        users: std::collections::HashMap<UserId, UserProfile>,
    }

    impl TestUserRepository {
        fn new() -> Self {
            Self {
                users: std::collections::HashMap::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl pixelle_core::UserRepository for TestUserRepository {
        async fn create_user(&self, user: &UserProfile) -> PixelleResult<UserProfile> {
            Ok(user.clone())
        }

        async fn get_user_by_id(&self, user_id: UserId) -> PixelleResult<Option<UserProfile>> {
            Ok(self.users.get(&user_id).cloned())
        }

        async fn get_user_by_username(&self, username: &str) -> PixelleResult<Option<UserProfile>> {
            Ok(self.users.values()
                .find(|u| u.username == username)
                .cloned())
        }

        async fn get_user_by_email(&self, email: &str) -> PixelleResult<Option<UserProfile>> {
            Ok(self.users.values()
                .find(|u| u.email == email)
                .cloned())
        }

        async fn update_user(&self, user: &UserProfile) -> PixelleResult<UserProfile> {
            Ok(user.clone())
        }

        async fn delete_user(&self, user_id: UserId) -> PixelleResult<()> {
            Ok(())
        }

        async fn search_users(&self, _query: &str, _pagination: &pixelle_core::PaginationParams) -> PixelleResult<pixelle_core::PaginatedResponse<UserProfile>> {
            Ok(pixelle_core::PaginatedResponse {
                items: vec![],
                total: 0,
                page: 1,
                per_page: 10,
                total_pages: 0,
            })
        }
    }

    #[tokio::test]
    async fn test_jwt_token_creation_and_validation() {
        let jwt_service = JwtService::new("test-secret".to_string());
        let user_id = Uuid::new_v4();

        // Create token
        let token = jwt_service.create_token(user_id).await.unwrap();
        assert!(!token.is_empty());

        // Validate token
        let validated_user_id = jwt_service.validate_token(&token).await.unwrap();
        assert_eq!(validated_user_id, Some(user_id));
    }

    #[tokio::test]
    async fn test_password_hashing() {
        let passphrase_service = PassphraseService::new();
        let password = "test_password_123";

        // Hash password
        let hash = passphrase_service.hash_passphrase(password).await.unwrap();
        assert!(!hash.is_empty());
        assert_ne!(hash, password);

        // Verify password
        let is_valid = passphrase_service.verify_passphrase(password, &hash).await.unwrap();
        assert!(is_valid);

        // Verify wrong password
        let is_invalid = passphrase_service.verify_passphrase("wrong_password", &hash).await.unwrap();
        assert!(!is_invalid);
    }

    #[tokio::test]
    async fn test_session_management() {
        let session_service = SessionService::new();
        let user_id = "test_user_123";
        let expires_at = Utc::now() + chrono::Duration::hours(1);

        // Create session
        let session_id = session_service.create_session(user_id, expires_at).await.unwrap();
        assert!(!session_id.is_empty());

        // Get session
        let retrieved_user_id = session_service.get_session(&session_id).await.unwrap();
        assert_eq!(retrieved_user_id, Some(user_id.to_string()));

        // Revoke session
        session_service.revoke_session(&session_id).await.unwrap();

        // Session should be gone
        let retrieved_user_id_after_revoke = session_service.get_session(&session_id).await.unwrap();
        assert_eq!(retrieved_user_id_after_revoke, None);
    }

    #[tokio::test]
    async fn test_enhanced_auth_service_register() {
        let user_repository = Arc::new(TestUserRepository::new());
        let database_repo = Arc::new(DatabaseRepository::new());
        let auth_service = EnhancedAuthService::new(
            "test-secret".to_string(),
            user_repository,
            database_repo,
        );

        let register_request = RegisterRequest {
            username: "testuser".to_string(),
            email: "test@example.com".to_string(),
            password: "password123".to_string(),
            display_name: Some("Test User".to_string()),
            bio: Some("Test bio".to_string()),
        };

        let result = auth_service.register(register_request).await;
        assert!(result.is_ok());

        let auth_token = result.unwrap();
        assert_eq!(auth_token.token_type, "Bearer");
        assert!(!auth_token.access_token.is_empty());
        assert!(!auth_token.refresh_token.is_empty());
    }

    #[tokio::test]
    async fn test_enhanced_auth_service_login() {
        let user_repository = Arc::new(TestUserRepository::new());
        let database_repo = Arc::new(DatabaseRepository::new());
        let auth_service = EnhancedAuthService::new(
            "test-secret".to_string(),
            user_repository,
            database_repo,
        );

        let login_request = LoginRequest {
            username_or_email: "testuser".to_string(),
            password: "password123".to_string(),
            remember_me: Some(false),
        };

        // This should fail because user doesn't exist
        let result = auth_service.login(login_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_auth_service_trait_implementation() {
        let user_repository = Arc::new(TestUserRepository::new());
        let database_repo = Arc::new(DatabaseRepository::new());
        let auth_service = EnhancedAuthService::new(
            "test-secret".to_string(),
            user_repository,
            database_repo,
        );

        // Test authenticate_user
        let result = auth_service.authenticate_user("testuser", "password123").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Test create_session
        let user_id = Uuid::new_v4();
        let session_token = auth_service.create_session(user_id).await.unwrap();
        assert!(!session_token.is_empty());

        // Test validate_session
        let validated_user_id = auth_service.validate_session(&session_token).await.unwrap();
        assert_eq!(validated_user_id, Some(user_id));

        // Test revoke_session
        let revoke_result = auth_service.revoke_session(&session_token).await;
        assert!(revoke_result.is_ok());

        // Test hash_passphrase
        let hash = auth_service.hash_passphrase("test_password").await.unwrap();
        assert!(!hash.is_empty());

        // Test verify_passphrase
        let is_valid = auth_service.verify_passphrase("test_password", &hash).await.unwrap();
        assert!(is_valid);
    }

    #[tokio::test]
    async fn test_password_validation() {
        let passphrase_service = PassphraseService::new();

        // Test valid password
        let valid_password = "ValidPassword123!";
        let hash = passphrase_service.hash_passphrase(valid_password).await.unwrap();
        let is_valid = passphrase_service.verify_passphrase(valid_password, &hash).await.unwrap();
        assert!(is_valid);

        // Test invalid password
        let invalid_password = "wrong_password";
        let is_invalid = passphrase_service.verify_passphrase(invalid_password, &hash).await.unwrap();
        assert!(!is_invalid);
    }

    #[tokio::test]
    async fn test_jwt_token_expiration() {
        let jwt_service = JwtService::new("test-secret".to_string());
        let user_id = Uuid::new_v4();

        // Create token
        let token = jwt_service.create_token(user_id).await.unwrap();

        // Token should be valid immediately
        let validated_user_id = jwt_service.validate_token(&token).await.unwrap();
        assert_eq!(validated_user_id, Some(user_id));

        // Test with invalid token
        let invalid_token = "invalid.token.here";
        let invalid_result = jwt_service.validate_token(invalid_token).await.unwrap();
        assert_eq!(invalid_result, None);
    }

    #[tokio::test]
    async fn test_session_cleanup() {
        let session_service = SessionService::new();
        let user_id = "test_user_123";
        
        // Create expired session
        let expired_time = Utc::now() - chrono::Duration::hours(1);
        let session_id = session_service.create_session(user_id, expired_time).await.unwrap();

        // Session should exist but be expired
        let retrieved_user_id = session_service.get_session(&session_id).await.unwrap();
        assert_eq!(retrieved_user_id, None);

        // Cleanup expired sessions
        session_service.cleanup_expired_sessions().await.unwrap();

        // Session should still be gone
        let retrieved_user_id_after_cleanup = session_service.get_session(&session_id).await.unwrap();
        assert_eq!(retrieved_user_id_after_cleanup, None);
    }
}