use pixelle_auth::{
    EnhancedAuthService, RegisterRequest, LoginRequest, AuthConfig,
    PasswordConfig, SessionConfig, RateLimitConfig, SecurityConfig
};
use pixelle_core::{UserProfile, UserId, PixelleResult};
use pixelle_database::DatabaseRepository;
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;

// Example of integrating with Qubit/Nimbux services
pub struct QubitIntegratedAuthService {
    auth_service: EnhancedAuthService,
    // Add Qubit/Nimbux client here
    // qubit_client: Arc<QubitClient>,
}

impl QubitIntegratedAuthService {
    pub fn new(
        jwt_secret: String,
        user_repository: Arc<dyn pixelle_core::UserRepository + Send + Sync>,
        database_repo: Arc<DatabaseRepository>,
    ) -> Self {
        let auth_service = EnhancedAuthService::new(
            jwt_secret,
            user_repository,
            database_repo,
        );

        Self {
            auth_service,
            // qubit_client: Arc::new(QubitClient::new()),
        }
    }

    /// Register a new user with Qubit integration
    pub async fn register_with_qubit(&self, request: RegisterRequest) -> PixelleResult<pixelle_core::AuthToken> {
        // Register user in auth service
        let auth_token = self.auth_service.register(request).await?;

        // Create user storage in Qubit/Nimbux
        // self.create_user_storage(&auth_token.user_id).await?;

        Ok(auth_token)
    }

    /// Login user with Qubit session management
    pub async fn login_with_qubit(&self, request: LoginRequest) -> PixelleResult<pixelle_core::AuthToken> {
        // Login user
        let auth_token = self.auth_service.login(request).await?;

        // Create session in Qubit/Nimbux
        // self.create_qubit_session(&auth_token.user_id).await?;

        Ok(auth_token)
    }

    /// Logout user and clean up Qubit resources
    pub async fn logout_with_qubit(&self, session_token: &str) -> PixelleResult<()> {
        // Logout from auth service
        self.auth_service.logout(session_token).await?;

        // Clean up Qubit/Nimbux session
        // self.cleanup_qubit_session(session_token).await?;

        Ok(())
    }

    /// Create user storage in Qubit/Nimbux
    async fn create_user_storage(&self, user_id: &UserId) -> PixelleResult<()> {
        // Example: Create user-specific storage bucket
        // let bucket_name = format!("user-{}", user_id);
        // self.qubit_client.create_bucket(&bucket_name).await?;
        
        // Example: Set up user permissions
        // self.qubit_client.set_user_permissions(user_id, &permissions).await?;
        
        Ok(())
    }

    /// Create session in Qubit/Nimbux
    async fn create_qubit_session(&self, user_id: &UserId) -> PixelleResult<()> {
        // Example: Create session metadata in Qubit
        // let session_data = SessionData {
        //     user_id: *user_id,
        //     created_at: Utc::now(),
        //     expires_at: Utc::now() + chrono::Duration::hours(24),
        // };
        // self.qubit_client.store_session(&session_data).await?;
        
        Ok(())
    }

    /// Clean up Qubit/Nimbux session
    async fn cleanup_qubit_session(&self, session_token: &str) -> PixelleResult<()> {
        // Example: Remove session from Qubit
        // self.qubit_client.remove_session(session_token).await?;
        
        Ok(())
    }
}

/// Example configuration for production use
pub fn create_production_config() -> AuthConfig {
    AuthConfig {
        jwt_secret: std::env::var("JWT_SECRET")
            .unwrap_or_else(|_| "your-super-secret-jwt-key-change-in-production".to_string()),
        jwt_expiration_seconds: 3600, // 1 hour
        refresh_token_expiration_seconds: 604800, // 7 days
        password_config: PasswordConfig {
            min_length: 12,
            max_length: 128,
            require_uppercase: true,
            require_lowercase: true,
            require_numbers: true,
            require_special_chars: true,
            argon2_memory_cost: 8192, // 8 MB
            argon2_time_cost: 4,
            argon2_parallelism: 2,
        },
        session_config: SessionConfig {
            timeout_seconds: 3600, // 1 hour
            max_concurrent_sessions: 3,
            cleanup_interval_seconds: 300, // 5 minutes
            cookie_name: "pixelle_session".to_string(),
            cookie_secure: true,
            cookie_http_only: true,
            cookie_same_site: "Strict".to_string(),
        },
        rate_limit_config: RateLimitConfig {
            enabled: true,
            max_requests: 100,
            window_seconds: 60,
            login_max_attempts: 3,
            login_window_seconds: 900, // 15 minutes
            password_reset_max_attempts: 2,
            password_reset_window_seconds: 3600, // 1 hour
        },
        security_config: SecurityConfig {
            enable_account_lockout: true,
            max_failed_attempts: 3,
            lockout_duration_seconds: 1800, // 30 minutes
            enable_password_history: true,
            password_history_count: 10,
            password_expiration_days: 90,
            enable_2fa: true,
            enable_email_verification: true,
            enable_password_reset: true,
            password_reset_token_expiration_hours: 24,
        },
    }
}

/// Example of using the auth service with Qubit integration
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create mock user repository (replace with real implementation)
    let user_repository = Arc::new(MockUserRepository::new());
    let database_repo = Arc::new(DatabaseRepository::new());

    // Create Qubit-integrated auth service
    let auth_service = QubitIntegratedAuthService::new(
        "your-jwt-secret-key".to_string(),
        user_repository,
        database_repo,
    );

    // Example: Register a new user
    let register_request = RegisterRequest {
        username: "testuser".to_string(),
        email: "test@example.com".to_string(),
        password: "SecurePassword123!".to_string(),
        display_name: Some("Test User".to_string()),
        bio: Some("Test user bio".to_string()),
    };

    match auth_service.register_with_qubit(register_request).await {
        Ok(auth_token) => {
            println!("User registered successfully!");
            println!("Access token: {}", auth_token.access_token);
            println!("Refresh token: {}", auth_token.refresh_token);
        }
        Err(e) => {
            println!("Registration failed: {}", e);
        }
    }

    // Example: Login user
    let login_request = LoginRequest {
        username_or_email: "testuser".to_string(),
        password: "SecurePassword123!".to_string(),
        remember_me: Some(false),
    };

    match auth_service.login_with_qubit(login_request).await {
        Ok(auth_token) => {
            println!("User logged in successfully!");
            println!("Access token: {}", auth_token.access_token);
        }
        Err(e) => {
            println!("Login failed: {}", e);
        }
    }

    Ok(())
}

// Mock user repository for example
struct MockUserRepository;

#[async_trait::async_trait]
impl pixelle_core::UserRepository for MockUserRepository {
    async fn create_user(&self, user: &UserProfile) -> PixelleResult<UserProfile> {
        Ok(user.clone())
    }

    async fn get_user_by_id(&self, _user_id: UserId) -> PixelleResult<Option<UserProfile>> {
        Ok(None)
    }

    async fn get_user_by_username(&self, _username: &str) -> PixelleResult<Option<UserProfile>> {
        Ok(None)
    }

    async fn get_user_by_email(&self, _email: &str) -> PixelleResult<Option<UserProfile>> {
        Ok(None)
    }

    async fn update_user(&self, user: &UserProfile) -> PixelleResult<UserProfile> {
        Ok(user.clone())
    }

    async fn delete_user(&self, _user_id: UserId) -> PixelleResult<()> {
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