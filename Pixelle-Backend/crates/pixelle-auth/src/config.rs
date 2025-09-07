use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Authentication service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// JWT secret key for token signing
    pub jwt_secret: String,
    
    /// JWT token expiration time in seconds
    pub jwt_expiration_seconds: u64,
    
    /// Refresh token expiration time in seconds
    pub refresh_token_expiration_seconds: u64,
    
    /// Password hashing configuration
    pub password_config: PasswordConfig,
    
    /// Session configuration
    pub session_config: SessionConfig,
    
    /// Rate limiting configuration
    pub rate_limit_config: RateLimitConfig,
    
    /// Security configuration
    pub security_config: SecurityConfig,
}

/// Password hashing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordConfig {
    /// Minimum password length
    pub min_length: usize,
    
    /// Maximum password length
    pub max_length: usize,
    
    /// Require uppercase letters
    pub require_uppercase: bool,
    
    /// Require lowercase letters
    pub require_lowercase: bool,
    
    /// Require numbers
    pub require_numbers: bool,
    
    /// Require special characters
    pub require_special_chars: bool,
    
    /// Argon2 memory cost (in KB)
    pub argon2_memory_cost: u32,
    
    /// Argon2 time cost (iterations)
    pub argon2_time_cost: u32,
    
    /// Argon2 parallelism
    pub argon2_parallelism: u32,
}

/// Session configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    /// Session timeout in seconds
    pub timeout_seconds: u64,
    
    /// Maximum concurrent sessions per user
    pub max_concurrent_sessions: u32,
    
    /// Session cleanup interval in seconds
    pub cleanup_interval_seconds: u64,
    
    /// Session cookie name
    pub cookie_name: String,
    
    /// Session cookie secure flag
    pub cookie_secure: bool,
    
    /// Session cookie http only flag
    pub cookie_http_only: bool,
    
    /// Session cookie same site policy
    pub cookie_same_site: String,
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Enable rate limiting
    pub enabled: bool,
    
    /// Maximum requests per window
    pub max_requests: u32,
    
    /// Rate limit window in seconds
    pub window_seconds: u64,
    
    /// Rate limit for login attempts
    pub login_max_attempts: u32,
    
    /// Login attempt window in seconds
    pub login_window_seconds: u64,
    
    /// Rate limit for password reset requests
    pub password_reset_max_attempts: u32,
    
    /// Password reset window in seconds
    pub password_reset_window_seconds: u64,
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Enable account lockout after failed attempts
    pub enable_account_lockout: bool,
    
    /// Maximum failed login attempts before lockout
    pub max_failed_attempts: u32,
    
    /// Account lockout duration in seconds
    pub lockout_duration_seconds: u64,
    
    /// Enable password history
    pub enable_password_history: bool,
    
    /// Number of previous passwords to remember
    pub password_history_count: u32,
    
    /// Password expiration in days (0 = never expires)
    pub password_expiration_days: u32,
    
    /// Enable two-factor authentication
    pub enable_2fa: bool,
    
    /// Enable email verification
    pub enable_email_verification: bool,
    
    /// Enable password reset
    pub enable_password_reset: bool,
    
    /// Password reset token expiration in hours
    pub password_reset_token_expiration_hours: u32,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            jwt_secret: "change-me-in-production".to_string(),
            jwt_expiration_seconds: 3600, // 1 hour
            refresh_token_expiration_seconds: 604800, // 7 days
            password_config: PasswordConfig::default(),
            session_config: SessionConfig::default(),
            rate_limit_config: RateLimitConfig::default(),
            security_config: SecurityConfig::default(),
        }
    }
}

impl Default for PasswordConfig {
    fn default() -> Self {
        Self {
            min_length: 8,
            max_length: 128,
            require_uppercase: true,
            require_lowercase: true,
            require_numbers: true,
            require_special_chars: true,
            argon2_memory_cost: 4096, // 4 MB
            argon2_time_cost: 3,
            argon2_parallelism: 1,
        }
    }
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 3600, // 1 hour
            max_concurrent_sessions: 5,
            cleanup_interval_seconds: 300, // 5 minutes
            cookie_name: "pixelle_session".to_string(),
            cookie_secure: true,
            cookie_http_only: true,
            cookie_same_site: "Strict".to_string(),
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_requests: 100,
            window_seconds: 60, // 1 minute
            login_max_attempts: 5,
            login_window_seconds: 900, // 15 minutes
            password_reset_max_attempts: 3,
            password_reset_window_seconds: 3600, // 1 hour
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enable_account_lockout: true,
            max_failed_attempts: 5,
            lockout_duration_seconds: 1800, // 30 minutes
            enable_password_history: true,
            password_history_count: 5,
            password_expiration_days: 90,
            enable_2fa: false,
            enable_email_verification: true,
            enable_password_reset: true,
            password_reset_token_expiration_hours: 24,
        }
    }
}

impl AuthConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        Self {
            jwt_secret: std::env::var("JWT_SECRET")
                .unwrap_or_else(|_| "change-me-in-production".to_string()),
            jwt_expiration_seconds: std::env::var("JWT_EXPIRATION_SECONDS")
                .unwrap_or_else(|_| "3600".to_string())
                .parse()
                .unwrap_or(3600),
            refresh_token_expiration_seconds: std::env::var("REFRESH_TOKEN_EXPIRATION_SECONDS")
                .unwrap_or_else(|_| "604800".to_string())
                .parse()
                .unwrap_or(604800),
            password_config: PasswordConfig::from_env(),
            session_config: SessionConfig::from_env(),
            rate_limit_config: RateLimitConfig::from_env(),
            security_config: SecurityConfig::from_env(),
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.jwt_secret.len() < 32 {
            return Err("JWT secret must be at least 32 characters long".to_string());
        }

        if self.jwt_expiration_seconds == 0 {
            return Err("JWT expiration must be greater than 0".to_string());
        }

        if self.refresh_token_expiration_seconds <= self.jwt_expiration_seconds {
            return Err("Refresh token expiration must be greater than JWT expiration".to_string());
        }

        self.password_config.validate()?;
        self.session_config.validate()?;
        self.rate_limit_config.validate()?;
        self.security_config.validate()?;

        Ok(())
    }
}

impl PasswordConfig {
    fn from_env() -> Self {
        Self {
            min_length: std::env::var("PASSWORD_MIN_LENGTH")
                .unwrap_or_else(|_| "8".to_string())
                .parse()
                .unwrap_or(8),
            max_length: std::env::var("PASSWORD_MAX_LENGTH")
                .unwrap_or_else(|_| "128".to_string())
                .parse()
                .unwrap_or(128),
            require_uppercase: std::env::var("PASSWORD_REQUIRE_UPPERCASE")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            require_lowercase: std::env::var("PASSWORD_REQUIRE_LOWERCASE")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            require_numbers: std::env::var("PASSWORD_REQUIRE_NUMBERS")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            require_special_chars: std::env::var("PASSWORD_REQUIRE_SPECIAL_CHARS")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            argon2_memory_cost: std::env::var("ARGON2_MEMORY_COST")
                .unwrap_or_else(|_| "4096".to_string())
                .parse()
                .unwrap_or(4096),
            argon2_time_cost: std::env::var("ARGON2_TIME_COST")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            argon2_parallelism: std::env::var("ARGON2_PARALLELISM")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .unwrap_or(1),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.min_length < 1 {
            return Err("Password minimum length must be at least 1".to_string());
        }

        if self.max_length < self.min_length {
            return Err("Password maximum length must be greater than minimum length".to_string());
        }

        if self.argon2_memory_cost < 1024 {
            return Err("Argon2 memory cost must be at least 1024 KB".to_string());
        }

        if self.argon2_time_cost < 1 {
            return Err("Argon2 time cost must be at least 1".to_string());
        }

        if self.argon2_parallelism < 1 {
            return Err("Argon2 parallelism must be at least 1".to_string());
        }

        Ok(())
    }
}

impl SessionConfig {
    fn from_env() -> Self {
        Self {
            timeout_seconds: std::env::var("SESSION_TIMEOUT_SECONDS")
                .unwrap_or_else(|_| "3600".to_string())
                .parse()
                .unwrap_or(3600),
            max_concurrent_sessions: std::env::var("MAX_CONCURRENT_SESSIONS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            cleanup_interval_seconds: std::env::var("SESSION_CLEANUP_INTERVAL_SECONDS")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
            cookie_name: std::env::var("SESSION_COOKIE_NAME")
                .unwrap_or_else(|_| "pixelle_session".to_string()),
            cookie_secure: std::env::var("SESSION_COOKIE_SECURE")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            cookie_http_only: std::env::var("SESSION_COOKIE_HTTP_ONLY")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            cookie_same_site: std::env::var("SESSION_COOKIE_SAME_SITE")
                .unwrap_or_else(|_| "Strict".to_string()),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.timeout_seconds == 0 {
            return Err("Session timeout must be greater than 0".to_string());
        }

        if self.max_concurrent_sessions == 0 {
            return Err("Maximum concurrent sessions must be greater than 0".to_string());
        }

        if self.cleanup_interval_seconds == 0 {
            return Err("Session cleanup interval must be greater than 0".to_string());
        }

        if self.cookie_name.is_empty() {
            return Err("Session cookie name cannot be empty".to_string());
        }

        Ok(())
    }
}

impl RateLimitConfig {
    fn from_env() -> Self {
        Self {
            enabled: std::env::var("RATE_LIMIT_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            max_requests: std::env::var("RATE_LIMIT_MAX_REQUESTS")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            window_seconds: std::env::var("RATE_LIMIT_WINDOW_SECONDS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60),
            login_max_attempts: std::env::var("LOGIN_MAX_ATTEMPTS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            login_window_seconds: std::env::var("LOGIN_WINDOW_SECONDS")
                .unwrap_or_else(|_| "900".to_string())
                .parse()
                .unwrap_or(900),
            password_reset_max_attempts: std::env::var("PASSWORD_RESET_MAX_ATTEMPTS")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            password_reset_window_seconds: std::env::var("PASSWORD_RESET_WINDOW_SECONDS")
                .unwrap_or_else(|_| "3600".to_string())
                .parse()
                .unwrap_or(3600),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.max_requests == 0 {
            return Err("Rate limit max requests must be greater than 0".to_string());
        }

        if self.window_seconds == 0 {
            return Err("Rate limit window must be greater than 0".to_string());
        }

        if self.login_max_attempts == 0 {
            return Err("Login max attempts must be greater than 0".to_string());
        }

        if self.login_window_seconds == 0 {
            return Err("Login window must be greater than 0".to_string());
        }

        if self.password_reset_max_attempts == 0 {
            return Err("Password reset max attempts must be greater than 0".to_string());
        }

        if self.password_reset_window_seconds == 0 {
            return Err("Password reset window must be greater than 0".to_string());
        }

        Ok(())
    }
}

impl SecurityConfig {
    fn from_env() -> Self {
        Self {
            enable_account_lockout: std::env::var("ENABLE_ACCOUNT_LOCKOUT")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            max_failed_attempts: std::env::var("MAX_FAILED_ATTEMPTS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            lockout_duration_seconds: std::env::var("LOCKOUT_DURATION_SECONDS")
                .unwrap_or_else(|_| "1800".to_string())
                .parse()
                .unwrap_or(1800),
            enable_password_history: std::env::var("ENABLE_PASSWORD_HISTORY")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            password_history_count: std::env::var("PASSWORD_HISTORY_COUNT")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            password_expiration_days: std::env::var("PASSWORD_EXPIRATION_DAYS")
                .unwrap_or_else(|_| "90".to_string())
                .parse()
                .unwrap_or(90),
            enable_2fa: std::env::var("ENABLE_2FA")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            enable_email_verification: std::env::var("ENABLE_EMAIL_VERIFICATION")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            enable_password_reset: std::env::var("ENABLE_PASSWORD_RESET")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            password_reset_token_expiration_hours: std::env::var("PASSWORD_RESET_TOKEN_EXPIRATION_HOURS")
                .unwrap_or_else(|_| "24".to_string())
                .parse()
                .unwrap_or(24),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.max_failed_attempts == 0 {
            return Err("Max failed attempts must be greater than 0".to_string());
        }

        if self.lockout_duration_seconds == 0 {
            return Err("Lockout duration must be greater than 0".to_string());
        }

        if self.password_history_count == 0 {
            return Err("Password history count must be greater than 0".to_string());
        }

        if self.password_reset_token_expiration_hours == 0 {
            return Err("Password reset token expiration must be greater than 0".to_string());
        }

        Ok(())
    }
}