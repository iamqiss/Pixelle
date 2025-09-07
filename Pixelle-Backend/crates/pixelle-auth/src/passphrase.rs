use argon2::{Argon2, PassphraseHash, PassphraseHasher, PassphraseVerifier, Algorithm, Version, Params};
use argon2::passphrase_hash::rand_core::OsRng;
use argon2::passphrase_hash::SaltString;
use pixelle_core::{PixelleResult, PixelleError};
use std::collections::HashMap;
use std::sync::RwLock;
use chrono::{DateTime, Utc, Duration};
use uuid::Uuid;
use tracing::{debug, warn, error, info};
use regex::Regex;
use zxcvbn::zxcvbn;

/// Password strength levels
#[derive(Debug, Clone, PartialEq)]
pub enum PasswordStrength {
    VeryWeak,
    Weak,
    Fair,
    Good,
    Strong,
}

/// Password validation result
#[derive(Debug)]
pub struct PasswordValidationResult {
    pub is_valid: bool,
    pub strength: PasswordStrength,
    pub score: u8,
    pub feedback: Vec<String>,
    pub entropy: f64,
    pub crack_time: String,
}

/// Password breach detection result
#[derive(Debug)]
pub struct BreachCheckResult {
    pub is_breached: bool,
    pub breach_count: u32,
    pub breach_sources: Vec<String>,
    pub last_checked: DateTime<Utc>,
}

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub max_attempts_per_minute: u32,
    pub max_attempts_per_hour: u32,
    pub max_attempts_per_day: u32,
    pub lockout_duration_minutes: u32,
    pub progressive_delay_seconds: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_attempts_per_minute: 5,
            max_attempts_per_hour: 20,
            max_attempts_per_day: 100,
            lockout_duration_minutes: 15,
            progressive_delay_seconds: 2,
        }
    }
}

/// Password policy configuration
#[derive(Debug, Clone)]
pub struct PasswordPolicy {
    pub min_length: usize,
    pub max_length: usize,
    pub require_uppercase: bool,
    pub require_lowercase: bool,
    pub require_numbers: bool,
    pub require_special_chars: bool,
    pub min_entropy: f64,
    pub forbidden_patterns: Vec<String>,
    pub max_repeating_chars: usize,
    pub max_sequential_chars: usize,
    pub prevent_common_passwords: bool,
    pub prevent_user_info: bool,
}

impl Default for PasswordPolicy {
    fn default() -> Self {
        Self {
            min_length: 12,
            max_length: 128,
            require_uppercase: true,
            require_lowercase: true,
            require_numbers: true,
            require_special_chars: true,
            min_entropy: 60.0,
            forbidden_patterns: vec![
                "password".to_string(),
                "123456".to_string(),
                "qwerty".to_string(),
                "admin".to_string(),
            ],
            max_repeating_chars: 3,
            max_sequential_chars: 3,
            prevent_common_passwords: true,
            prevent_user_info: true,
        }
    }
}

/// Advanced password security service with comprehensive features
pub struct PassphraseService {
    rate_limit_config: RateLimitConfig,
    password_policy: PasswordPolicy,
    attempt_tracker: RwLock<HashMap<String, Vec<DateTime<Utc>>>>,
    breach_cache: RwLock<HashMap<String, BreachCheckResult>>,
    common_passwords: RwLock<Vec<String>>,
    argon2_config: Argon2Config,
}

#[derive(Debug, Clone)]
struct Argon2Config {
    algorithm: Algorithm,
    version: Version,
    memory_cost: u32,
    time_cost: u32,
    parallelism: u32,
}

impl Default for Argon2Config {
    fn default() -> Self {
        Self {
            algorithm: Algorithm::Argon2id,
            version: Version::V0x13,
            memory_cost: 65536, // 64 MB
            time_cost: 3,
            parallelism: 4,
        }
    }
}

impl PassphraseService {
    /// Create a new password service with default configuration
    pub fn new() -> Self {
        Self::with_config(
            RateLimitConfig::default(),
            PasswordPolicy::default(),
            Argon2Config::default(),
        )
    }

    /// Create a new password service with custom configuration
    pub fn with_config(
        rate_limit_config: RateLimitConfig,
        password_policy: PasswordPolicy,
        argon2_config: Argon2Config,
    ) -> Self {
        let mut service = Self {
            rate_limit_config,
            password_policy,
            attempt_tracker: RwLock::new(HashMap::new()),
            breach_cache: RwLock::new(HashMap::new()),
            common_passwords: RwLock::new(Vec::new()),
            argon2_config,
        };

        // Load common passwords for validation
        service.load_common_passwords();
        service
    }

    /// Hash a password with advanced security features
    pub async fn hash_password(&self, password: &str) -> PixelleResult<String> {
        // Validate password before hashing
        let validation = self.validate_password_strength(password, None, None, None)?;
        if !validation.is_valid {
            return Err(PixelleError::Validation(format!(
                "Password does not meet security requirements: {}",
                validation.feedback.join(", ")
            )));
        }

        // Generate cryptographically secure salt
        let salt = SaltString::generate(&mut OsRng);
        
        // Create Argon2 instance with custom parameters
        let params = Params::new(
            self.argon2_config.memory_cost,
            self.argon2_config.time_cost,
            self.argon2_config.parallelism,
            Some(32), // Output length
        ).map_err(|e| PixelleError::Internal(format!("Argon2 parameter error: {}", e)))?;

        let argon2 = Argon2::new(
            self.argon2_config.algorithm,
            self.argon2_config.version,
            params,
        );

        // Hash the password
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| PixelleError::Internal(format!("Password hashing error: {}", e)))?;

        debug!("Successfully hashed password with strength: {:?}", validation.strength);
        Ok(password_hash.to_string())
    }

    /// Verify a password against its hash
    pub async fn verify_password(&self, password: &str, hash: &str) -> PixelleResult<bool> {
        let parsed_hash = PassphraseHash::new(hash)
            .map_err(|e| PixelleError::Internal(format!("Invalid password hash: {}", e)))?;

        let result = Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok();

        Ok(result)
    }

    /// Verify password with rate limiting and security checks
    pub async fn verify_password_with_checks(
        &self,
        password: &str,
        hash: &str,
        identifier: &str, // username, email, or IP
        user_info: Option<&UserInfo>,
    ) -> PixelleResult<bool> {
        // Check rate limiting
        if !self.check_rate_limit(identifier).await? {
            return Err(PixelleError::RateLimit("Too many password verification attempts".to_string()));
        }

        // Record attempt
        self.record_attempt(identifier).await;

        // Check for password breach
        if self.is_password_breached(password).await? {
            warn!("Breached password detected for identifier: {}", identifier);
            return Err(PixelleError::Security("Password has been found in data breaches".to_string()));
        }

        // Verify password
        let is_valid = self.verify_password(password, hash).await?;

        if !is_valid {
            // Log failed attempt
            self.log_failed_attempt(identifier, user_info).await;
        } else {
            // Clear failed attempts on successful verification
            self.clear_failed_attempts(identifier).await;
        }

        Ok(is_valid)
    }

    /// Validate password strength against policy
    pub fn validate_password_strength(
        &self,
        password: &str,
        username: Option<&str>,
        email: Option<&str>,
        user_info: Option<&UserInfo>,
    ) -> PixelleResult<PasswordValidationResult> {
        let mut feedback = Vec::new();
        let mut is_valid = true;

        // Check length
        if password.len() < self.password_policy.min_length {
            feedback.push(format!("Password must be at least {} characters long", self.password_policy.min_length));
            is_valid = false;
        }
        if password.len() > self.password_policy.max_length {
            feedback.push(format!("Password must be no more than {} characters long", self.password_policy.max_length));
            is_valid = false;
        }

        // Check character requirements
        if self.password_policy.require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            feedback.push("Password must contain at least one uppercase letter".to_string());
            is_valid = false;
        }
        if self.password_policy.require_lowercase && !password.chars().any(|c| c.is_lowercase()) {
            feedback.push("Password must contain at least one lowercase letter".to_string());
            is_valid = false;
        }
        if self.password_policy.require_numbers && !password.chars().any(|c| c.is_numeric()) {
            feedback.push("Password must contain at least one number".to_string());
            is_valid = false;
        }
        if self.password_policy.require_special_chars && !password.chars().any(|c| !c.is_alphanumeric()) {
            feedback.push("Password must contain at least one special character".to_string());
            is_valid = false;
        }

        // Check for repeating characters
        if self.has_repeating_chars(password, self.password_policy.max_repeating_chars) {
            feedback.push(format!("Password cannot have more than {} consecutive identical characters", self.password_policy.max_repeating_chars));
            is_valid = false;
        }

        // Check for sequential characters
        if self.has_sequential_chars(password, self.password_policy.max_sequential_chars) {
            feedback.push(format!("Password cannot have more than {} consecutive sequential characters", self.password_policy.max_sequential_chars));
            is_valid = false;
        }

        // Check forbidden patterns
        for pattern in &self.password_policy.forbidden_patterns {
            if password.to_lowercase().contains(pattern) {
                feedback.push(format!("Password cannot contain common patterns like '{}'", pattern));
                is_valid = false;
            }
        }

        // Check against common passwords
        if self.password_policy.prevent_common_passwords && self.is_common_password(password) {
            feedback.push("Password is too common and easily guessable".to_string());
            is_valid = false;
        }

        // Check against user information
        if self.password_policy.prevent_user_info {
            if let Some(username) = username {
                if password.to_lowercase().contains(&username.to_lowercase()) {
                    feedback.push("Password cannot contain your username".to_string());
                    is_valid = false;
                }
            }
            if let Some(email) = email {
                let email_local = email.split('@').next().unwrap_or("");
                if password.to_lowercase().contains(&email_local.to_lowercase()) {
                    feedback.push("Password cannot contain your email address".to_string());
                    is_valid = false;
                }
            }
            if let Some(info) = user_info {
                if let Some(first_name) = &info.first_name {
                    if password.to_lowercase().contains(&first_name.to_lowercase()) {
                        feedback.push("Password cannot contain your first name".to_string());
                        is_valid = false;
                    }
                }
                if let Some(last_name) = &info.last_name {
                    if password.to_lowercase().contains(&last_name.to_lowercase()) {
                        feedback.push("Password cannot contain your last name".to_string());
                        is_valid = false;
                    }
                }
            }
        }

        // Calculate password strength using zxcvbn
        let entropy_result = zxcvbn(password, &[]).unwrap_or_else(|_| zxcvbn::EntropyResult {
            score: 0,
            entropy: 0.0,
            crack_times_seconds: zxcvbn::CrackTimes {
                online_throttling_100_per_hour: 0.0,
                online_no_throttling_10_per_second: 0.0,
                offline_slow_hashing_1e4_per_second: 0.0,
                offline_fast_hashing_1e10_per_second: 0.0,
            },
            feedback: zxcvbn::Feedback {
                warning: None,
                suggestions: vec![],
            },
        });

        let strength = match entropy_result.score {
            0 => PasswordStrength::VeryWeak,
            1 => PasswordStrength::Weak,
            2 => PasswordStrength::Fair,
            3 => PasswordStrength::Good,
            4 => PasswordStrength::Strong,
            _ => PasswordStrength::VeryWeak,
        };

        // Check minimum entropy requirement
        if entropy_result.entropy < self.password_policy.min_entropy {
            feedback.push(format!("Password entropy ({:.1}) is below minimum requirement ({})", 
                                entropy_result.entropy, self.password_policy.min_entropy));
            is_valid = false;
        }

        // Add zxcvbn feedback
        if let Some(warning) = &entropy_result.feedback.warning {
            feedback.push(warning.clone());
        }
        feedback.extend(entropy_result.feedback.suggestions.clone());

        let crack_time = if entropy_result.crack_times_seconds.offline_fast_hashing_1e10_per_second < 1.0 {
            "less than a second".to_string()
        } else if entropy_result.crack_times_seconds.offline_fast_hashing_1e10_per_second < 60.0 {
            format!("{:.0} seconds", entropy_result.crack_times_seconds.offline_fast_hashing_1e10_per_second)
        } else if entropy_result.crack_times_seconds.offline_fast_hashing_1e10_per_second < 3600.0 {
            format!("{:.0} minutes", entropy_result.crack_times_seconds.offline_fast_hashing_1e10_per_second / 60.0)
        } else if entropy_result.crack_times_seconds.offline_fast_hashing_1e10_per_second < 86400.0 {
            format!("{:.0} hours", entropy_result.crack_times_seconds.offline_fast_hashing_1e10_per_second / 3600.0)
        } else {
            format!("{:.0} days", entropy_result.crack_times_seconds.offline_fast_hashing_1e10_per_second / 86400.0)
        };

        Ok(PasswordValidationResult {
            is_valid,
            strength,
            score: entropy_result.score,
            feedback,
            entropy: entropy_result.entropy,
            crack_time,
        })
    }

    /// Check if password has been breached
    pub async fn is_password_breached(&self, password: &str) -> PixelleResult<bool> {
        // Check cache first
        {
            let cache = self.breach_cache.read().unwrap();
            if let Some(result) = cache.get(password) {
                // Use cached result if it's less than 24 hours old
                if result.last_checked > Utc::now() - Duration::hours(24) {
                    return Ok(result.is_breached);
                }
            }
        }

        // In a real implementation, this would check against HaveIBeenPwned API
        // For now, we'll simulate with a simple check
        let is_breached = self.check_breach_database(password).await?;

        // Cache the result
        {
            let mut cache = self.breach_cache.write().unwrap();
            cache.insert(password.to_string(), BreachCheckResult {
                is_breached,
                breach_count: if is_breached { 1 } else { 0 },
                breach_sources: if is_breached { vec!["simulated_breach".to_string()] } else { vec![] },
                last_checked: Utc::now(),
            });
        }

        Ok(is_breached)
    }

    /// Check rate limiting for an identifier
    async fn check_rate_limit(&self, identifier: &str) -> PixelleResult<bool> {
        let now = Utc::now();
        let attempts = {
            let tracker = self.attempt_tracker.read().unwrap();
            tracker.get(identifier).cloned().unwrap_or_default()
        };

        // Clean old attempts
        let recent_attempts: Vec<DateTime<Utc>> = attempts
            .into_iter()
            .filter(|&attempt| attempt > now - Duration::days(1))
            .collect();

        // Check limits
        let minute_ago = now - Duration::minutes(1);
        let hour_ago = now - Duration::hours(1);
        let day_ago = now - Duration::days(1);

        let attempts_last_minute = recent_attempts.iter().filter(|&&attempt| attempt > minute_ago).count() as u32;
        let attempts_last_hour = recent_attempts.iter().filter(|&&attempt| attempt > hour_ago).count() as u32;
        let attempts_last_day = recent_attempts.iter().filter(|&&attempt| attempt > day_ago).count() as u32;

        if attempts_last_minute >= self.rate_limit_config.max_attempts_per_minute ||
           attempts_last_hour >= self.rate_limit_config.max_attempts_per_hour ||
           attempts_last_day >= self.rate_limit_config.max_attempts_per_day {
            warn!("Rate limit exceeded for identifier: {}", identifier);
            return Ok(false);
        }

        Ok(true)
    }

    /// Record a password attempt
    async fn record_attempt(&self, identifier: &str) {
        let now = Utc::now();
        let mut tracker = self.attempt_tracker.write().unwrap();
        let attempts = tracker.entry(identifier.to_string()).or_insert_with(Vec::new);
        attempts.push(now);

        // Keep only last 24 hours of attempts
        attempts.retain(|&attempt| attempt > now - Duration::days(1));
    }

    /// Clear failed attempts for an identifier
    async fn clear_failed_attempts(&self, identifier: &str) {
        let mut tracker = self.attempt_tracker.write().unwrap();
        tracker.remove(identifier);
    }

    /// Log failed attempt for security monitoring
    async fn log_failed_attempt(&self, identifier: &str, user_info: Option<&UserInfo>) {
        warn!("Failed password attempt for identifier: {}", identifier);
        // In a real implementation, this would log to a security monitoring system
    }

    /// Check if password has repeating characters
    fn has_repeating_chars(&self, password: &str, max_repeating: usize) -> bool {
        let chars: Vec<char> = password.chars().collect();
        let mut current_char = None;
        let mut count = 0;

        for &ch in &chars {
            if Some(ch) == current_char {
                count += 1;
                if count > max_repeating {
                    return true;
                }
            } else {
                current_char = Some(ch);
                count = 1;
            }
        }
        false
    }

    /// Check if password has sequential characters
    fn has_sequential_chars(&self, password: &str, max_sequential: usize) -> bool {
        let chars: Vec<char> = password.chars().collect();
        
        for i in 0..chars.len().saturating_sub(max_sequential) {
            let mut is_sequential = true;
            for j in 1..=max_sequential {
                if i + j >= chars.len() {
                    break;
                }
                if chars[i + j] as u32 != chars[i] as u32 + j as u32 {
                    is_sequential = false;
                    break;
                }
            }
            if is_sequential {
                return true;
            }
        }
        false
    }

    /// Check if password is in common passwords list
    fn is_common_password(&self, password: &str) -> bool {
        let common_passwords = self.common_passwords.read().unwrap();
        common_passwords.contains(&password.to_lowercase())
    }

    /// Load common passwords for validation
    fn load_common_passwords(&self) {
        let common_passwords = vec![
            "password", "123456", "123456789", "qwerty", "abc123", "password123",
            "admin", "letmein", "welcome", "monkey", "1234567890", "password1",
            "qwerty123", "dragon", "master", "hello", "freedom", "whatever",
            "qazwsx", "trustno1", "654321", "jordan23", "harley", "password1",
            "jordan", "jennifer", "zxcvbn", "asdfgh", "123123", "qwertyuiop",
        ];
        
        let mut passwords = self.common_passwords.write().unwrap();
        *passwords = common_passwords.into_iter().map(|s| s.to_string()).collect();
    }

    /// Simulate breach database check
    async fn check_breach_database(&self, password: &str) -> PixelleResult<bool> {
        // In a real implementation, this would hash the password and check against
        // HaveIBeenPwned API or similar breach database
        // For now, we'll simulate with a simple check
        let breached_passwords = vec![
            "password", "123456", "admin", "qwerty", "letmein", "welcome",
        ];
        
        Ok(breached_passwords.contains(&password.to_lowercase().as_str()))
    }

    /// Get password policy
    pub fn get_password_policy(&self) -> &PasswordPolicy {
        &self.password_policy
    }

    /// Update password policy
    pub fn update_password_policy(&mut self, policy: PasswordPolicy) {
        self.password_policy = policy;
    }

    /// Get rate limit configuration
    pub fn get_rate_limit_config(&self) -> &RateLimitConfig {
        &self.rate_limit_config
    }

    /// Update rate limit configuration
    pub fn update_rate_limit_config(&mut self, config: RateLimitConfig) {
        self.rate_limit_config = config;
    }

    /// Clean up old attempt records
    pub async fn cleanup_old_attempts(&self) -> PixelleResult<usize> {
        let now = Utc::now();
        let cutoff = now - Duration::days(7); // Keep only last 7 days
        
        let mut tracker = self.attempt_tracker.write().unwrap();
        let mut cleaned_count = 0;
        
        tracker.retain(|_, attempts| {
            let initial_count = attempts.len();
            attempts.retain(|&attempt| attempt > cutoff);
            let final_count = attempts.len();
            cleaned_count += initial_count - final_count;
            !attempts.is_empty()
        });
        
        if cleaned_count > 0 {
            debug!("Cleaned up {} old password attempt records", cleaned_count);
        }
        
        Ok(cleaned_count)
    }

    /// Clean up old breach cache entries
    pub async fn cleanup_breach_cache(&self) -> PixelleResult<usize> {
        let now = Utc::now();
        let cutoff = now - Duration::days(1); // Cache for 24 hours
        
        let mut cache = self.breach_cache.write().unwrap();
        let initial_count = cache.len();
        
        cache.retain(|_, result| result.last_checked > cutoff);
        
        let cleaned_count = initial_count - cache.len();
        if cleaned_count > 0 {
            debug!("Cleaned up {} old breach cache entries", cleaned_count);
        }
        
        Ok(cleaned_count)
    }
}

/// User information for password validation
#[derive(Debug, Clone)]
pub struct UserInfo {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub email: Option<String>,
    pub username: Option<String>,
    pub birth_date: Option<chrono::DateTime<Utc>>,
}

impl Default for PassphraseService {
    fn default() -> Self {
        Self::new()
    }
}
