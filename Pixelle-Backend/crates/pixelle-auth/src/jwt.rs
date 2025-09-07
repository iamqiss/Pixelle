use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation, Algorithm};
use serde::{Deserialize, Serialize};
use pixelle_core::{PixelleResult, UserId, PixelleError};
use chrono::{Duration, Utc};
use uuid::Uuid;
use std::collections::HashMap;
use tracing::{debug, warn, error};

/// Enhanced JWT claims with comprehensive security features
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    // Standard claims
    pub sub: String,        // Subject (User ID)
    pub exp: i64,          // Expiration time
    pub iat: i64,          // Issued at
    pub nbf: i64,          // Not before
    pub iss: String,       // Issuer
    pub aud: String,       // Audience
    pub jti: String,       // JWT ID (unique token identifier)
    
    // Custom claims for enhanced security
    pub session_id: String, // Session identifier
    pub device_id: String,  // Device identifier
    pub device_fingerprint: String, // Device fingerprint
    pub ip_address: String, // IP address at token creation
    pub user_agent: String, // User agent at token creation
    pub token_type: TokenType, // Type of token (access/refresh)
    pub roles: Vec<String>, // User roles
    pub permissions: Vec<String>, // User permissions
    pub scopes: Vec<String>, // Token scopes
    pub version: u32,       // Token version for invalidation
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TokenType {
    Access,
    Refresh,
    ApiKey,
    Service,
}

/// Token pair for access and refresh tokens
#[derive(Debug, Clone)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
    pub access_expires_at: chrono::DateTime<Utc>,
    pub refresh_expires_at: chrono::DateTime<Utc>,
}

/// Token validation result with detailed information
#[derive(Debug)]
pub struct TokenValidationResult {
    pub valid: bool,
    pub user_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub device_id: Option<String>,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
    pub scopes: Vec<String>,
    pub token_type: Option<TokenType>,
    pub expires_at: Option<chrono::DateTime<Utc>>,
    pub issued_at: Option<chrono::DateTime<Utc>>,
    pub reason: Option<String>,
}

/// JWT service configuration
#[derive(Debug, Clone)]
pub struct JwtConfig {
    pub secret: String,
    pub issuer: String,
    pub audience: String,
    pub access_token_duration: Duration,
    pub refresh_token_duration: Duration,
    pub api_key_duration: Duration,
    pub service_token_duration: Duration,
    pub algorithm: Algorithm,
    pub enable_blacklist: bool,
    pub enable_device_validation: bool,
    pub enable_ip_validation: bool,
    pub max_token_version: u32,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            secret: "your-secret-key".to_string(),
            issuer: "pixelle-auth".to_string(),
            audience: "pixelle-api".to_string(),
            access_token_duration: Duration::hours(1),
            refresh_token_duration: Duration::days(30),
            api_key_duration: Duration::days(365),
            service_token_duration: Duration::hours(24),
            algorithm: Algorithm::HS256,
            enable_blacklist: true,
            enable_device_validation: true,
            enable_ip_validation: true,
            max_token_version: 1,
        }
    }
}

/// Enterprise-grade JWT service with advanced security features
pub struct JwtService {
    config: JwtConfig,
    blacklisted_tokens: std::sync::RwLock<HashMap<String, chrono::DateTime<Utc>>>,
    token_versions: std::sync::RwLock<HashMap<Uuid, u32>>, // User ID -> Token version
}

impl JwtService {
    /// Create a new JWT service with configuration
    pub fn new(config: JwtConfig) -> Self {
        Self {
            config,
            blacklisted_tokens: std::sync::RwLock::new(HashMap::new()),
            token_versions: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Create a new JWT service with default configuration
    pub fn with_secret(secret: String) -> Self {
        let mut config = JwtConfig::default();
        config.secret = secret;
        Self::new(config)
    }

    /// Create access and refresh token pair
    pub async fn create_token_pair(
        &self,
        user_id: Uuid,
        session_id: Uuid,
        device_id: String,
        device_fingerprint: String,
        ip_address: String,
        user_agent: String,
        roles: Vec<String>,
        permissions: Vec<String>,
        scopes: Vec<String>,
    ) -> PixelleResult<TokenPair> {
        let now = Utc::now();
        let jti = Uuid::new_v4().to_string();
        
        // Get current token version for user
        let token_version = self.get_user_token_version(user_id);
        
        // Create access token
        let access_claims = Claims {
            sub: user_id.to_string(),
            exp: (now + self.config.access_token_duration).timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            jti: format!("{}_access", jti),
            session_id: session_id.to_string(),
            device_id: device_id.clone(),
            device_fingerprint: device_fingerprint.clone(),
            ip_address: ip_address.clone(),
            user_agent: user_agent.clone(),
            token_type: TokenType::Access,
            roles: roles.clone(),
            permissions: permissions.clone(),
            scopes: scopes.clone(),
            version: token_version,
        };

        let access_token = self.encode_token(&access_claims)?;
        let access_expires_at = now + self.config.access_token_duration;

        // Create refresh token
        let refresh_claims = Claims {
            sub: user_id.to_string(),
            exp: (now + self.config.refresh_token_duration).timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            jti: format!("{}_refresh", jti),
            session_id: session_id.to_string(),
            device_id,
            device_fingerprint,
            ip_address,
            user_agent,
            token_type: TokenType::Refresh,
            roles,
            permissions,
            scopes,
            version: token_version,
        };

        let refresh_token = self.encode_token(&refresh_claims)?;
        let refresh_expires_at = now + self.config.refresh_token_duration;

        debug!("Created token pair for user: {} session: {}", user_id, session_id);

        Ok(TokenPair {
            access_token,
            refresh_token,
            access_expires_at,
            refresh_expires_at,
        })
    }

    /// Create access token only
    pub async fn create_access_token(
        &self,
        user_id: Uuid,
        session_id: Uuid,
        device_id: String,
        device_fingerprint: String,
        ip_address: String,
        user_agent: String,
        roles: Vec<String>,
        permissions: Vec<String>,
        scopes: Vec<String>,
    ) -> PixelleResult<String> {
        let now = Utc::now();
        let jti = Uuid::new_v4().to_string();
        let token_version = self.get_user_token_version(user_id);

        let claims = Claims {
            sub: user_id.to_string(),
            exp: (now + self.config.access_token_duration).timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            jti,
            session_id: session_id.to_string(),
            device_id,
            device_fingerprint,
            ip_address,
            user_agent,
            token_type: TokenType::Access,
            roles,
            permissions,
            scopes,
            version: token_version,
        };

        self.encode_token(&claims)
    }

    /// Create API key token
    pub async fn create_api_key_token(
        &self,
        user_id: Uuid,
        api_key_id: Uuid,
        permissions: Vec<String>,
        scopes: Vec<String>,
    ) -> PixelleResult<String> {
        let now = Utc::now();
        let jti = Uuid::new_v4().to_string();
        let token_version = self.get_user_token_version(user_id);

        let claims = Claims {
            sub: user_id.to_string(),
            exp: (now + self.config.api_key_duration).timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            jti,
            session_id: api_key_id.to_string(),
            device_id: "api_key".to_string(),
            device_fingerprint: "api_key".to_string(),
            ip_address: "api_key".to_string(),
            user_agent: "api_key".to_string(),
            token_type: TokenType::ApiKey,
            roles: vec!["api_user".to_string()],
            permissions,
            scopes,
            version: token_version,
        };

        self.encode_token(&claims)
    }

    /// Create service token for internal services
    pub async fn create_service_token(
        &self,
        service_name: String,
        permissions: Vec<String>,
    ) -> PixelleResult<String> {
        let now = Utc::now();
        let jti = Uuid::new_v4().to_string();

        let claims = Claims {
            sub: service_name.clone(),
            exp: (now + self.config.service_token_duration).timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            jti,
            session_id: "service".to_string(),
            device_id: "service".to_string(),
            device_fingerprint: "service".to_string(),
            ip_address: "service".to_string(),
            user_agent: "service".to_string(),
            token_type: TokenType::Service,
            roles: vec!["service".to_string()],
            permissions,
            scopes: vec!["service".to_string()],
            version: 1,
        };

        self.encode_token(&claims)
    }

    /// Validate token with comprehensive security checks
    pub async fn validate_token(
        &self,
        token: &str,
        current_ip: Option<String>,
        current_user_agent: Option<String>,
        current_device_fingerprint: Option<String>,
    ) -> PixelleResult<TokenValidationResult> {
        // Check if token is blacklisted
        if self.config.enable_blacklist && self.is_token_blacklisted(token).await {
            return Ok(TokenValidationResult {
                valid: false,
                reason: Some("Token is blacklisted".to_string()),
                ..Default::default()
            });
        }

        // Decode and validate token
        let token_data = match self.decode_token(token) {
            Ok(data) => data,
            Err(e) => {
                return Ok(TokenValidationResult {
                    valid: false,
                    reason: Some(format!("Token decode error: {}", e)),
                    ..Default::default()
                });
            }
        };

        let claims = &token_data.claims;
        let user_id = match claims.sub.parse::<Uuid>() {
            Ok(id) => id,
            Err(_) => {
                return Ok(TokenValidationResult {
                    valid: false,
                    reason: Some("Invalid user ID in token".to_string()),
                    ..Default::default()
                });
            }
        };

        // Check token version
        let current_version = self.get_user_token_version(user_id);
        if claims.version < current_version {
            return Ok(TokenValidationResult {
                valid: false,
                reason: Some("Token version is outdated".to_string()),
                ..Default::default()
            });
        }

        // Validate device fingerprint if enabled
        if self.config.enable_device_validation {
            if let Some(current_fingerprint) = current_device_fingerprint {
                if claims.device_fingerprint != current_fingerprint {
                    return Ok(TokenValidationResult {
                        valid: false,
                        reason: Some("Device fingerprint mismatch".to_string()),
                        ..Default::default()
                    });
                }
            }
        }

        // Validate IP address if enabled
        if self.config.enable_ip_validation {
            if let Some(current_ip) = current_ip {
                if claims.ip_address != current_ip {
                    warn!("IP address mismatch for user: {} - token: {} current: {}", 
                          user_id, claims.ip_address, current_ip);
                    // Note: We don't invalidate the token for IP mismatch as users might have dynamic IPs
                }
            }
        }

        let session_id = match claims.session_id.parse::<Uuid>() {
            Ok(id) => Some(id),
            Err(_) => None,
        };

        Ok(TokenValidationResult {
            valid: true,
            user_id: Some(user_id),
            session_id,
            device_id: Some(claims.device_id.clone()),
            roles: claims.roles.clone(),
            permissions: claims.permissions.clone(),
            scopes: claims.scopes.clone(),
            token_type: Some(claims.token_type.clone()),
            expires_at: Some(chrono::DateTime::from_timestamp(claims.exp, 0).unwrap_or_else(|| Utc::now())),
            issued_at: Some(chrono::DateTime::from_timestamp(claims.iat, 0).unwrap_or_else(|| Utc::now())),
            reason: None,
        })
    }

    /// Refresh access token using refresh token
    pub async fn refresh_access_token(
        &self,
        refresh_token: &str,
        current_ip: Option<String>,
        current_user_agent: Option<String>,
        current_device_fingerprint: Option<String>,
    ) -> PixelleResult<TokenPair> {
        let validation_result = self.validate_token(
            refresh_token,
            current_ip.clone(),
            current_user_agent.clone(),
            current_device_fingerprint.clone(),
        ).await?;

        if !validation_result.valid {
            return Err(PixelleError::Authentication(
                validation_result.reason.unwrap_or("Invalid refresh token".to_string())
            ));
        }

        if validation_result.token_type != Some(TokenType::Refresh) {
            return Err(PixelleError::Authentication("Token is not a refresh token".to_string()));
        }

        let user_id = validation_result.user_id.unwrap();
        let session_id = validation_result.session_id.unwrap();

        // Create new token pair
        self.create_token_pair(
            user_id,
            session_id,
            validation_result.device_id.unwrap_or_default(),
            current_device_fingerprint.unwrap_or_default(),
            current_ip.unwrap_or_default(),
            current_user_agent.unwrap_or_default(),
            validation_result.roles,
            validation_result.permissions,
            validation_result.scopes,
        ).await
    }

    /// Blacklist a token
    pub async fn blacklist_token(&self, token: &str) -> PixelleResult<()> {
        if !self.config.enable_blacklist {
            return Ok(());
        }

        // Decode token to get expiration time
        let token_data = self.decode_token(token)?;
        let expires_at = chrono::DateTime::from_timestamp(token_data.claims.exp, 0)
            .unwrap_or_else(|| Utc::now());

        let mut blacklist = self.blacklisted_tokens.write().unwrap();
        blacklist.insert(token.to_string(), expires_at);

        debug!("Blacklisted token: {}", token_data.claims.jti);
        Ok(())
    }

    /// Invalidate all tokens for a user by incrementing version
    pub async fn invalidate_user_tokens(&self, user_id: Uuid) -> PixelleResult<()> {
        let mut versions = self.token_versions.write().unwrap();
        let current_version = versions.get(&user_id).unwrap_or(&0) + 1;
        versions.insert(user_id, current_version);

        info!("Invalidated all tokens for user: {} (version: {})", user_id, current_version);
        Ok(())
    }

    /// Check if user has required permission
    pub fn has_permission(&self, validation_result: &TokenValidationResult, permission: &str) -> bool {
        validation_result.permissions.contains(&permission.to_string()) ||
        validation_result.permissions.contains(&"*".to_string()) // Wildcard permission
    }

    /// Check if user has required role
    pub fn has_role(&self, validation_result: &TokenValidationResult, role: &str) -> bool {
        validation_result.roles.contains(&role.to_string()) ||
        validation_result.roles.contains(&"admin".to_string()) // Admin has all roles
    }

    /// Check if user has required scope
    pub fn has_scope(&self, validation_result: &TokenValidationResult, scope: &str) -> bool {
        validation_result.scopes.contains(&scope.to_string()) ||
        validation_result.scopes.contains(&"*".to_string()) // Wildcard scope
    }

    // Private helper methods

    fn encode_token(&self, claims: &Claims) -> PixelleResult<String> {
        let mut header = Header::new(self.config.algorithm);
        header.typ = Some("JWT".to_string());

        encode(
            &header,
            claims,
            &EncodingKey::from_secret(self.config.secret.as_ref()),
        )
        .map_err(|e| PixelleError::Internal(format!("JWT encoding error: {}", e)))
    }

    fn decode_token(&self, token: &str) -> PixelleResult<jsonwebtoken::TokenData<Claims>> {
        let mut validation = Validation::new(self.config.algorithm);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        validation.set_required_spec_claims(&["exp", "iat", "iss", "aud"]);

        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.config.secret.as_ref()),
            &validation,
        )
        .map_err(|e| PixelleError::Authentication(format!("JWT decode error: {}", e)))
    }

    async fn is_token_blacklisted(&self, token: &str) -> bool {
        let blacklist = self.blacklisted_tokens.read().unwrap();
        if let Some(expires_at) = blacklist.get(token) {
            if *expires_at > Utc::now() {
                return true;
            }
        }
        false
    }

    fn get_user_token_version(&self, user_id: Uuid) -> u32 {
        let versions = self.token_versions.read().unwrap();
        versions.get(&user_id).unwrap_or(&0).clone()
    }

    /// Clean up expired blacklisted tokens
    pub async fn cleanup_blacklist(&self) -> PixelleResult<usize> {
        let now = Utc::now();
        let mut blacklist = self.blacklisted_tokens.write().unwrap();
        let initial_count = blacklist.len();
        
        blacklist.retain(|_, expires_at| *expires_at > now);
        
        let cleaned_count = initial_count - blacklist.len();
        if cleaned_count > 0 {
            debug!("Cleaned up {} expired blacklisted tokens", cleaned_count);
        }
        
        Ok(cleaned_count)
    }

    /// Get service configuration
    pub fn config(&self) -> &JwtConfig {
        &self.config
    }

    /// Update service configuration
    pub fn update_config(&mut self, config: JwtConfig) {
        self.config = config;
    }
}

impl Default for TokenValidationResult {
    fn default() -> Self {
        Self {
            valid: false,
            user_id: None,
            session_id: None,
            device_id: None,
            roles: Vec::new(),
            permissions: Vec::new(),
            scopes: Vec::new(),
            token_type: None,
            expires_at: None,
            issued_at: None,
            reason: None,
        }
    }
}
