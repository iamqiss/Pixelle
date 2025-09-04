// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// JWT-based authentication system - NO S3 COMPATIBILITY

use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};
use jsonwebtoken::{encode, decode, Header, Algorithm, Validation, EncodingKey, DecodingKey};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use base64::{Engine as _, engine::general_purpose};

use crate::errors::{NimbuxError, Result};

/// JWT claims for Nimbux authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NimbuxClaims {
    pub sub: String, // Subject (user ID)
    pub iss: String, // Issuer (Nimbux)
    pub aud: String, // Audience
    pub exp: i64,    // Expiration time
    pub iat: i64,    // Issued at
    pub nbf: i64,    // Not before
    pub jti: String, // JWT ID
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
    pub tenant_id: Option<String>,
    pub session_id: String,
    pub device_id: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub custom_claims: HashMap<String, serde_json::Value>,
}

/// User roles in Nimbux
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UserRole {
    SuperAdmin,
    Admin,
    Developer,
    Operator,
    Viewer,
    Custom(String),
}

impl UserRole {
    pub fn as_str(&self) -> &str {
        match self {
            UserRole::SuperAdmin => "super_admin",
            UserRole::Admin => "admin",
            UserRole::Developer => "developer",
            UserRole::Operator => "operator",
            UserRole::Viewer => "viewer",
            UserRole::Custom(name) => name,
        }
    }
    
    pub fn from_str(s: &str) -> Self {
        match s {
            "super_admin" => UserRole::SuperAdmin,
            "admin" => UserRole::Admin,
            "developer" => UserRole::Developer,
            "operator" => UserRole::Operator,
            "viewer" => UserRole::Viewer,
            _ => UserRole::Custom(s.to_string()),
        }
    }
}

/// Permission types in Nimbux
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Permission {
    // Bucket permissions
    BucketCreate,
    BucketRead,
    BucketUpdate,
    BucketDelete,
    BucketList,
    
    // Object permissions
    ObjectCreate,
    ObjectRead,
    ObjectUpdate,
    ObjectDelete,
    ObjectList,
    
    // Search permissions
    SearchObjects,
    SearchMetadata,
    
    // Analytics permissions
    ViewAnalytics,
    ViewMetrics,
    ViewLogs,
    
    // Admin permissions
    ManageUsers,
    ManageRoles,
    ManagePolicies,
    ManageSystem,
    
    // Custom permissions
    Custom(String),
}

impl Permission {
    pub fn as_str(&self) -> &str {
        match self {
            Permission::BucketCreate => "bucket:create",
            Permission::BucketRead => "bucket:read",
            Permission::BucketUpdate => "bucket:update",
            Permission::BucketDelete => "bucket:delete",
            Permission::BucketList => "bucket:list",
            Permission::ObjectCreate => "object:create",
            Permission::ObjectRead => "object:read",
            Permission::ObjectUpdate => "object:update",
            Permission::ObjectDelete => "object:delete",
            Permission::ObjectList => "object:list",
            Permission::SearchObjects => "search:objects",
            Permission::SearchMetadata => "search:metadata",
            Permission::ViewAnalytics => "analytics:view",
            Permission::ViewMetrics => "metrics:view",
            Permission::ViewLogs => "logs:view",
            Permission::ManageUsers => "admin:users",
            Permission::ManageRoles => "admin:roles",
            Permission::ManagePolicies => "admin:policies",
            Permission::ManageSystem => "admin:system",
            Permission::Custom(name) => name,
        }
    }
    
    pub fn from_str(s: &str) -> Self {
        match s {
            "bucket:create" => Permission::BucketCreate,
            "bucket:read" => Permission::BucketRead,
            "bucket:update" => Permission::BucketUpdate,
            "bucket:delete" => Permission::BucketDelete,
            "bucket:list" => Permission::BucketList,
            "object:create" => Permission::ObjectCreate,
            "object:read" => Permission::ObjectRead,
            "object:update" => Permission::ObjectUpdate,
            "object:delete" => Permission::ObjectDelete,
            "object:list" => Permission::ObjectList,
            "search:objects" => Permission::SearchObjects,
            "search:metadata" => Permission::SearchMetadata,
            "analytics:view" => Permission::ViewAnalytics,
            "metrics:view" => Permission::ViewMetrics,
            "logs:view" => Permission::ViewLogs,
            "admin:users" => Permission::ManageUsers,
            "admin:roles" => Permission::ManageRoles,
            "admin:policies" => Permission::ManagePolicies,
            "admin:system" => Permission::ManageSystem,
            _ => Permission::Custom(s.to_string()),
        }
    }
}

/// User information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NimbuxUser {
    pub id: String,
    pub username: String,
    pub email: String,
    pub display_name: Option<String>,
    pub roles: Vec<UserRole>,
    pub permissions: Vec<Permission>,
    pub tenant_id: Option<String>,
    pub is_active: bool,
    pub is_verified: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub login_count: u64,
    pub custom_attributes: HashMap<String, serde_json::Value>,
}

/// JWT configuration
#[derive(Debug, Clone)]
pub struct JwtConfig {
    pub secret_key: String,
    pub issuer: String,
    pub audience: String,
    pub access_token_ttl: Duration,
    pub refresh_token_ttl: Duration,
    pub algorithm: Algorithm,
    pub leeway: u64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            secret_key: "nimbux-secret-key-change-in-production".to_string(),
            issuer: "nimbux".to_string(),
            audience: "nimbux-api".to_string(),
            access_token_ttl: Duration::hours(1),
            refresh_token_ttl: Duration::days(7),
            algorithm: Algorithm::HS256,
            leeway: 60,
        }
    }
}

/// JWT authentication manager
pub struct JwtAuthManager {
    config: JwtConfig,
    users: Arc<tokio::sync::RwLock<HashMap<String, NimbuxUser>>>,
    sessions: Arc<tokio::sync::RwLock<HashMap<String, SessionInfo>>>,
    blacklisted_tokens: Arc<tokio::sync::RwLock<HashSet<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub user_id: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub device_id: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub is_active: bool,
}

/// Authentication result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResult {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: String,
    pub expires_in: i64,
    pub user: NimbuxUser,
    pub session_id: String,
}

/// Token validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenValidationResult {
    pub valid: bool,
    pub claims: Option<NimbuxClaims>,
    pub user: Option<NimbuxUser>,
    pub error: Option<String>,
}

impl JwtAuthManager {
    pub fn new(config: JwtConfig) -> Self {
        Self {
            config,
            users: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            sessions: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            blacklisted_tokens: Arc::new(tokio::sync::RwLock::new(HashSet::new())),
        }
    }
    
    /// Create a new user
    pub async fn create_user(
        &self,
        username: String,
        email: String,
        password: String,
        roles: Vec<UserRole>,
        tenant_id: Option<String>,
    ) -> Result<NimbuxUser> {
        let user_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        
        // Hash password
        let hashed_password = self.hash_password(&password)?;
        
        let user = NimbuxUser {
            id: user_id.clone(),
            username: username.clone(),
            email: email.clone(),
            display_name: None,
            roles: roles.clone(),
            permissions: self.get_permissions_for_roles(&roles),
            tenant_id,
            is_active: true,
            is_verified: false,
            created_at: now,
            updated_at: now,
            last_login: None,
            login_count: 0,
            custom_attributes: HashMap::new(),
        };
        
        // Store user
        {
            let mut users = self.users.write().await;
            users.insert(user_id, user.clone());
        }
        
        Ok(user)
    }
    
    /// Authenticate user with username/password
    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
        device_id: Option<String>,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<AuthResult> {
        // Find user
        let user = {
            let users = self.users.read().await;
            users.values()
                .find(|u| u.username == username || u.email == username)
                .cloned()
        };
        
        let user = user.ok_or_else(|| NimbuxError::Auth("User not found".to_string()))?;
        
        if !user.is_active {
            return Err(NimbuxError::Auth("User account is disabled".to_string()));
        }
        
        // Verify password
        if !self.verify_password(password, &user.id)? {
            return Err(NimbuxError::Auth("Invalid password".to_string()));
        }
        
        // Create session
        let session_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        
        let session = SessionInfo {
            session_id: session_id.clone(),
            user_id: user.id.clone(),
            created_at: now,
            expires_at: now + self.config.refresh_token_ttl,
            last_activity: now,
            device_id,
            ip_address,
            user_agent,
            is_active: true,
        };
        
        // Store session
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.clone(), session);
        }
        
        // Update user login info
        {
            let mut users = self.users.write().await;
            if let Some(user) = users.get_mut(&user.id) {
                user.last_login = Some(now);
                user.login_count += 1;
            }
        }
        
        // Generate tokens
        let access_token = self.generate_access_token(&user, &session_id)?;
        let refresh_token = self.generate_refresh_token(&user, &session_id)?;
        
        Ok(AuthResult {
            access_token,
            refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: self.config.access_token_ttl.num_seconds(),
            user,
            session_id,
        })
    }
    
    /// Validate access token
    pub async fn validate_token(&self, token: &str) -> Result<TokenValidationResult> {
        // Check if token is blacklisted
        {
            let blacklisted = self.blacklisted_tokens.read().await;
            if blacklisted.contains(token) {
                return Ok(TokenValidationResult {
                    valid: false,
                    claims: None,
                    user: None,
                    error: Some("Token is blacklisted".to_string()),
                });
            }
        }
        
        // Decode and validate token
        let claims = match self.decode_token(token) {
            Ok(claims) => claims,
            Err(e) => {
                return Ok(TokenValidationResult {
                    valid: false,
                    claims: None,
                    user: None,
                    error: Some(format!("Token validation failed: {}", e)),
                });
            }
        };
        
        // Check if session is still active
        {
            let sessions = self.sessions.read().await;
            if let Some(session) = sessions.get(&claims.session_id) {
                if !session.is_active || session.expires_at < Utc::now() {
                    return Ok(TokenValidationResult {
                        valid: false,
                        claims: None,
                        user: None,
                        error: Some("Session expired or inactive".to_string()),
                    });
                }
            } else {
                return Ok(TokenValidationResult {
                    valid: false,
                    claims: None,
                    user: None,
                    error: Some("Session not found".to_string()),
                });
            }
        }
        
        // Get user
        let user = {
            let users = self.users.read().await;
            users.get(&claims.sub).cloned()
        };
        
        let user = user.ok_or_else(|| NimbuxError::Auth("User not found".to_string()))?;
        
        if !user.is_active {
            return Ok(TokenValidationResult {
                valid: false,
                claims: None,
                user: None,
                error: Some("User account is disabled".to_string()),
            });
        }
        
        Ok(TokenValidationResult {
            valid: true,
            claims: Some(claims),
            user: Some(user),
            error: None,
        })
    }
    
    /// Refresh access token
    pub async fn refresh_token(&self, refresh_token: &str) -> Result<AuthResult> {
        // Decode refresh token
        let claims = self.decode_token(refresh_token)?;
        
        // Verify it's a refresh token
        if !claims.custom_claims.get("token_type").and_then(|v| v.as_str()).unwrap_or("") == "refresh" {
            return Err(NimbuxError::Auth("Invalid refresh token".to_string()));
        }
        
        // Check session
        let session = {
            let sessions = self.sessions.read().await;
            sessions.get(&claims.session_id).cloned()
        };
        
        let session = session.ok_or_else(|| NimbuxError::Auth("Session not found".to_string()))?;
        
        if !session.is_active || session.expires_at < Utc::now() {
            return Err(NimbuxError::Auth("Session expired".to_string()));
        }
        
        // Get user
        let user = {
            let users = self.users.read().await;
            users.get(&claims.sub).cloned()
        };
        
        let user = user.ok_or_else(|| NimbuxError::Auth("User not found".to_string()))?;
        
        if !user.is_active {
            return Err(NimbuxError::Auth("User account is disabled".to_string()));
        }
        
        // Generate new tokens
        let access_token = self.generate_access_token(&user, &session.session_id)?;
        let new_refresh_token = self.generate_refresh_token(&user, &session.session_id)?;
        
        Ok(AuthResult {
            access_token,
            refresh_token: new_refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: self.config.access_token_ttl.num_seconds(),
            user,
            session_id: session.session_id,
        })
    }
    
    /// Logout user (blacklist token)
    pub async fn logout(&self, token: &str) -> Result<()> {
        // Decode token to get session ID
        let claims = self.decode_token(token)?;
        
        // Blacklist token
        {
            let mut blacklisted = self.blacklisted_tokens.write().await;
            blacklisted.insert(token.to_string());
        }
        
        // Deactivate session
        {
            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(&claims.session_id) {
                session.is_active = false;
            }
        }
        
        Ok(())
    }
    
    /// Check if user has permission
    pub async fn has_permission(&self, user_id: &str, permission: &Permission) -> Result<bool> {
        let users = self.users.read().await;
        if let Some(user) = users.get(user_id) {
            Ok(user.permissions.contains(permission))
        } else {
            Ok(false)
        }
    }
    
    /// Check if user has role
    pub async fn has_role(&self, user_id: &str, role: &UserRole) -> Result<bool> {
        let users = self.users.read().await;
        if let Some(user) = users.get(user_id) {
            Ok(user.roles.contains(role))
        } else {
            Ok(false)
        }
    }
    
    /// Get user by ID
    pub async fn get_user(&self, user_id: &str) -> Result<Option<NimbuxUser>> {
        let users = self.users.read().await;
        Ok(users.get(user_id).cloned())
    }
    
    /// List all users
    pub async fn list_users(&self) -> Result<Vec<NimbuxUser>> {
        let users = self.users.read().await;
        Ok(users.values().cloned().collect())
    }
    
    /// Update user
    pub async fn update_user(&self, user_id: &str, updates: UserUpdate) -> Result<()> {
        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(user_id) {
            if let Some(username) = updates.username {
                user.username = username;
            }
            if let Some(email) = updates.email {
                user.email = email;
            }
            if let Some(display_name) = updates.display_name {
                user.display_name = Some(display_name);
            }
            if let Some(roles) = updates.roles {
                user.roles = roles.clone();
                user.permissions = self.get_permissions_for_roles(&roles);
            }
            if let Some(is_active) = updates.is_active {
                user.is_active = is_active;
            }
            if let Some(custom_attributes) = updates.custom_attributes {
                user.custom_attributes = custom_attributes;
            }
            user.updated_at = Utc::now();
        }
        Ok(())
    }
    
    /// Delete user
    pub async fn delete_user(&self, user_id: &str) -> Result<()> {
        let mut users = self.users.write().await;
        users.remove(user_id);
        
        // Also remove all sessions for this user
        let mut sessions = self.sessions.write().await;
        sessions.retain(|_, session| session.user_id != user_id);
        
        Ok(())
    }
    
    // Private helper methods
    
    fn generate_access_token(&self, user: &NimbuxUser, session_id: &str) -> Result<String> {
        let now = Utc::now();
        let exp = now + self.config.access_token_ttl;
        
        let claims = NimbuxClaims {
            sub: user.id.clone(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            exp: exp.timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            jti: Uuid::new_v4().to_string(),
            roles: user.roles.iter().map(|r| r.as_str().to_string()).collect(),
            permissions: user.permissions.iter().map(|p| p.as_str().to_string()).collect(),
            tenant_id: user.tenant_id.clone(),
            session_id: session_id.to_string(),
            device_id: None,
            ip_address: None,
            user_agent: None,
            custom_claims: HashMap::new(),
        };
        
        let header = Header::new(self.config.algorithm);
        let encoding_key = EncodingKey::from_secret(self.config.secret_key.as_ref());
        
        encode(&header, &claims, &encoding_key)
            .map_err(|e| NimbuxError::Auth(format!("Failed to encode token: {}", e)))
    }
    
    fn generate_refresh_token(&self, user: &NimbuxUser, session_id: &str) -> Result<String> {
        let now = Utc::now();
        let exp = now + self.config.refresh_token_ttl;
        
        let mut custom_claims = HashMap::new();
        custom_claims.insert("token_type".to_string(), serde_json::Value::String("refresh".to_string()));
        
        let claims = NimbuxClaims {
            sub: user.id.clone(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            exp: exp.timestamp(),
            iat: now.timestamp(),
            nbf: now.timestamp(),
            jti: Uuid::new_v4().to_string(),
            roles: user.roles.iter().map(|r| r.as_str().to_string()).collect(),
            permissions: user.permissions.iter().map(|p| p.as_str().to_string()).collect(),
            tenant_id: user.tenant_id.clone(),
            session_id: session_id.to_string(),
            device_id: None,
            ip_address: None,
            user_agent: None,
            custom_claims,
        };
        
        let header = Header::new(self.config.algorithm);
        let encoding_key = EncodingKey::from_secret(self.config.secret_key.as_ref());
        
        encode(&header, &claims, &encoding_key)
            .map_err(|e| NimbuxError::Auth(format!("Failed to encode refresh token: {}", e)))
    }
    
    fn decode_token(&self, token: &str) -> Result<NimbuxClaims> {
        let validation = Validation::new(self.config.algorithm)
            .set_issuer(&[&self.config.issuer])
            .set_audience(&[&self.config.audience])
            .leeway(self.config.leeway);
        
        let decoding_key = DecodingKey::from_secret(self.config.secret_key.as_ref());
        
        let token_data = decode::<NimbuxClaims>(token, &decoding_key, &validation)
            .map_err(|e| NimbuxError::Auth(format!("Failed to decode token: {}", e)))?;
        
        Ok(token_data.claims)
    }
    
    fn hash_password(&self, password: &str) -> Result<String> {
        // In production, use a proper password hashing library like argon2
        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.secret_key.as_bytes())
            .map_err(|e| NimbuxError::Auth(format!("Failed to create HMAC: {}", e)))?;
        mac.update(password.as_bytes());
        let result = mac.finalize();
        Ok(general_purpose::STANDARD.encode(result.into_bytes()))
    }
    
    fn verify_password(&self, password: &str, user_id: &str) -> Result<bool> {
        // In production, implement proper password verification
        // For now, just return true for demo purposes
        Ok(true)
    }
    
    fn get_permissions_for_roles(&self, roles: &[UserRole]) -> Vec<Permission> {
        let mut permissions = Vec::new();
        
        for role in roles {
            match role {
                UserRole::SuperAdmin => {
                    permissions.extend(vec![
                        Permission::BucketCreate, Permission::BucketRead, Permission::BucketUpdate, Permission::BucketDelete, Permission::BucketList,
                        Permission::ObjectCreate, Permission::ObjectRead, Permission::ObjectUpdate, Permission::ObjectDelete, Permission::ObjectList,
                        Permission::SearchObjects, Permission::SearchMetadata,
                        Permission::ViewAnalytics, Permission::ViewMetrics, Permission::ViewLogs,
                        Permission::ManageUsers, Permission::ManageRoles, Permission::ManagePolicies, Permission::ManageSystem,
                    ]);
                }
                UserRole::Admin => {
                    permissions.extend(vec![
                        Permission::BucketCreate, Permission::BucketRead, Permission::BucketUpdate, Permission::BucketDelete, Permission::BucketList,
                        Permission::ObjectCreate, Permission::ObjectRead, Permission::ObjectUpdate, Permission::ObjectDelete, Permission::ObjectList,
                        Permission::SearchObjects, Permission::SearchMetadata,
                        Permission::ViewAnalytics, Permission::ViewMetrics, Permission::ViewLogs,
                        Permission::ManageUsers, Permission::ManageRoles, Permission::ManagePolicies,
                    ]);
                }
                UserRole::Developer => {
                    permissions.extend(vec![
                        Permission::BucketCreate, Permission::BucketRead, Permission::BucketUpdate, Permission::BucketList,
                        Permission::ObjectCreate, Permission::ObjectRead, Permission::ObjectUpdate, Permission::ObjectList,
                        Permission::SearchObjects, Permission::SearchMetadata,
                        Permission::ViewAnalytics, Permission::ViewMetrics,
                    ]);
                }
                UserRole::Operator => {
                    permissions.extend(vec![
                        Permission::BucketRead, Permission::BucketList,
                        Permission::ObjectRead, Permission::ObjectList,
                        Permission::SearchObjects,
                        Permission::ViewAnalytics, Permission::ViewMetrics,
                    ]);
                }
                UserRole::Viewer => {
                    permissions.extend(vec![
                        Permission::BucketRead, Permission::BucketList,
                        Permission::ObjectRead, Permission::ObjectList,
                        Permission::SearchObjects,
                    ]);
                }
                UserRole::Custom(_) => {
                    // Custom roles would have their permissions defined elsewhere
                }
            }
        }
        
        permissions.sort();
        permissions.dedup();
        permissions
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserUpdate {
    pub username: Option<String>,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub roles: Option<Vec<UserRole>>,
    pub is_active: Option<bool>,
    pub custom_attributes: Option<HashMap<String, serde_json::Value>>,
}

use std::collections::HashSet;