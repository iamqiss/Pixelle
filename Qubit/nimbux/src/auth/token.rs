// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Token-based auth

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use hmac::{Hmac, Mac};
use sha2::{Sha256, Digest};
use base64::Engine;
use uuid::Uuid;
use tracing::{info, warn, error, debug};

use crate::errors::{NimbuxError, Result};

/// HMAC type for signature verification
type HmacSha256 = Hmac<Sha256>;

/// Access key for authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessKey {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub user_id: String,
    pub created_at: u64,
    pub last_used: Option<u64>,
    pub status: KeyStatus,
}

/// Key status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KeyStatus {
    Active,
    Inactive,
    Suspended,
}

/// IAM policy document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDocument {
    pub version: String,
    pub statement: Vec<PolicyStatement>,
}

/// Individual policy statement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyStatement {
    pub effect: String, // "Allow" or "Deny"
    pub action: Vec<String>,
    pub resource: Vec<String>,
    pub condition: Option<HashMap<String, serde_json::Value>>,
}

/// User with associated policies and keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_id: String,
    pub username: String,
    pub email: String,
    pub policies: Vec<PolicyDocument>,
    pub access_keys: Vec<AccessKey>,
    pub created_at: u64,
    pub last_login: Option<u64>,
}

/// Authentication context for requests
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user: User,
    pub access_key: AccessKey,
    pub request_time: u64,
    pub signature: String,
}

/// AWS Signature V4 implementation for Nimbux
pub struct SignatureV4 {
    access_key_id: String,
    secret_access_key: String,
    region: String,
    service: String,
}

impl SignatureV4 {
    pub fn new(access_key_id: String, secret_access_key: String, region: String) -> Self {
        Self {
            access_key_id,
            secret_access_key,
            region,
            service: "nimbux".to_string(),
        }
    }

    /// Generate signature for a request
    pub fn sign_request(
        &self,
        method: &str,
        uri: &str,
        query_string: &str,
        headers: &HashMap<String, String>,
        payload_hash: &str,
        timestamp: &str,
    ) -> Result<String> {
        // Create canonical request
        let canonical_request = self.create_canonical_request(
            method, uri, query_string, headers, payload_hash,
        )?;

        debug!("Canonical request: {}", canonical_request);

        // Create string to sign
        let date_stamp = &timestamp[..8];
        let credential_scope = format!("{}/{}/{}/aws4_request", date_stamp, self.region, self.service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            timestamp,
            credential_scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );

        debug!("String to sign: {}", string_to_sign);

        // Calculate signature
        let signature = self.calculate_signature(&string_to_sign, date_stamp)?;

        Ok(signature)
    }

    /// Create canonical request
    fn create_canonical_request(
        &self,
        method: &str,
        uri: &str,
        query_string: &str,
        headers: &HashMap<String, String>,
        payload_hash: &str,
    ) -> Result<String> {
        // Sort headers
        let mut sorted_headers: Vec<_> = headers.iter().collect();
        sorted_headers.sort_by_key(|(k, _)| k.to_lowercase());

        // Create canonical headers
        let canonical_headers = sorted_headers
            .iter()
            .map(|(k, v)| format!("{}:{}", k.to_lowercase(), v.trim()))
            .collect::<Vec<_>>()
            .join("\n");

        // Create signed headers
        let signed_headers = sorted_headers
            .iter()
            .map(|(k, _)| k.to_lowercase())
            .collect::<Vec<_>>()
            .join(";");

        Ok(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            uri,
            query_string,
            canonical_headers,
            signed_headers,
            payload_hash
        ))
    }

    /// Calculate the final signature
    fn calculate_signature(&self, string_to_sign: &str, date_stamp: &str) -> Result<String> {
        // Create signing key
        let signing_key = self.get_signing_key(date_stamp)?;

        // Calculate signature
        let mut mac = HmacSha256::new_from_slice(&signing_key)
            .map_err(|e| NimbuxError::Authentication(format!("Failed to create HMAC: {}", e)))?;
        mac.update(string_to_sign.as_bytes());
        let signature = mac.finalize();

        Ok(hex::encode(signature.into_bytes()))
    }

    /// Get signing key for the date
    fn get_signing_key(&self, date_stamp: &str) -> Result<Vec<u8>> {
        let mut key = format!("AWS4{}", self.secret_access_key).into_bytes();

        // Date key
        let mut mac = HmacSha256::new_from_slice(&key)
            .map_err(|e| NimbuxError::Authentication(format!("Failed to create HMAC: {}", e)))?;
        mac.update(date_stamp.as_bytes());
        key = mac.finalize().into_bytes();

        // Region key
        let mut mac = HmacSha256::new_from_slice(&key)
            .map_err(|e| NimbuxError::Authentication(format!("Failed to create HMAC: {}", e)))?;
        mac.update(self.region.as_bytes());
        key = mac.finalize().into_bytes();

        // Service key
        let mut mac = HmacSha256::new_from_slice(&key)
            .map_err(|e| NimbuxError::Authentication(format!("Failed to create HMAC: {}", e)))?;
        mac.update(self.service.as_bytes());
        key = mac.finalize().into_bytes();

        // Request key
        let mut mac = HmacSha256::new_from_slice(&key)
            .map_err(|e| NimbuxError::Authentication(format!("Failed to create HMAC: {}", e)))?;
        mac.update(b"aws4_request");
        key = mac.finalize().into_bytes();

        Ok(key)
    }
}

/// Authentication manager for Nimbux
pub struct AuthManager {
    users: Arc<tokio::sync::RwLock<HashMap<String, User>>>,
    access_keys: Arc<tokio::sync::RwLock<HashMap<String, AccessKey>>>,
}

impl AuthManager {
    /// Create a new authentication manager
    pub fn new() -> Self {
        Self {
            users: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            access_keys: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Create a new user
    pub async fn create_user(
        &self,
        username: String,
        email: String,
    ) -> Result<User> {
        let user_id = Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let user = User {
            user_id: user_id.clone(),
            username: username.clone(),
            email,
            policies: Vec::new(),
            access_keys: Vec::new(),
            created_at: now,
            last_login: None,
        };

        let mut users = self.users.write().await;
        users.insert(user_id.clone(), user.clone());

        info!("Created user: {} ({})", username, user_id);
        Ok(user)
    }

    /// Create access key for user
    pub async fn create_access_key(&self, user_id: &str) -> Result<AccessKey> {
        let access_key_id = format!("NIMB{}", Uuid::new_v4().to_string().replace('-', ""));
        let secret_access_key = base64::engine::general_purpose::STANDARD
            .encode(Uuid::new_v4().as_bytes());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let access_key = AccessKey {
            access_key_id: access_key_id.clone(),
            secret_access_key,
            user_id: user_id.to_string(),
            created_at: now,
            last_used: None,
            status: KeyStatus::Active,
        };

        // Add to access keys map
        let mut access_keys = self.access_keys.write().await;
        access_keys.insert(access_key_id.clone(), access_key.clone());

        // Add to user's access keys
        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(user_id) {
            user.access_keys.push(access_key.clone());
        }

        info!("Created access key for user: {}", user_id);
        Ok(access_key)
    }

    /// Authenticate request using AWS Signature V4
    pub async fn authenticate_request(
        &self,
        access_key_id: &str,
        signature: &str,
        method: &str,
        uri: &str,
        query_string: &str,
        headers: &HashMap<String, String>,
        payload_hash: &str,
        timestamp: &str,
    ) -> Result<AuthContext> {
        // Get access key
        let access_keys = self.access_keys.read().await;
        let access_key = access_keys.get(access_key_id)
            .ok_or_else(|| NimbuxError::Authentication("Invalid access key".to_string()))?;

        if access_key.status != KeyStatus::Active {
            return Err(NimbuxError::Authentication("Access key is not active".to_string()));
        }

        // Get user
        let users = self.users.read().await;
        let user = users.get(&access_key.user_id)
            .ok_or_else(|| NimbuxError::Authentication("User not found".to_string()))?;

        // Verify signature
        let signer = SignatureV4::new(
            access_key.access_key_id.clone(),
            access_key.secret_access_key.clone(),
            "us-east-1".to_string(), // Default region
        );

        let expected_signature = signer.sign_request(
            method, uri, query_string, headers, payload_hash, timestamp,
        )?;

        if signature != expected_signature {
            warn!("Signature mismatch for access key: {}", access_key_id);
            return Err(NimbuxError::Authentication("Invalid signature".to_string()));
        }

        // Update last used time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        drop(users);
        drop(access_keys);

        let mut access_keys = self.access_keys.write().await;
        if let Some(key) = access_keys.get_mut(access_key_id) {
            key.last_used = Some(now);
        }

        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(&access_key.user_id) {
            user.last_login = Some(now);
        }

        Ok(AuthContext {
            user: user.clone(),
            access_key: access_key.clone(),
            request_time: now,
            signature: signature.to_string(),
        })
    }

    /// Check if user has permission for action on resource
    pub async fn check_permission(
        &self,
        auth_context: &AuthContext,
        action: &str,
        resource: &str,
    ) -> Result<bool> {
        for policy in &auth_context.user.policies {
            for statement in &policy.statement {
                // Check if action matches
                let action_matches = statement.action.iter().any(|a| {
                    a == "*" || a == action || a.ends_with("*") && action.starts_with(&a[..a.len()-1])
                });

                if !action_matches {
                    continue;
                }

                // Check if resource matches
                let resource_matches = statement.resource.iter().any(|r| {
                    r == "*" || r == resource || r.ends_with("*") && resource.starts_with(&r[..r.len()-1])
                });

                if !resource_matches {
                    continue;
                }

                // Check effect
                match statement.effect.as_str() {
                    "Allow" => return Ok(true),
                    "Deny" => return Ok(false),
                    _ => continue,
                }
            }
        }

        // Default deny
        Ok(false)
    }

    /// Add policy to user
    pub async fn add_policy(&self, user_id: &str, policy: PolicyDocument) -> Result<()> {
        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(user_id) {
            user.policies.push(policy);
            info!("Added policy to user: {}", user_id);
        } else {
            return Err(NimbuxError::Authentication("User not found".to_string()));
        }
        Ok(())
    }

    /// Get user by ID
    pub async fn get_user(&self, user_id: &str) -> Result<Option<User>> {
        let users = self.users.read().await;
        Ok(users.get(user_id).cloned())
    }

    /// List all users
    pub async fn list_users(&self) -> Result<Vec<User>> {
        let users = self.users.read().await;
        Ok(users.values().cloned().collect())
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}