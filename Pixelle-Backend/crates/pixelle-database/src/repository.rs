use anyhow::Result;
use largetable::{Client, Database, Collection, Document, DocumentId, StorageEngine};
use crate::models::*;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

/// Enterprise-grade user repository with largetable integration
pub struct UserRepository {
    client: Client,
    database: Database,
    users_collection: Collection,
    sessions_collection: Collection,
    auth_attempts_collection: Collection,
    security_events_collection: Collection,
    roles_collection: Collection,
}

impl UserRepository {
    /// Initialize the repository with largetable client
    pub async fn new() -> Result<Self> {
        let client = Client::new("pixelle_auth".to_string(), StorageEngine::Lsm).await?;
        let database = client.database("pixelle_auth".to_string()).await?;
        
        let users_collection = database.collection("users".to_string()).await?;
        let sessions_collection = database.collection("sessions".to_string()).await?;
        let auth_attempts_collection = database.collection("auth_attempts".to_string()).await?;
        let security_events_collection = database.collection("security_events".to_string()).await?;
        let roles_collection = database.collection("roles".to_string()).await?;

        // Create indexes for optimal performance
        Self::create_indexes(&users_collection, &sessions_collection, &auth_attempts_collection).await?;

        Ok(Self {
            client,
            database,
            users_collection,
            sessions_collection,
            auth_attempts_collection,
            security_events_collection,
            roles_collection,
        })
    }

    /// Create database indexes for optimal query performance
    async fn create_indexes(
        users_collection: &Collection,
        sessions_collection: &Collection,
        auth_attempts_collection: &Collection,
    ) -> Result<()> {
        // User indexes
        users_collection.create_index("username".to_string(), largetable::IndexType::Hash).await?;
        users_collection.create_index("email".to_string(), largetable::IndexType::Hash).await?;
        users_collection.create_index("created_at".to_string(), largetable::IndexType::BTree).await?;
        users_collection.create_index("status".to_string(), largetable::IndexType::Hash).await?;

        // Session indexes
        sessions_collection.create_index("user_id".to_string(), largetable::IndexType::Hash).await?;
        sessions_collection.create_index("access_token".to_string(), largetable::IndexType::Hash).await?;
        sessions_collection.create_index("refresh_token".to_string(), largetable::IndexType::Hash).await?;
        sessions_collection.create_index("expires_at".to_string(), largetable::IndexType::BTree).await?;
        sessions_collection.create_index("device_id".to_string(), largetable::IndexType::Hash).await?;
        sessions_collection.create_index("ip_address".to_string(), largetable::IndexType::Hash).await?;

        // Auth attempts indexes
        auth_attempts_collection.create_index("username".to_string(), largetable::IndexType::Hash).await?;
        auth_attempts_collection.create_index("ip_address".to_string(), largetable::IndexType::Hash).await?;
        auth_attempts_collection.create_index("timestamp".to_string(), largetable::IndexType::BTree).await?;
        auth_attempts_collection.create_index("success".to_string(), largetable::IndexType::Hash).await?;

        info!("Created database indexes for optimal performance");
        Ok(())
    }

    // === USER MANAGEMENT ===

    /// Create a new user with comprehensive validation
    pub async fn create_user(&self, user: &User) -> Result<DocumentId> {
        // Validate user data
        self.validate_user_data(user)?;

        // Check for existing username/email
        if self.find_by_username(&user.username).await?.is_some() {
            return Err(anyhow::anyhow!("Username already exists"));
        }
        if self.find_by_email(&user.email).await?.is_some() {
            return Err(anyhow::anyhow!("Email already exists"));
        }

        let document = user.to_document();
        let user_id = self.users_collection.insert(document).await?;

        // Log security event
        self.log_security_event(&SecurityEvent {
            id: Uuid::new_v4(),
            user_id: Some(user.id),
            event_type: SecurityEventType::LoginAttempt,
            severity: SecuritySeverity::Low,
            description: "User account created".to_string(),
            ip_address: "system".to_string(),
            user_agent: "system".to_string(),
            metadata: HashMap::new(),
            timestamp: Utc::now(),
            resolved: true,
            resolved_at: Some(Utc::now()),
        }).await?;

        info!("Created user: {} ({})", user.username, user.id);
        Ok(user_id)
    }

    /// Find user by ID
    pub async fn find_by_id(&self, id: &Uuid) -> Result<Option<User>> {
        let document = self.users_collection.find_by_id(id).await?;
        match document {
            Some(doc) => {
                let user = User::from_document(doc)?;
                Ok(Some(user))
            }
            None => Ok(None),
        }
    }

    /// Find user by username
    pub async fn find_by_username(&self, username: &str) -> Result<Option<User>> {
        // For now, we'll scan all users. In production, this would use the index
        let documents = self.users_collection.find_many(None, 1000).await?;
        
        for (_, doc) in documents {
            if let Some(Value::String(u)) = doc.fields.get("username") {
                if u == username {
                    return Ok(Some(User::from_document(doc)?));
                }
            }
        }
        Ok(None)
    }

    /// Find user by email
    pub async fn find_by_email(&self, email: &str) -> Result<Option<User>> {
        let documents = self.users_collection.find_many(None, 1000).await?;
        
        for (_, doc) in documents {
            if let Some(Value::String(e)) = doc.fields.get("email") {
                if e == email {
                    return Ok(Some(User::from_document(doc)?));
                }
            }
        }
        Ok(None)
    }

    /// Update user with comprehensive validation
    pub async fn update_user(&self, user: &User) -> Result<Option<User>> {
        self.validate_user_data(user)?;

        let document = user.to_document();
        let updated_doc = self.users_collection.update_by_id(&user.id, document).await?;

        match updated_doc {
            Some(doc) => {
                let updated_user = User::from_document(doc)?;
                info!("Updated user: {} ({})", updated_user.username, updated_user.id);
                Ok(Some(updated_user))
            }
            None => Ok(None),
        }
    }

    /// Delete user (soft delete by setting status to Banned)
    pub async fn delete_user(&self, id: &Uuid) -> Result<bool> {
        if let Some(mut user) = self.find_by_id(id).await? {
            user.status = UserStatus::Banned;
            user.updated_at = Utc::now();
            
            self.update_user(&user).await?;
            
            // Revoke all active sessions
            self.revoke_all_user_sessions(id).await?;
            
            info!("Soft deleted user: {} ({})", user.username, user.id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Validate user data
    fn validate_user_data(&self, user: &User) -> Result<()> {
        if user.username.is_empty() || user.username.len() < 3 {
            return Err(anyhow::anyhow!("Username must be at least 3 characters"));
        }
        if user.username.len() > 50 {
            return Err(anyhow::anyhow!("Username must be less than 50 characters"));
        }
        if !user.email.contains('@') {
            return Err(anyhow::anyhow!("Invalid email format"));
        }
        if user.password_hash.is_empty() {
            return Err(anyhow::anyhow!("Password hash cannot be empty"));
        }
        Ok(())
    }

    // === SESSION MANAGEMENT ===

    /// Create a new session
    pub async fn create_session(&self, session: &Session) -> Result<DocumentId> {
        let document = session.to_document();
        let session_id = self.sessions_collection.insert(document).await?;

        debug!("Created session: {} for user: {}", session.id, session.user_id);
        Ok(session_id)
    }

    /// Find session by access token
    pub async fn find_session_by_access_token(&self, access_token: &str) -> Result<Option<Session>> {
        let documents = self.sessions_collection.find_many(None, 1000).await?;
        
        for (_, doc) in documents {
            if let Some(Value::String(token)) = doc.fields.get("access_token") {
                if token == access_token {
                    return Ok(Some(Session::from_document(doc)?));
                }
            }
        }
        Ok(None)
    }

    /// Find session by refresh token
    pub async fn find_session_by_refresh_token(&self, refresh_token: &str) -> Result<Option<Session>> {
        let documents = self.sessions_collection.find_many(None, 1000).await?;
        
        for (_, doc) in documents {
            if let Some(Value::String(token)) = doc.fields.get("refresh_token") {
                if token == refresh_token {
                    return Ok(Some(Session::from_document(doc)?));
                }
            }
        }
        Ok(None)
    }

    /// Find all active sessions for a user
    pub async fn find_user_sessions(&self, user_id: &Uuid) -> Result<Vec<Session>> {
        let documents = self.sessions_collection.find_many(None, 1000).await?;
        let mut sessions = Vec::new();
        
        for (_, doc) in documents {
            if let Some(Value::ObjectId(uid)) = doc.fields.get("user_id") {
                if *uid == *user_id {
                    if let Some(Value::Bool(active)) = doc.fields.get("is_active") {
                        if *active {
                            sessions.push(Session::from_document(doc)?);
                        }
                    }
                }
            }
        }
        Ok(sessions)
    }

    /// Update session
    pub async fn update_session(&self, session: &Session) -> Result<Option<Session>> {
        let document = session.to_document();
        let updated_doc = self.sessions_collection.update_by_id(&session.id, document).await?;

        match updated_doc {
            Some(doc) => Ok(Some(Session::from_document(doc)?)),
            None => Ok(None),
        }
    }

    /// Revoke a specific session
    pub async fn revoke_session(&self, session_id: &Uuid) -> Result<bool> {
        if let Some(mut session) = self.find_session_by_id(session_id).await? {
            session.is_active = false;
            session.updated_at = Utc::now();
            
            self.update_session(&session).await?;
            debug!("Revoked session: {}", session_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Revoke all sessions for a user
    pub async fn revoke_all_user_sessions(&self, user_id: &Uuid) -> Result<usize> {
        let sessions = self.find_user_sessions(user_id).await?;
        let mut revoked_count = 0;

        for session in sessions {
            if self.revoke_session(&session.id).await? {
                revoked_count += 1;
            }
        }

        info!("Revoked {} sessions for user: {}", revoked_count, user_id);
        Ok(revoked_count)
    }

    /// Find session by ID
    async fn find_session_by_id(&self, session_id: &Uuid) -> Result<Option<Session>> {
        let document = self.sessions_collection.find_by_id(session_id).await?;
        match document {
            Some(doc) => Ok(Some(Session::from_document(doc)?)),
            None => Ok(None),
        }
    }

    /// Clean up expired sessions
    pub async fn cleanup_expired_sessions(&self) -> Result<usize> {
        let documents = self.sessions_collection.find_many(None, 1000).await?;
        let now = Utc::now();
        let mut cleaned_count = 0;

        for (_, doc) in documents {
            if let Some(Value::Timestamp(expires_at)) = doc.fields.get("expires_at") {
                if *expires_at < now.timestamp_micros() {
                    if let Some(Value::ObjectId(session_id)) = doc.fields.get("id") {
                        if self.revoke_session(session_id).await? {
                            cleaned_count += 1;
                        }
                    }
                }
            }
        }

        if cleaned_count > 0 {
            info!("Cleaned up {} expired sessions", cleaned_count);
        }
        Ok(cleaned_count)
    }

    // === AUTHENTICATION ATTEMPTS ===

    /// Log authentication attempt
    pub async fn log_auth_attempt(&self, attempt: &AuthAttempt) -> Result<DocumentId> {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::ObjectId(attempt.id));
        fields.insert("username".to_string(), Value::String(attempt.username.clone()));
        fields.insert("ip_address".to_string(), Value::String(attempt.ip_address.clone()));
        fields.insert("user_agent".to_string(), Value::String(attempt.user_agent.clone()));
        fields.insert("success".to_string(), Value::Bool(attempt.success));
        fields.insert("timestamp".to_string(), Value::Timestamp(attempt.timestamp.timestamp_micros()));
        
        if let Some(user_id) = attempt.user_id {
            fields.insert("user_id".to_string(), Value::ObjectId(user_id));
        }
        if let Some(reason) = &attempt.failure_reason {
            fields.insert("failure_reason".to_string(), Value::String(reason.clone()));
        }
        if let Some(location) = &attempt.location {
            fields.insert("location".to_string(), Value::String(location.clone()));
        }
        if let Some(fingerprint) = &attempt.device_fingerprint {
            fields.insert("device_fingerprint".to_string(), Value::String(fingerprint.clone()));
        }

        let document = Document {
            id: attempt.id,
            fields,
            version: 1,
            created_at: attempt.timestamp.timestamp_micros(),
            updated_at: attempt.timestamp.timestamp_micros(),
        };

        let attempt_id = self.auth_attempts_collection.insert(document).await?;
        Ok(attempt_id)
    }

    /// Get recent failed attempts for rate limiting
    pub async fn get_recent_failed_attempts(&self, username: &str, ip_address: &str, minutes: i64) -> Result<Vec<AuthAttempt>> {
        let documents = self.auth_attempts_collection.find_many(None, 1000).await?;
        let cutoff_time = Utc::now().timestamp_micros() - (minutes * 60 * 1_000_000);
        let mut attempts = Vec::new();

        for (_, doc) in documents {
            if let Some(Value::String(u)) = doc.fields.get("username") {
                if u == username {
                    if let Some(Value::String(ip)) = doc.fields.get("ip_address") {
                        if ip == ip_address {
                            if let Some(Value::Timestamp(timestamp)) = doc.fields.get("timestamp") {
                                if *timestamp > cutoff_time {
                                    if let Some(Value::Bool(success)) = doc.fields.get("success") {
                                        if !success {
                                            attempts.push(AuthAttempt {
                                                id: doc.id,
                                                user_id: doc.fields.get("user_id").and_then(|v| v.as_object_id()),
                                                username: u.clone(),
                                                ip_address: ip.clone(),
                                                user_agent: doc.fields.get("user_agent").and_then(|v| v.as_string()).unwrap_or_default(),
                                                success: *success,
                                                failure_reason: doc.fields.get("failure_reason").and_then(|v| v.as_string()),
                                                timestamp: DateTime::from_timestamp_micros(*timestamp).unwrap_or_else(|| Utc::now()),
                                                location: doc.fields.get("location").and_then(|v| v.as_string()),
                                                device_fingerprint: doc.fields.get("device_fingerprint").and_then(|v| v.as_string()),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(attempts)
    }

    // === SECURITY EVENTS ===

    /// Log security event
    pub async fn log_security_event(&self, event: &SecurityEvent) -> Result<DocumentId> {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::ObjectId(event.id));
        fields.insert("event_type".to_string(), Value::String(format!("{:?}", event.event_type)));
        fields.insert("severity".to_string(), Value::String(format!("{:?}", event.severity)));
        fields.insert("description".to_string(), Value::String(event.description.clone()));
        fields.insert("ip_address".to_string(), Value::String(event.ip_address.clone()));
        fields.insert("user_agent".to_string(), Value::String(event.user_agent.clone()));
        fields.insert("timestamp".to_string(), Value::Timestamp(event.timestamp.timestamp_micros()));
        fields.insert("resolved".to_string(), Value::Bool(event.resolved));
        
        if let Some(user_id) = event.user_id {
            fields.insert("user_id".to_string(), Value::ObjectId(user_id));
        }
        if let Some(resolved_at) = event.resolved_at {
            fields.insert("resolved_at".to_string(), Value::Timestamp(resolved_at.timestamp_micros()));
        }
        fields.insert("metadata".to_string(), Value::String(serde_json::to_string(&event.metadata).unwrap_or_default()));

        let document = Document {
            id: event.id,
            fields,
            version: 1,
            created_at: event.timestamp.timestamp_micros(),
            updated_at: event.timestamp.timestamp_micros(),
        };

        let event_id = self.security_events_collection.insert(document).await?;
        Ok(event_id)
    }

    /// Get security events for a user
    pub async fn get_user_security_events(&self, user_id: &Uuid, limit: usize) -> Result<Vec<SecurityEvent>> {
        let documents = self.security_events_collection.find_many(None, limit).await?;
        let mut events = Vec::new();

        for (_, doc) in documents {
            if let Some(Value::ObjectId(uid)) = doc.fields.get("user_id") {
                if *uid == *user_id {
                    events.push(SecurityEvent {
                        id: doc.id,
                        user_id: Some(*uid),
                        event_type: doc.fields.get("event_type")
                            .and_then(|v| v.as_string())
                            .and_then(|s| serde_json::from_str(&format!("\"{}\"", s)).ok())
                            .unwrap_or(SecurityEventType::LoginAttempt),
                        severity: doc.fields.get("severity")
                            .and_then(|v| v.as_string())
                            .and_then(|s| serde_json::from_str(&format!("\"{}\"", s)).ok())
                            .unwrap_or(SecuritySeverity::Low),
                        description: doc.fields.get("description").and_then(|v| v.as_string()).unwrap_or_default(),
                        ip_address: doc.fields.get("ip_address").and_then(|v| v.as_string()).unwrap_or_default(),
                        user_agent: doc.fields.get("user_agent").and_then(|v| v.as_string()).unwrap_or_default(),
                        metadata: doc.fields.get("metadata")
                            .and_then(|v| v.as_string())
                            .and_then(|s| serde_json::from_str(s).ok())
                            .unwrap_or_default(),
                        timestamp: DateTime::from_timestamp_micros(
                            doc.fields.get("timestamp").and_then(|v| v.as_timestamp()).unwrap_or(0)
                        ).unwrap_or_else(|| Utc::now()),
                        resolved: doc.fields.get("resolved").and_then(|v| v.as_bool()).unwrap_or(false),
                        resolved_at: doc.fields.get("resolved_at")
                            .and_then(|v| v.as_timestamp())
                            .and_then(|ts| DateTime::from_timestamp_micros(ts)),
                    });
                }
            }
        }

        // Sort by timestamp descending
        events.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(events)
    }

    // === STATISTICS AND MONITORING ===

    /// Get user statistics
    pub async fn get_user_stats(&self) -> Result<UserStats> {
        let documents = self.users_collection.find_many(None, 10000).await?;
        let mut stats = UserStats::default();

        for (_, doc) in documents {
            stats.total_users += 1;
            
            if let Some(Value::String(status)) = doc.fields.get("status") {
                match status.as_str() {
                    "Active" => stats.active_users += 1,
                    "Inactive" => stats.inactive_users += 1,
                    "Suspended" => stats.suspended_users += 1,
                    "Banned" => stats.banned_users += 1,
                    "PendingVerification" => stats.pending_verification += 1,
                    _ => {}
                }
            }

            if let Some(Value::Bool(verified)) = doc.fields.get("email_verified") {
                if *verified {
                    stats.verified_users += 1;
                }
            }
        }

        Ok(stats)
    }

    /// Get session statistics
    pub async fn get_session_stats(&self) -> Result<SessionStats> {
        let documents = self.sessions_collection.find_many(None, 10000).await?;
        let mut stats = SessionStats::default();

        for (_, doc) in documents {
            stats.total_sessions += 1;
            
            if let Some(Value::Bool(active)) = doc.fields.get("is_active") {
                if *active {
                    stats.active_sessions += 1;
                }
            }

            if let Some(Value::Timestamp(expires_at)) = doc.fields.get("expires_at") {
                if *expires_at > Utc::now().timestamp_micros() {
                    stats.valid_sessions += 1;
                }
            }
        }

        Ok(stats)
    }
}

/// User statistics
#[derive(Debug, Default)]
pub struct UserStats {
    pub total_users: usize,
    pub active_users: usize,
    pub inactive_users: usize,
    pub suspended_users: usize,
    pub banned_users: usize,
    pub pending_verification: usize,
    pub verified_users: usize,
}

/// Session statistics
#[derive(Debug, Default)]
pub struct SessionStats {
    pub total_sessions: usize,
    pub active_sessions: usize,
    pub valid_sessions: usize,
}

/// Legacy database repository for backward compatibility
pub struct DatabaseRepository;

impl DatabaseRepository {
    pub fn new() -> Self {
        Self
    }

    pub async fn health_check(&self) -> Result<bool> {
        Ok(true)
    }
}