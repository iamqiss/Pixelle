use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use largetable::{Document, Value, DocumentId};
use std::collections::HashMap;

/// Enhanced user model with comprehensive security features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub username: String,
    pub email: String,
    pub email_verified: bool,
    pub password_hash: String,
    pub salt: String,
    pub profile: UserProfile,
    pub security: UserSecurity,
    pub preferences: UserPreferences,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub status: UserStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfile {
    pub display_name: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar_url: Option<String>,
    pub bio: Option<String>,
    pub location: Option<String>,
    pub website: Option<String>,
    pub birth_date: Option<DateTime<Utc>>,
    pub timezone: Option<String>,
    pub language: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSecurity {
    pub two_factor_enabled: bool,
    pub two_factor_secret: Option<String>,
    pub backup_codes: Vec<String>,
    pub login_attempts: u32,
    pub locked_until: Option<DateTime<Utc>>,
    pub password_changed_at: DateTime<Utc>,
    pub security_questions: Vec<SecurityQuestion>,
    pub trusted_devices: Vec<TrustedDevice>,
    pub api_keys: Vec<ApiKey>,
    pub oauth_providers: Vec<OAuthProvider>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityQuestion {
    pub question: String,
    pub answer_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustedDevice {
    pub device_id: String,
    pub device_name: String,
    pub device_type: DeviceType,
    pub fingerprint: String,
    pub ip_address: String,
    pub user_agent: String,
    pub location: Option<String>,
    pub trusted_at: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceType {
    Desktop,
    Mobile,
    Tablet,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: Uuid,
    pub name: String,
    pub key_hash: String,
    pub permissions: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub last_used: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthProvider {
    pub provider: String,
    pub provider_id: String,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub connected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPreferences {
    pub theme: String,
    pub notifications: NotificationPreferences,
    pub privacy: PrivacyPreferences,
    pub content: ContentPreferences,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPreferences {
    pub email_notifications: bool,
    pub push_notifications: bool,
    pub sms_notifications: bool,
    pub marketing_emails: bool,
    pub security_alerts: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyPreferences {
    pub profile_visibility: ProfileVisibility,
    pub show_online_status: bool,
    pub allow_direct_messages: bool,
    pub data_sharing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProfileVisibility {
    Public,
    Friends,
    Private,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentPreferences {
    pub language: String,
    pub content_filter: ContentFilter,
    pub autoplay_videos: bool,
    pub show_nsfw_content: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContentFilter {
    None,
    Moderate,
    Strict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserStatus {
    Active,
    Inactive,
    Suspended,
    Banned,
    PendingVerification,
}

/// Session model with advanced security features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: Uuid,
    pub user_id: Uuid,
    pub device_id: String,
    pub device_name: String,
    pub device_type: DeviceType,
    pub fingerprint: String,
    pub ip_address: String,
    pub user_agent: String,
    pub location: Option<String>,
    pub access_token: String,
    pub refresh_token: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub is_active: bool,
    pub security_flags: Vec<SecurityFlag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityFlag {
    SuspiciousLocation,
    NewDevice,
    UnusualActivity,
    MultipleLogins,
    LongInactive,
}

/// Authentication attempt tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthAttempt {
    pub id: Uuid,
    pub user_id: Option<Uuid>,
    pub username: String,
    pub ip_address: String,
    pub user_agent: String,
    pub success: bool,
    pub failure_reason: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub location: Option<String>,
    pub device_fingerprint: Option<String>,
}

/// Rate limiting and security events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityEvent {
    pub id: Uuid,
    pub user_id: Option<Uuid>,
    pub event_type: SecurityEventType,
    pub severity: SecuritySeverity,
    pub description: String,
    pub ip_address: String,
    pub user_agent: String,
    pub metadata: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
    pub resolved: bool,
    pub resolved_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityEventType {
    LoginAttempt,
    PasswordChange,
    TwoFactorSetup,
    DeviceTrusted,
    SuspiciousActivity,
    AccountLocked,
    AccountUnlocked,
    ApiKeyCreated,
    ApiKeyRevoked,
    OAuthConnected,
    OAuthDisconnected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecuritySeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Role-based access control
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub permissions: Vec<Permission>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    pub resource: String,
    pub actions: Vec<String>,
    pub conditions: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRole {
    pub user_id: Uuid,
    pub role_id: Uuid,
    pub assigned_at: DateTime<Utc>,
    pub assigned_by: Uuid,
    pub expires_at: Option<DateTime<Utc>>,
}

// Conversion methods for largetable integration
impl User {
    pub fn to_document(&self) -> Document {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::ObjectId(self.id));
        fields.insert("username".to_string(), Value::String(self.username.clone()));
        fields.insert("email".to_string(), Value::String(self.email.clone()));
        fields.insert("email_verified".to_string(), Value::Bool(self.email_verified));
        fields.insert("password_hash".to_string(), Value::String(self.password_hash.clone()));
        fields.insert("salt".to_string(), Value::String(self.salt.clone()));
        fields.insert("created_at".to_string(), Value::Timestamp(self.created_at.timestamp_micros()));
        fields.insert("updated_at".to_string(), Value::Timestamp(self.updated_at.timestamp_micros()));
        fields.insert("last_login".to_string(), 
            self.last_login.map(|t| Value::Timestamp(t.timestamp_micros())).unwrap_or(Value::Null));
        fields.insert("status".to_string(), Value::String(format!("{:?}", self.status)));
        
        // Serialize complex objects as JSON strings
        fields.insert("profile".to_string(), 
            Value::String(serde_json::to_string(&self.profile).unwrap_or_default()));
        fields.insert("security".to_string(), 
            Value::String(serde_json::to_string(&self.security).unwrap_or_default()));
        fields.insert("preferences".to_string(), 
            Value::String(serde_json::to_string(&self.preferences).unwrap_or_default()));

        Document {
            id: self.id,
            fields,
            version: 1,
            created_at: self.created_at.timestamp_micros(),
            updated_at: self.updated_at.timestamp_micros(),
        }
    }

    pub fn from_document(doc: Document) -> Result<Self, serde_json::Error> {
        let id = if let Some(Value::ObjectId(id)) = doc.fields.get("id") {
            *id
        } else {
            doc.id
        };

        let username = doc.fields.get("username")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let email = doc.fields.get("email")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let email_verified = doc.fields.get("email_verified")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let password_hash = doc.fields.get("password_hash")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let salt = doc.fields.get("salt")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let created_at = DateTime::from_timestamp_micros(
            doc.fields.get("created_at")
                .and_then(|v| v.as_timestamp())
                .unwrap_or(0)
        ).unwrap_or_else(|| Utc::now());

        let updated_at = DateTime::from_timestamp_micros(
            doc.fields.get("updated_at")
                .and_then(|v| v.as_timestamp())
                .unwrap_or(0)
        ).unwrap_or_else(|| Utc::now());

        let last_login = doc.fields.get("last_login")
            .and_then(|v| v.as_timestamp())
            .and_then(|ts| DateTime::from_timestamp_micros(ts));

        let status = doc.fields.get("status")
            .and_then(|v| v.as_string())
            .and_then(|s| serde_json::from_str(&format!("\"{}\"", s)).ok())
            .unwrap_or(UserStatus::Active);

        let profile = doc.fields.get("profile")
            .and_then(|v| v.as_string())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let security = doc.fields.get("security")
            .and_then(|v| v.as_string())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let preferences = doc.fields.get("preferences")
            .and_then(|v| v.as_string())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        Ok(User {
            id,
            username,
            email,
            email_verified,
            password_hash,
            salt,
            profile,
            security,
            preferences,
            created_at,
            updated_at,
            last_login,
            status,
        })
    }
}

impl Session {
    pub fn to_document(&self) -> Document {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::ObjectId(self.id));
        fields.insert("user_id".to_string(), Value::ObjectId(self.user_id));
        fields.insert("device_id".to_string(), Value::String(self.device_id.clone()));
        fields.insert("device_name".to_string(), Value::String(self.device_name.clone()));
        fields.insert("fingerprint".to_string(), Value::String(self.fingerprint.clone()));
        fields.insert("ip_address".to_string(), Value::String(self.ip_address.clone()));
        fields.insert("user_agent".to_string(), Value::String(self.user_agent.clone()));
        fields.insert("access_token".to_string(), Value::String(self.access_token.clone()));
        fields.insert("refresh_token".to_string(), Value::String(self.refresh_token.clone()));
        fields.insert("created_at".to_string(), Value::Timestamp(self.created_at.timestamp_micros()));
        fields.insert("expires_at".to_string(), Value::Timestamp(self.expires_at.timestamp_micros()));
        fields.insert("last_activity".to_string(), Value::Timestamp(self.last_activity.timestamp_micros()));
        fields.insert("is_active".to_string(), Value::Bool(self.is_active));
        
        fields.insert("device_type".to_string(), Value::String(format!("{:?}", self.device_type)));
        fields.insert("location".to_string(), 
            self.location.as_ref().map(|l| Value::String(l.clone())).unwrap_or(Value::Null));
        fields.insert("security_flags".to_string(), 
            Value::String(serde_json::to_string(&self.security_flags).unwrap_or_default()));

        Document {
            id: self.id,
            fields,
            version: 1,
            created_at: self.created_at.timestamp_micros(),
            updated_at: self.updated_at.timestamp_micros(),
        }
    }

    pub fn from_document(doc: Document) -> Result<Self, serde_json::Error> {
        let id = if let Some(Value::ObjectId(id)) = doc.fields.get("id") {
            *id
        } else {
            doc.id
        };

        let user_id = doc.fields.get("user_id")
            .and_then(|v| v.as_object_id())
            .unwrap_or_default();

        let device_id = doc.fields.get("device_id")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let device_name = doc.fields.get("device_name")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let fingerprint = doc.fields.get("fingerprint")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let ip_address = doc.fields.get("ip_address")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let user_agent = doc.fields.get("user_agent")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let access_token = doc.fields.get("access_token")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let refresh_token = doc.fields.get("refresh_token")
            .and_then(|v| v.as_string())
            .unwrap_or_default();

        let created_at = DateTime::from_timestamp_micros(
            doc.fields.get("created_at")
                .and_then(|v| v.as_timestamp())
                .unwrap_or(0)
        ).unwrap_or_else(|| Utc::now());

        let expires_at = DateTime::from_timestamp_micros(
            doc.fields.get("expires_at")
                .and_then(|v| v.as_timestamp())
                .unwrap_or(0)
        ).unwrap_or_else(|| Utc::now());

        let last_activity = DateTime::from_timestamp_micros(
            doc.fields.get("last_activity")
                .and_then(|v| v.as_timestamp())
                .unwrap_or(0)
        ).unwrap_or_else(|| Utc::now());

        let is_active = doc.fields.get("is_active")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let device_type = doc.fields.get("device_type")
            .and_then(|v| v.as_string())
            .and_then(|s| serde_json::from_str(&format!("\"{}\"", s)).ok())
            .unwrap_or(DeviceType::Unknown);

        let location = doc.fields.get("location")
            .and_then(|v| v.as_string());

        let security_flags = doc.fields.get("security_flags")
            .and_then(|v| v.as_string())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        Ok(Session {
            id,
            user_id,
            device_id,
            device_name,
            device_type,
            fingerprint,
            ip_address,
            user_agent,
            location,
            access_token,
            refresh_token,
            created_at,
            expires_at,
            last_activity,
            is_active,
            security_flags,
        })
    }
}

// Helper trait for Value type
trait ValueExt {
    fn as_string(&self) -> Option<String>;
    fn as_bool(&self) -> Option<bool>;
    fn as_timestamp(&self) -> Option<i64>;
    fn as_object_id(&self) -> Option<Uuid>;
}

impl ValueExt for Value {
    fn as_string(&self) -> Option<String> {
        match self {
            Value::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    fn as_timestamp(&self) -> Option<i64> {
        match self {
            Value::Timestamp(t) => Some(*t),
            _ => None,
        }
    }

    fn as_object_id(&self) -> Option<Uuid> {
        match self {
            Value::ObjectId(id) => Some(*id),
            _ => None,
        }
    }
}

// Default implementations
impl Default for UserProfile {
    fn default() -> Self {
        Self {
            display_name: None,
            first_name: None,
            last_name: None,
            avatar_url: None,
            bio: None,
            location: None,
            website: None,
            birth_date: None,
            timezone: None,
            language: None,
        }
    }
}

impl Default for UserSecurity {
    fn default() -> Self {
        Self {
            two_factor_enabled: false,
            two_factor_secret: None,
            backup_codes: Vec::new(),
            login_attempts: 0,
            locked_until: None,
            password_changed_at: Utc::now(),
            security_questions: Vec::new(),
            trusted_devices: Vec::new(),
            api_keys: Vec::new(),
            oauth_providers: Vec::new(),
        }
    }
}

impl Default for UserPreferences {
    fn default() -> Self {
        Self {
            theme: "light".to_string(),
            notifications: NotificationPreferences::default(),
            privacy: PrivacyPreferences::default(),
            content: ContentPreferences::default(),
        }
    }
}

impl Default for NotificationPreferences {
    fn default() -> Self {
        Self {
            email_notifications: true,
            push_notifications: true,
            sms_notifications: false,
            marketing_emails: false,
            security_alerts: true,
        }
    }
}

impl Default for PrivacyPreferences {
    fn default() -> Self {
        Self {
            profile_visibility: ProfileVisibility::Public,
            show_online_status: true,
            allow_direct_messages: true,
            data_sharing: false,
        }
    }
}

impl Default for ContentPreferences {
    fn default() -> Self {
        Self {
            language: "en".to_string(),
            content_filter: ContentFilter::Moderate,
            autoplay_videos: false,
            show_nsfw_content: false,
        }
    }
}
