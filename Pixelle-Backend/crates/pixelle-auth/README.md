# Pixelle Authentication System

A PhD-level, enterprise-grade authentication system built with Rust, featuring advanced security, largetable database integration, and comprehensive user management capabilities.

## 🚀 Features

### Core Authentication
- **JWT-based authentication** with access and refresh tokens
- **Role-based access control (RBAC)** with fine-grained permissions
- **Session management** with device tracking and security monitoring
- **Password security** with advanced policies and breach detection
- **Rate limiting** and brute force protection
- **Device fingerprinting** and location tracking

### Advanced Security
- **Two-factor authentication (2FA)** support
- **OAuth integration** for social login
- **API key management** with scoped permissions
- **Security event logging** and monitoring
- **Password breach detection** using HaveIBeenPwned-style checks
- **Progressive security policies** with adaptive requirements

### Database Integration
- **Largetable NoSQL database** for high-performance storage
- **Zero-copy serialization** for optimal performance
- **Automatic indexing** for fast queries
- **Data versioning** and migration support
- **Distributed storage** with replication

## 🏗️ Architecture

### Components

```
pixelle-auth/
├── src/
│   ├── lib.rs              # Main library exports
│   ├── auth_service.rs     # Core authentication service
│   ├── jwt.rs             # JWT token management
│   ├── passphrase.rs      # Password security and validation
│   └── session.rs         # Session management
├── Cargo.toml             # Dependencies and configuration
└── README.md              # This file
```

### Database Models

#### User Model
```rust
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
```

#### Session Model
```rust
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
```

## 🔐 Security Features

### Password Security
- **Argon2id hashing** with configurable parameters
- **Password strength validation** using zxcvbn algorithm
- **Breach detection** with caching
- **Rate limiting** for verification attempts
- **Policy enforcement** with customizable rules

### JWT Security
- **Token blacklisting** for immediate revocation
- **Version-based invalidation** for mass token revocation
- **Device fingerprinting** validation
- **IP address monitoring** with anomaly detection
- **Configurable expiration** times for different token types

### Session Security
- **Device tracking** with fingerprinting
- **Location monitoring** for suspicious activity
- **Automatic cleanup** of expired sessions
- **Security flagging** for unusual behavior
- **Multi-device management** with device trust

## 📊 Performance Features

### Largetable Integration
- **Async-first architecture** for high concurrency
- **Zero-copy serialization** for optimal memory usage
- **Automatic indexing** on critical fields
- **Connection pooling** for database efficiency
- **Query optimization** with intelligent caching

### Caching Strategy
- **In-memory caching** for frequently accessed data
- **Redis integration** for distributed caching
- **Cache invalidation** on data updates
- **TTL-based expiration** for automatic cleanup

## 🛠️ Usage Examples

### Basic Authentication
```rust
use pixelle_auth::{AuthService, JwtService, PassphraseService};
use pixelle_database::UserRepository;

// Initialize services
let jwt_service = JwtService::with_secret("your-secret-key".to_string());
let password_service = PassphraseService::new();
let user_repo = UserRepository::new().await?;

// Create user
let user = User {
    id: Uuid::new_v4(),
    username: "john_doe".to_string(),
    email: "john@example.com".to_string(),
    // ... other fields
};

let user_id = user_repo.create_user(&user).await?;

// Authenticate user
let token_pair = jwt_service.create_token_pair(
    user.id,
    session_id,
    device_id,
    device_fingerprint,
    ip_address,
    user_agent,
    roles,
    permissions,
    scopes,
).await?;
```

### Password Validation
```rust
// Validate password strength
let validation = password_service.validate_password_strength(
    "MySecurePassword123!",
    Some("john_doe"),
    Some("john@example.com"),
    Some(&user_info),
)?;

if validation.is_valid {
    println!("Password strength: {:?}", validation.strength);
    println!("Estimated crack time: {}", validation.crack_time);
} else {
    println!("Password issues: {:?}", validation.feedback);
}
```

### Session Management
```rust
// Create session
let session = Session {
    id: Uuid::new_v4(),
    user_id: user.id,
    device_id: "device_123".to_string(),
    device_name: "Chrome on Windows".to_string(),
    device_type: DeviceType::Desktop,
    fingerprint: "fp_123456".to_string(),
    ip_address: "192.168.1.100".to_string(),
    user_agent: "Mozilla/5.0...".to_string(),
    // ... other fields
};

let session_id = user_repo.create_session(&session).await?;

// Validate session
let validation_result = jwt_service.validate_token(
    &access_token,
    Some(current_ip),
    Some(current_user_agent),
    Some(current_fingerprint),
).await?;

if validation_result.valid {
    println!("User ID: {:?}", validation_result.user_id);
    println!("Roles: {:?}", validation_result.roles);
    println!("Permissions: {:?}", validation_result.permissions);
}
```

## 🔧 Configuration

### JWT Configuration
```rust
let jwt_config = JwtConfig {
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
};

let jwt_service = JwtService::new(jwt_config);
```

### Password Policy
```rust
let password_policy = PasswordPolicy {
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
    ],
    max_repeating_chars: 3,
    max_sequential_chars: 3,
    prevent_common_passwords: true,
    prevent_user_info: true,
};

let password_service = PassphraseService::with_config(
    RateLimitConfig::default(),
    password_policy,
    Argon2Config::default(),
);
```

## 📈 Monitoring and Analytics

### Security Events
```rust
// Log security event
let security_event = SecurityEvent {
    id: Uuid::new_v4(),
    user_id: Some(user.id),
    event_type: SecurityEventType::LoginAttempt,
    severity: SecuritySeverity::Medium,
    description: "Failed login attempt".to_string(),
    ip_address: "192.168.1.100".to_string(),
    user_agent: "Mozilla/5.0...".to_string(),
    metadata: HashMap::new(),
    timestamp: Utc::now(),
    resolved: false,
    resolved_at: None,
};

user_repo.log_security_event(&security_event).await?;
```

### Statistics
```rust
// Get user statistics
let user_stats = user_repo.get_user_stats().await?;
println!("Total users: {}", user_stats.total_users);
println!("Active users: {}", user_stats.active_users);
println!("Verified users: {}", user_stats.verified_users);

// Get session statistics
let session_stats = user_repo.get_session_stats().await?;
println!("Active sessions: {}", session_stats.active_sessions);
println!("Valid sessions: {}", session_stats.valid_sessions);
```

## 🚀 Performance Optimizations

### Database Indexing
- **Username and email** - Hash indexes for fast lookups
- **Session tokens** - Hash indexes for O(1) validation
- **Timestamps** - B-tree indexes for range queries
- **User status** - Hash indexes for filtering

### Memory Management
- **Connection pooling** for database efficiency
- **In-memory caching** for frequently accessed data
- **Automatic cleanup** of expired data
- **Memory-mapped storage** for large datasets

### Async Operations
- **Non-blocking I/O** for all database operations
- **Concurrent request handling** with tokio
- **Background cleanup tasks** for maintenance
- **Streaming responses** for large datasets

## 🔒 Security Best Practices

### Password Security
1. **Use Argon2id** for password hashing
2. **Implement rate limiting** for login attempts
3. **Check against breach databases** regularly
4. **Enforce strong password policies**
5. **Monitor for suspicious patterns**

### Token Security
1. **Use short-lived access tokens** (1 hour)
2. **Implement token blacklisting** for revocation
3. **Validate device fingerprints** for security
4. **Monitor for token abuse** patterns
5. **Use secure random generation** for secrets

### Session Security
1. **Track device information** for each session
2. **Monitor for unusual activity** patterns
3. **Implement automatic cleanup** of expired sessions
4. **Use secure session storage** with encryption
5. **Log all security events** for analysis

## 📚 Dependencies

### Core Dependencies
- `jsonwebtoken` - JWT token handling
- `argon2` - Password hashing
- `uuid` - Unique identifier generation
- `chrono` - Date and time handling
- `serde` - Serialization/deserialization

### Security Dependencies
- `ring` - Cryptographic operations
- `base64` - Base64 encoding/decoding
- `zxcvbn` - Password strength analysis
- `regex` - Pattern matching

### Database Dependencies
- `largetable` - Next-generation NoSQL database
- `tokio` - Async runtime
- `tracing` - Structured logging

## 🧪 Testing

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_password_validation() {
        let service = PassphraseService::new();
        let result = service.validate_password_strength(
            "TestPassword123!",
            None,
            None,
            None,
        ).unwrap();
        
        assert!(result.is_valid);
        assert_eq!(result.strength, PasswordStrength::Good);
    }

    #[tokio::test]
    async fn test_jwt_token_creation() {
        let service = JwtService::with_secret("test-secret".to_string());
        let user_id = Uuid::new_v4();
        
        let token_pair = service.create_token_pair(
            user_id,
            Uuid::new_v4(),
            "device_123".to_string(),
            "fingerprint_123".to_string(),
            "127.0.0.1".to_string(),
            "test-agent".to_string(),
            vec!["user".to_string()],
            vec!["read".to_string()],
            vec!["api".to_string()],
        ).await.unwrap();
        
        assert!(!token_pair.access_token.is_empty());
        assert!(!token_pair.refresh_token.is_empty());
    }
}
```

### Integration Tests
```rust
#[tokio::test]
async fn test_full_authentication_flow() {
    let user_repo = UserRepository::new().await.unwrap();
    let jwt_service = JwtService::with_secret("test-secret".to_string());
    let password_service = PassphraseService::new();
    
    // Create user
    let user = create_test_user();
    let user_id = user_repo.create_user(&user).await.unwrap();
    
    // Create session
    let session = create_test_session(user.id);
    let session_id = user_repo.create_session(&session).await.unwrap();
    
    // Validate token
    let validation = jwt_service.validate_token(
        &session.access_token,
        None,
        None,
        None,
    ).await.unwrap();
    
    assert!(validation.valid);
    assert_eq!(validation.user_id, Some(user.id));
}
```

## 🚀 Deployment

### Docker Configuration
```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/target/release/pixelle-auth /usr/local/bin/
EXPOSE 8080
CMD ["pixelle-auth"]
```

### Environment Variables
```bash
# Database
LARGETABLE_URL=largetable://localhost:8080
LARGETABLE_DATABASE=pixelle_auth

# JWT
JWT_SECRET=your-super-secret-jwt-key
JWT_ISSUER=pixelle-auth
JWT_AUDIENCE=pixelle-api

# Security
ENABLE_RATE_LIMITING=true
ENABLE_BREACH_DETECTION=true
ENABLE_DEVICE_VALIDATION=true

# Monitoring
LOG_LEVEL=info
ENABLE_METRICS=true
```

## 📊 Metrics and Monitoring

### Key Metrics
- **Authentication success/failure rates**
- **Password strength distribution**
- **Session duration and activity**
- **Rate limiting triggers**
- **Security event frequency**
- **Database performance metrics**

### Health Checks
```rust
pub async fn health_check() -> Result<HealthStatus> {
    let mut status = HealthStatus::healthy();
    
    // Check database connectivity
    if !user_repo.health_check().await? {
        status.add_issue("Database connection failed");
    }
    
    // Check JWT service
    if jwt_service.config().secret.is_empty() {
        status.add_issue("JWT secret not configured");
    }
    
    // Check password service
    if password_service.get_password_policy().min_length < 8 {
        status.add_issue("Password policy too weak");
    }
    
    Ok(status)
}
```

## 🔮 Future Enhancements

### Planned Features
- **Biometric authentication** support
- **Hardware security keys** (FIDO2/WebAuthn)
- **Advanced threat detection** with ML
- **Multi-tenant support** with isolation
- **GraphQL API** for flexible queries
- **Real-time notifications** for security events

### Performance Improvements
- **Distributed caching** with Redis Cluster
- **Database sharding** for horizontal scaling
- **CDN integration** for global performance
- **Edge computing** support for low latency
- **GPU acceleration** for cryptographic operations

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For support and questions:
- Create an issue in the repository
- Join our Discord community
- Check the documentation wiki
- Contact the development team

---

**Built with ❤️ by the Pixelle team using Rust and advanced security practices.**