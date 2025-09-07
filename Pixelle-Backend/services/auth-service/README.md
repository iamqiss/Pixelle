# Pixelle Auth Service

A comprehensive authentication service for the Pixelle backend, providing secure user authentication, session management, and authorization features.

## Features

- **User Registration & Login**: Secure user registration and authentication
- **JWT Token Management**: Access and refresh token generation and validation
- **Password Security**: Argon2 password hashing with configurable parameters
- **Session Management**: Secure session creation, validation, and cleanup
- **Rate Limiting**: Configurable rate limiting for auth endpoints
- **Input Validation**: Comprehensive input validation using the `validator` crate
- **Database Integration**: Integration with the pixelle-database crate
- **Qubit Integration**: Integration with Qubit/Nimbux services for object storage
- **Middleware**: Authentication middleware for protecting routes
- **Configuration**: Environment-based configuration with sensible defaults
- **Testing**: Comprehensive test suite with unit and integration tests

## API Endpoints

### Authentication
- `POST /api/v1/auth/register` - Register a new user
- `POST /api/v1/auth/login` - Login user
- `POST /api/v1/auth/logout` - Logout user
- `POST /api/v1/auth/refresh` - Refresh access token

### Password Management
- `POST /api/v1/auth/change-password` - Change user password
- `POST /api/v1/auth/request-password-reset` - Request password reset
- `POST /api/v1/auth/confirm-password-reset` - Confirm password reset

### User Profile
- `GET /api/v1/auth/profile` - Get user profile
- `PUT /api/v1/auth/profile` - Update user profile
- `DELETE /api/v1/auth/account` - Delete user account

### Health Check
- `GET /health` - Service health check

## Configuration

The service can be configured using environment variables or by providing a configuration file. Here are the available configuration options:

### JWT Configuration
- `JWT_SECRET` - Secret key for JWT token signing (default: "change-me-in-production")
- `JWT_EXPIRATION_SECONDS` - JWT token expiration time in seconds (default: 3600)
- `REFRESH_TOKEN_EXPIRATION_SECONDS` - Refresh token expiration time in seconds (default: 604800)

### Password Configuration
- `PASSWORD_MIN_LENGTH` - Minimum password length (default: 8)
- `PASSWORD_MAX_LENGTH` - Maximum password length (default: 128)
- `PASSWORD_REQUIRE_UPPERCASE` - Require uppercase letters (default: true)
- `PASSWORD_REQUIRE_LOWERCASE` - Require lowercase letters (default: true)
- `PASSWORD_REQUIRE_NUMBERS` - Require numbers (default: true)
- `PASSWORD_REQUIRE_SPECIAL_CHARS` - Require special characters (default: true)
- `ARGON2_MEMORY_COST` - Argon2 memory cost in KB (default: 4096)
- `ARGON2_TIME_COST` - Argon2 time cost iterations (default: 3)
- `ARGON2_PARALLELISM` - Argon2 parallelism (default: 1)

### Session Configuration
- `SESSION_TIMEOUT_SECONDS` - Session timeout in seconds (default: 3600)
- `MAX_CONCURRENT_SESSIONS` - Maximum concurrent sessions per user (default: 5)
- `SESSION_CLEANUP_INTERVAL_SECONDS` - Session cleanup interval in seconds (default: 300)
- `SESSION_COOKIE_NAME` - Session cookie name (default: "pixelle_session")
- `SESSION_COOKIE_SECURE` - Session cookie secure flag (default: true)
- `SESSION_COOKIE_HTTP_ONLY` - Session cookie http only flag (default: true)
- `SESSION_COOKIE_SAME_SITE` - Session cookie same site policy (default: "Strict")

### Rate Limiting Configuration
- `RATE_LIMIT_ENABLED` - Enable rate limiting (default: true)
- `RATE_LIMIT_MAX_REQUESTS` - Maximum requests per window (default: 100)
- `RATE_LIMIT_WINDOW_SECONDS` - Rate limit window in seconds (default: 60)
- `LOGIN_MAX_ATTEMPTS` - Maximum login attempts (default: 5)
- `LOGIN_WINDOW_SECONDS` - Login attempt window in seconds (default: 900)
- `PASSWORD_RESET_MAX_ATTEMPTS` - Maximum password reset attempts (default: 3)
- `PASSWORD_RESET_WINDOW_SECONDS` - Password reset window in seconds (default: 3600)

### Security Configuration
- `ENABLE_ACCOUNT_LOCKOUT` - Enable account lockout after failed attempts (default: true)
- `MAX_FAILED_ATTEMPTS` - Maximum failed login attempts before lockout (default: 5)
- `LOCKOUT_DURATION_SECONDS` - Account lockout duration in seconds (default: 1800)
- `ENABLE_PASSWORD_HISTORY` - Enable password history (default: true)
- `PASSWORD_HISTORY_COUNT` - Number of previous passwords to remember (default: 5)
- `PASSWORD_EXPIRATION_DAYS` - Password expiration in days (default: 90)
- `ENABLE_2FA` - Enable two-factor authentication (default: false)
- `ENABLE_EMAIL_VERIFICATION` - Enable email verification (default: true)
- `ENABLE_PASSWORD_RESET` - Enable password reset (default: true)
- `PASSWORD_RESET_TOKEN_EXPIRATION_HOURS` - Password reset token expiration in hours (default: 24)

## Usage

### Starting the Service

```bash
# Set environment variables
export JWT_SECRET="your-super-secret-jwt-key-here"
export JWT_EXPIRATION_SECONDS="3600"
export REFRESH_TOKEN_EXPIRATION_SECONDS="604800"

# Run the service
cargo run --bin pixelle-auth-service
```

### Using the Service

#### Register a New User

```bash
curl -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "email": "test@example.com",
    "password": "SecurePassword123!",
    "display_name": "Test User",
    "bio": "Test user bio"
  }'
```

#### Login

```bash
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username_or_email": "testuser",
    "password": "SecurePassword123!",
    "remember_me": false
  }'
```

#### Access Protected Endpoints

```bash
curl -X GET http://localhost:8080/api/v1/auth/profile \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

## Architecture

The auth service is built with a modular architecture:

- **Enhanced Auth Service**: Main service implementation with comprehensive features
- **JWT Service**: JWT token creation and validation
- **Passphrase Service**: Password hashing and verification using Argon2
- **Session Service**: Session management and cleanup
- **Middleware**: Authentication and rate limiting middleware
- **Configuration**: Environment-based configuration management
- **Mock Repository**: Mock user repository for development and testing

## Security Features

- **Password Hashing**: Argon2id password hashing with configurable parameters
- **JWT Tokens**: Secure JWT token generation with configurable expiration
- **Session Management**: Secure session creation and validation
- **Rate Limiting**: Configurable rate limiting to prevent abuse
- **Input Validation**: Comprehensive input validation and sanitization
- **Account Lockout**: Automatic account lockout after failed login attempts
- **Password History**: Prevention of password reuse
- **Secure Cookies**: HTTP-only, secure, and SameSite cookie configuration

## Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_jwt_token_creation_and_validation
```

## Dependencies

- **actix-web**: Web framework for HTTP endpoints
- **jsonwebtoken**: JWT token handling
- **argon2**: Password hashing
- **validator**: Input validation
- **serde**: Serialization/deserialization
- **tokio**: Async runtime
- **uuid**: UUID generation
- **chrono**: Date and time handling

## Integration

The auth service integrates with:

- **Pixelle Core**: Core types and traits
- **Pixelle Database**: Database operations and user storage
- **Pixelle Monitoring**: Logging and monitoring
- **Qubit/Nimbux**: Object storage and advanced features

## Development

### Adding New Features

1. Add new endpoints to `main.rs`
2. Implement business logic in `enhanced_auth_service.rs`
3. Add configuration options to `config.rs`
4. Write tests in `tests.rs`
5. Update documentation

### Code Style

- Use `cargo fmt` to format code
- Use `cargo clippy` to check for linting issues
- Follow Rust naming conventions
- Add documentation for public APIs

## License

This project is part of the Pixelle backend system and follows the same licensing terms.