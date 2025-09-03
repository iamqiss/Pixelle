/// Application constants

/// Default pagination values
pub const DEFAULT_PAGE: u32 = 1;
pub const DEFAULT_PER_PAGE: u32 = 20;
pub const MAX_PER_PAGE: u32 = 50;

/// JWT token constants
pub const JWT_SECRET: &str = "your-secret-key-here";
pub const JWT_EXPIRATION_HOURS: i64 = 24;
pub const REFRESH_TOKEN_EXPIRATION_DAYS: i64 = 30;

/// Password requirements
pub const MIN_PASSWORD_LENGTH: usize = 8;
pub const MAX_PASSWORD_LENGTH: usize = 128;

/// Username requirements
pub const MIN_USERNAME_LENGTH: usize = 3;
pub const MAX_USERNAME_LENGTH: usize = 20;

/// Post content limits
pub const MAX_POST_CONTENT_LENGTH: usize = 10000;
pub const MAX_COMMENT_CONTENT_LENGTH: usize = 1000;

/// Rate limiting
pub const RATE_LIMIT_REQUESTS_PER_MINUTE: u32 = 100;
pub const RATE_LIMIT_REQUESTS_PER_HOUR: u32 = 1000;

/// File upload limits
pub const MAX_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024; // 10MB
pub const ALLOWED_IMAGE_TYPES: &[&str] = &["image/jpeg", "image/png", "image/gif", "image/webp"];
pub const ALLOWED_VIDEO_TYPES: &[&str] = &["video/mp4", "video/webm", "video/ogg"];

/// Cache TTL values (in seconds)
pub const USER_CACHE_TTL: u64 = 3600; // 1 hour
pub const POST_CACHE_TTL: u64 = 1800; // 30 minutes
pub const FEED_CACHE_TTL: u64 = 300;  // 5 minutes

/// Database connection pool settings
pub const DB_POOL_MIN_SIZE: u32 = 5;
pub const DB_POOL_MAX_SIZE: u32 = 20;

/// Redis connection settings
pub const REDIS_POOL_SIZE: u32 = 10;
pub const REDIS_TIMEOUT_SECONDS: u64 = 5;
