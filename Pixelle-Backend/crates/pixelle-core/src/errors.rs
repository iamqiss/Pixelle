use thiserror::Error;

/// Core error types for the Pixelle backend
#[derive(Error, Debug)]
pub enum PixelleError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    
    #[error("Authentication error: {0}")]
    Authentication(String),
    
    #[error("Authorization error: {0}")]
    Authorization(String),
    
    #[error("Validation error: {0}")]
    Validation(String),
    
    #[error("Not found: {0}")]
    NotFound(String),
    
    #[error("Conflict: {0}")]
    Conflict(String),
    
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    
    #[error("Internal server error: {0}")]
    Internal(String),
    
    #[error("External service error: {0}")]
    ExternalService(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type alias for Pixelle operations
pub type PixelleResult<T> = Result<T, PixelleError>;

/// HTTP status code mapping for errors
impl PixelleError {
    pub fn status_code(&self) -> u16 {
        match self {
            PixelleError::Authentication(_) => 401,
            PixelleError::Authorization(_) => 403,
            PixelleError::Validation(_) => 400,
            PixelleError::NotFound(_) => 404,
            PixelleError::Conflict(_) => 409,
            PixelleError::RateLimitExceeded => 429,
            PixelleError::Database(_) => 500,
            PixelleError::Internal(_) => 500,
            PixelleError::ExternalService(_) => 502,
            PixelleError::Serialization(_) => 400,
            PixelleError::Io(_) => 500,
        }
    }
}
