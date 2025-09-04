use thiserror::Error;

#[derive(Error, Debug)]
pub enum ModerationError {
	#[error("database error: {0}")]
	Database(#[from] sqlx::Error),
	#[error("redis error: {0}")]
	Redis(#[from] redis::RedisError),
	#[error("other error: {0}")]
	Other(#[from] anyhow::Error),
}