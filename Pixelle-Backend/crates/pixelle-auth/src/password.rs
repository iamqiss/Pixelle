use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use pixelle_core::{PixelleResult, PixelleError};

pub struct PasswordService;

impl PasswordService {
    pub fn new() -> Self {
        Self
    }

    pub async fn hash_password(&self, password: &str) -> PixelleResult<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| PixelleError::Internal(format!("Password hashing error: {}", e)))?;

        Ok(password_hash.to_string())
    }

    pub async fn verify_password(&self, password: &str, hash: &str) -> PixelleResult<bool> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|e| PixelleError::Internal(format!("Invalid password hash: {}", e)))?;

        let result = Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok();

        Ok(result)
    }
}
