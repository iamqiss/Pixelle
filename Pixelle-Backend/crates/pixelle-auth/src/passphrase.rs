use argon2::{Argon2, PassphraseHash, PassphraseHasher, PassphraseVerifier};
use argon2::passphrase_hash::rand_core::OsRng;
use argon2::passphrase_hash::SaltString;
use pixelle_core::{PixelleResult, PixelleError};

pub struct PassphraseService;

impl PassphraseService {
    pub fn new() -> Self {
        Self
    }

    pub async fn hash_passphrase(&self, passphrase: &str) -> PixelleResult<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        
        let passphrase_hash = argon2
            .hash_passphrase(passphrase.as_bytes(), &salt)
            .map_err(|e| PixelleError::Internal(format!("Passphrase hashing error: {}", e)))?;

        Ok(passphrase_hash.to_string())
    }

    pub async fn verify_passphrase(&self, passphrase: &str, hash: &str) -> PixelleResult<bool> {
        let parsed_hash = PassphraseHash::new(hash)
            .map_err(|e| PixelleError::Internal(format!("Invalid passphrase hash: {}", e)))?;

        let result = Argon2::default()
            .verify_passphrase(passphrase.as_bytes(), &parsed_hash)
            .is_ok();

        Ok(result)
    }
}
