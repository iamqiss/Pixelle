use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use pixelle_core::{PixelleResult, UserId, PixelleError};
use chrono::{Duration, Utc};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String, // User ID
    exp: i64,    // Expiration time
    iat: i64,    // Issued at
}

pub struct JwtService {
    secret: String,
}

impl JwtService {
    pub fn new(secret: String) -> Self {
        Self { secret }
    }

    pub async fn create_token(&self, user_id: UserId) -> PixelleResult<String> {
        let expiration = Utc::now()
            .checked_add_signed(Duration::hours(24))
            .expect("valid timestamp")
            .timestamp();

        let claims = Claims {
            sub: user_id.to_string(),
            exp: expiration,
            iat: Utc::now().timestamp(),
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_ref()),
        )
        .map_err(|e| PixelleError::Internal(format!("JWT encoding error: {}", e)))
    }

    pub async fn validate_token(&self, token: &str) -> PixelleResult<Option<UserId>> {
        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_ref()),
            &Validation::default(),
        );

        match token_data {
            Ok(token_data) => {
                let user_id = token_data.claims.sub.parse::<UserId>()
                    .map_err(|_| PixelleError::Authentication("Invalid user ID in token".to_string()))?;
                Ok(Some(user_id))
            }
            Err(_) => Ok(None),
        }
    }
}
