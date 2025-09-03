use pixelle_core::{PixelleResult, PixelleError};
use std::collections::HashMap;
use std::sync::Mutex;
use chrono::{DateTime, Utc};

pub struct SessionService {
    sessions: Mutex<HashMap<String, SessionData>>,
}

struct SessionData {
    user_id: String,
    expires_at: DateTime<Utc>,
}

impl SessionService {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn create_session(&self, user_id: &str, expires_at: DateTime<Utc>) -> PixelleResult<String> {
        let session_id = uuid::Uuid::new_v4().to_string();
        let session_data = SessionData {
            user_id: user_id.to_string(),
            expires_at,
        };

        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.insert(session_id.clone(), session_data);
        }

        Ok(session_id)
    }

    pub async fn get_session(&self, session_id: &str) -> PixelleResult<Option<String>> {
        if let Ok(sessions) = self.sessions.lock() {
            if let Some(session_data) = sessions.get(session_id) {
                if session_data.expires_at > Utc::now() {
                    return Ok(Some(session_data.user_id.clone()));
                }
            }
        }
        Ok(None)
    }

    pub async fn revoke_session(&self, session_id: &str) -> PixelleResult<()> {
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.remove(session_id);
        }
        Ok(())
    }

    pub async fn cleanup_expired_sessions(&self) -> PixelleResult<()> {
        let now = Utc::now();
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.retain(|_, session_data| session_data.expires_at > now);
        }
        Ok(())
    }
}
