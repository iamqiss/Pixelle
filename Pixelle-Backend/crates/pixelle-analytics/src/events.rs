use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum EventType {
    UserRegistered,
    UserLoggedIn,
    PostCreated,
    PostLiked,
    CommentCreated,
    FollowUser,
    UnfollowUser,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub event_type: EventType,
    pub user_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub metadata: serde_json::Value,
}
