// This module can contain additional models specific to the user service
// For now, we're using the core types from pixelle_core

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct UserStats {
    pub total_posts: u32,
    pub total_followers: u32,
    pub total_following: u32,
    pub total_likes_received: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserPreferences {
    pub email_notifications: bool,
    pub push_notifications: bool,
    pub privacy_level: PrivacyLevel,
    pub language: String,
    pub timezone: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PrivacyLevel {
    Public,
    Private,
    FriendsOnly,
}
