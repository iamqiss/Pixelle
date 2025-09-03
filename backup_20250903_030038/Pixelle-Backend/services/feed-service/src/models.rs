use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FeedItem {
    pub post_id: String,
    pub author_id: String,
    pub content: String,
    pub media_urls: Vec<String>,
    pub engagement_score: f64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrendingPost {
    pub post_id: String,
    pub title: String,
    pub engagement_score: f64,
    pub trending_rank: u32,
}
