use pixelle_core::{Post, PaginationParams, PaginatedResponse, PixelleResult};
use std::collections::HashMap;
use std::sync::Mutex;

pub struct FeedService {
    posts: Mutex<HashMap<String, Vec<Post>>>,
}

impl FeedService {
    pub fn new() -> Self {
        let mut posts = HashMap::new();
        
        // Add some sample posts for demonstration
        let sample_posts = vec![
            Post {
                id: pixelle_core::generate_id(),
                author_id: pixelle_core::generate_id(),
                content: "This is a sample post for the feed!".to_string(),
                media_urls: vec![],
                likes_count: 42,
                comments_count: 5,
                shares_count: 2,
                is_public: true,
                created_at: pixelle_core::now(),
                updated_at: pixelle_core::now(),
            },
            Post {
                id: pixelle_core::generate_id(),
                author_id: pixelle_core::generate_id(),
                content: "Another interesting post about technology and innovation.".to_string(),
                media_urls: vec![],
                likes_count: 128,
                comments_count: 15,
                shares_count: 8,
                is_public: true,
                created_at: pixelle_core::now(),
                updated_at: pixelle_core::now(),
            },
        ];
        
        posts.insert("user1".to_string(), sample_posts);
        
        Self {
            posts: Mutex::new(posts),
        }
    }

    pub async fn get_user_feed(&self, user_id: &str, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<Post>> {
        let posts = self.posts.lock().unwrap();
        
        let user_posts = posts.get(user_id).cloned().unwrap_or_default();
        let total = user_posts.len() as u64;
        
        let start = ((pagination.page - 1) * pagination.per_page) as usize;
        let end = (start + pagination.per_page as usize).min(user_posts.len());
        
        let items = if start < user_posts.len() {
            user_posts[start..end].to_vec()
        } else {
            Vec::new()
        };

        let total_pages = ((total as f64) / (pagination.per_page as f64)).ceil() as u32;

        Ok(PaginatedResponse {
            items,
            total,
            page: pagination.page,
            per_page: pagination.per_page,
            total_pages,
        })
    }

    pub async fn get_trending_posts(&self, pagination: &PaginationParams) -> PixelleResult<PaginatedResponse<Post>> {
        let posts = self.posts.lock().unwrap();
        
        // Flatten all posts and sort by likes
        let mut all_posts: Vec<Post> = posts.values().flatten().cloned().collect();
        all_posts.sort_by(|a, b| b.likes_count.cmp(&a.likes_count));
        
        let total = all_posts.len() as u64;
        
        let start = ((pagination.page - 1) * pagination.per_page) as usize;
        let end = (start + pagination.per_page as usize).min(all_posts.len());
        
        let items = if start < all_posts.len() {
            all_posts[start..end].to_vec()
        } else {
            Vec::new()
        };

        let total_pages = ((total as f64) / (pagination.per_page as f64)).ceil() as u32;

        Ok(PaginatedResponse {
            items,
            total,
            page: pagination.page,
            per_page: pagination.per_page,
            total_pages,
        })
    }
}
