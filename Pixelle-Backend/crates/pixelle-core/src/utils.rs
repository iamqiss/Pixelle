use chrono::{DateTime, Utc};
use uuid::Uuid;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

/// Generate a new UUID
pub fn generate_id() -> Uuid {
    Uuid::new_v4()
}

/// Get current timestamp
pub fn now() -> DateTime<Utc> {
    Utc::now()
}

/// Encode string to base64
pub fn encode_base64(input: &str) -> String {
    BASE64.encode(input.as_bytes())
}

/// Decode base64 string
pub fn decode_base64(input: &str) -> Result<String, base64::DecodeError> {
    let bytes = BASE64.decode(input)?;
    Ok(String::from_utf8(bytes).unwrap_or_default())
}

/// Validate email format
pub fn is_valid_email(email: &str) -> bool {
    use regex::Regex;
    let email_regex = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap();
    email_regex.is_match(email)
}

/// Validate username format
pub fn is_valid_username(username: &str) -> bool {
    use regex::Regex;
    let username_regex = Regex::new(r"^[a-zA-Z0-9_]{3,20}$").unwrap();
    username_regex.is_match(username)
}

/// Generate a random string
pub fn generate_random_string(length: usize) -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Calculate pagination info
pub fn calculate_pagination_info(total: u64, page: u32, per_page: u32) -> (u32, u32, u32) {
    let total_pages = ((total as f64) / (per_page as f64)).ceil() as u32;
    let offset = (page - 1) * per_page;
    (total_pages, offset, per_page)
}
