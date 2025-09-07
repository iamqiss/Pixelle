// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Metadata management

pub mod search_engine;

// Re-export commonly used types
pub use search_engine::{SearchEngine, SearchQuery, SearchResponse, SearchResult, IndexedDocument, SearchIndex, IndexStats};
