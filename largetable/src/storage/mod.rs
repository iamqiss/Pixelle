// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Pluggable storage engine architecture

pub mod engines;
pub mod wal;
pub mod cache;
pub mod compression;
pub mod checksum;
pub mod hotswap;

use crate::{Result, DocumentId, Document};
use async_trait::async_trait;

#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn get(&self, id: &DocumentId) -> Result<Option<Document>>;
    async fn put(&self, id: DocumentId, doc: Document) -> Result<()>;
    async fn delete(&self, id: &DocumentId) -> Result<bool>;
    async fn scan(&self, start: Option<DocumentId>, limit: usize) -> Result<Vec<(DocumentId, Document)>>;
}
