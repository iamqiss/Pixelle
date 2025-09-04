// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Full-text search index implementation

use crate::{Result, DocumentId, Document, LargetableError, IndexType, IndexQuery, IndexStats};
use crate::index::Index;
use crate::document::DocumentUtils;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// Full-text search index using Tantivy
pub struct FullTextIndex {
    field: String,
    language: String,
    stop_words: Vec<String>,
    // In a real implementation, this would use Tantivy
    // For now, we'll use a simple inverted index
    inverted_index: Arc<RwLock<HashMap<String, Vec<DocumentId>>>>,
    document_terms: Arc<RwLock<HashMap<DocumentId, Vec<String>>>>,
}

impl FullTextIndex {
    /// Create a new full-text index
    pub fn new(field: String, language: String, stop_words: Vec<String>) -> Self {
        Self {
            field,
            language,
            stop_words,
            inverted_index: Arc::new(RwLock::new(HashMap::new())),
            document_terms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Extract text from a document field
    fn extract_text(&self, doc: &Document) -> Option<String> {
        DocumentUtils::get_field(doc, &self.field).and_then(|value| match value {
            crate::Value::String(s) => Some(s.clone()),
            _ => None,
        })
    }

    /// Tokenize text into terms
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.split_whitespace()
            .map(|word| word.to_lowercase())
            .filter(|word| !self.stop_words.contains(word))
            .filter(|word| word.len() > 2) // Filter out very short words
            .collect()
    }

    /// Update the inverted index
    async fn update_inverted_index(&self, doc_id: DocumentId, terms: Vec<String>) -> Result<()> {
        let mut inverted_index = self.inverted_index.write().await;
        let mut document_terms = self.document_terms.write().await;
        
        // Remove old terms for this document
        if let Some(old_terms) = document_terms.remove(&doc_id) {
            for term in old_terms {
                if let Some(doc_ids) = inverted_index.get_mut(&term) {
                    doc_ids.retain(|&id| id != doc_id);
                    if doc_ids.is_empty() {
                        inverted_index.remove(&term);
                    }
                }
            }
        }
        
        // Add new terms
        for term in &terms {
            inverted_index.entry(term.clone()).or_default().push(doc_id);
        }
        
        document_terms.insert(doc_id, terms);
        Ok(())
    }

    /// Search for terms in the inverted index
    async fn search_terms(&self, query: &str) -> Result<Vec<DocumentId>> {
        let inverted_index = self.inverted_index.read().await;
        let query_terms = self.tokenize(query);
        
        if query_terms.is_empty() {
            return Ok(Vec::new());
        }
        
        // Find documents that contain all query terms (AND search)
        let mut results = Vec::new();
        
        if let Some(first_term_docs) = inverted_index.get(&query_terms[0]) {
            let mut candidate_docs = first_term_docs.clone();
            
            for term in &query_terms[1..] {
                if let Some(term_docs) = inverted_index.get(term) {
                    candidate_docs.retain(|doc_id| term_docs.contains(doc_id));
                } else {
                    // If any term is not found, no results
                    return Ok(Vec::new());
                }
            }
            
            results = candidate_docs;
        }
        
        Ok(results)
    }
}

#[async_trait::async_trait]
impl Index for FullTextIndex {
    async fn insert(&self, id: DocumentId, doc: &Document) -> Result<()> {
        if let Some(text) = self.extract_text(doc) {
            let terms = self.tokenize(&text);
            self.update_inverted_index(id, terms).await?;
            debug!("Inserted document {} into full-text index on field '{}'", id, self.field);
        }
        Ok(())
    }

    async fn remove(&self, id: &DocumentId) -> Result<()> {
        let mut inverted_index = self.inverted_index.write().await;
        let mut document_terms = self.document_terms.write().await;
        
        if let Some(terms) = document_terms.remove(id) {
            for term in terms {
                if let Some(doc_ids) = inverted_index.get_mut(&term) {
                    doc_ids.retain(|&doc_id| doc_id != *id);
                    if doc_ids.is_empty() {
                        inverted_index.remove(&term);
                    }
                }
            }
        }
        
        debug!("Removed document {} from full-text index on field '{}'", id, self.field);
        Ok(())
    }

    async fn update(&self, id: DocumentId, old_doc: &Document, new_doc: &Document) -> Result<()> {
        // Remove old terms
        self.remove(&id).await?;
        
        // Insert new terms
        self.insert(id, new_doc).await?;
        
        debug!("Updated document {} in full-text index on field '{}'", id, self.field);
        Ok(())
    }

    async fn search(&self, query: &IndexQuery) -> Result<Vec<DocumentId>> {
        match query {
            IndexQuery::FullText { field, query: search_query } if field == &self.field => {
                self.search_terms(search_query).await
            }
            _ => {
                Err(LargetableError::Index(format!(
                    "Full-text index on field '{}' only supports full-text search, got: {:?}",
                    self.field, query
                )))
            }
        }
    }

    async fn stats(&self) -> Result<IndexStats> {
        let inverted_index = self.inverted_index.read().await;
        let document_terms = self.document_terms.read().await;
        
        let total_entries = document_terms.len();
        let memory_usage = std::mem::size_of_val(&*inverted_index) + 
            std::mem::size_of_val(&*document_terms) +
            inverted_index.iter().map(|(k, v)| std::mem::size_of_val(k) + std::mem::size_of_val(v)).sum::<usize>() +
            document_terms.iter().map(|(k, v)| std::mem::size_of_val(k) + std::mem::size_of_val(v)).sum::<usize>();
        
        Ok(IndexStats {
            total_entries,
            memory_usage,
            index_type: IndexType::FullText {
                language: self.language.clone(),
                stop_words: self.stop_words.clone(),
            },
        })
    }

    fn index_type(&self) -> IndexType {
        IndexType::FullText {
            language: self.language.clone(),
            stop_words: self.stop_words.clone(),
        }
    }
}