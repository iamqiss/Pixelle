// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Advanced metadata search and indexing system

use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::errors::{NimbuxError, Result};

/// Search query with advanced filtering capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub text: Option<String>,
    pub filters: Vec<SearchFilter>,
    pub sort: Option<SortSpec>,
    pub pagination: Option<PaginationSpec>,
    pub facets: Option<Vec<String>>,
    pub highlight: Option<HighlightSpec>,
    pub aggregations: Option<Vec<AggregationSpec>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchFilter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: FilterValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Contains,
    StartsWith,
    EndsWith,
    Regex,
    In,
    NotIn,
    Exists,
    NotExists,
    Range,
    GeoWithin,
    GeoNear,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterValue {
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Date(DateTime<Utc>),
    Array(Vec<FilterValue>),
    GeoPoint { lat: f64, lon: f64 },
    GeoPolygon(Vec<(f64, f64)>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    pub field: String,
    pub order: SortOrder,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationSpec {
    pub page: u32,
    pub per_page: u32,
    pub offset: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighlightSpec {
    pub fields: Vec<String>,
    pub max_fragments: Option<u32>,
    pub fragment_size: Option<u32>,
    pub pre_tag: Option<String>,
    pub post_tag: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationSpec {
    pub name: String,
    pub field: String,
    pub aggregation_type: AggregationType,
    pub size: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationType {
    Terms,
    Range,
    DateHistogram,
    Histogram,
    GeoHash,
    Cardinality,
    Stats,
    ExtendedStats,
    Percentiles,
    TopHits,
}

/// Search result with highlighting and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub object_id: String,
    pub score: f64,
    pub highlights: HashMap<String, Vec<String>>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub matched_fields: Vec<String>,
}

/// Search response with results and aggregations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    pub total: u64,
    pub page: u32,
    pub per_page: u32,
    pub total_pages: u32,
    pub aggregations: HashMap<String, AggregationResult>,
    pub facets: HashMap<String, FacetResult>,
    pub suggestions: Vec<String>,
    pub query_time_ms: u64,
    pub max_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationResult {
    pub buckets: Vec<Bucket>,
    pub doc_count: u64,
    pub other_doc_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub key: serde_json::Value,
    pub doc_count: u64,
    pub sub_aggregations: Option<HashMap<String, AggregationResult>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetResult {
    pub field: String,
    pub values: Vec<FacetValue>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetValue {
    pub value: serde_json::Value,
    pub count: u64,
    pub selected: bool,
}

/// Indexed document for search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedDocument {
    pub id: String,
    pub fields: HashMap<String, IndexedField>,
    pub metadata: DocumentMetadata,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedField {
    pub value: FieldValue,
    pub indexed: bool,
    pub stored: bool,
    pub analyzed: bool,
    pub boost: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValue {
    Text(String),
    Keyword(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Date(DateTime<Utc>),
    GeoPoint { lat: f64, lon: f64 },
    Array(Vec<FieldValue>),
    Object(HashMap<String, FieldValue>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    pub object_id: String,
    pub bucket: String,
    pub content_type: Option<String>,
    pub size: u64,
    pub tags: HashMap<String, String>,
    pub custom_metadata: HashMap<String, String>,
    pub access_count: u64,
    pub last_accessed: Option<DateTime<Utc>>,
    pub storage_class: String,
    pub compression_ratio: Option<f64>,
    pub encryption_status: String,
}

/// Search index with multiple field types
pub struct SearchIndex {
    pub name: String,
    pub fields: HashMap<String, FieldMapping>,
    pub settings: IndexSettings,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    pub field_type: FieldType,
    pub analyzer: Option<String>,
    pub search_analyzer: Option<String>,
    pub index: bool,
    pub store: bool,
    pub boost: f64,
    pub properties: Option<HashMap<String, FieldMapping>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    Text,
    Keyword,
    Long,
    Integer,
    Short,
    Byte,
    Double,
    Float,
    Boolean,
    Date,
    GeoPoint,
    GeoShape,
    Nested,
    Object,
    Completion,
    TokenCount,
    Percolator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSettings {
    pub number_of_shards: u32,
    pub number_of_replicas: u32,
    pub refresh_interval: String,
    pub max_result_window: u32,
    pub analysis: AnalysisSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisSettings {
    pub analyzers: HashMap<String, Analyzer>,
    pub tokenizers: HashMap<String, Tokenizer>,
    pub filters: HashMap<String, Filter>,
    pub char_filters: HashMap<String, CharFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Analyzer {
    pub tokenizer: String,
    pub filters: Vec<String>,
    pub char_filters: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tokenizer {
    pub tokenizer_type: TokenizerType,
    pub parameters: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TokenizerType {
    Standard,
    Keyword,
    Letter,
    Lowercase,
    Whitespace,
    UaxUrlEmail,
    Pattern,
    NGram,
    EdgeNGram,
    PathHierarchy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub filter_type: FilterType,
    pub parameters: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterType {
    Lowercase,
    Uppercase,
    Stop,
    Stemmer,
    Synonym,
    AsciiFolding,
    Length,
    Truncate,
    Unique,
    Reverse,
    EdgeNGram,
    NGram,
    Shingle,
    WordDelimiter,
    PatternReplace,
    Trim,
    LimitTokenCount,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharFilter {
    pub char_filter_type: CharFilterType,
    pub parameters: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CharFilterType {
    HtmlStrip,
    Mapping,
    PatternReplace,
    Normalize,
}

/// Search engine with advanced capabilities
pub struct SearchEngine {
    indexes: Arc<RwLock<HashMap<String, SearchIndex>>>,
    documents: Arc<RwLock<HashMap<String, IndexedDocument>>>,
    inverted_index: Arc<RwLock<HashMap<String, HashMap<String, Vec<String>>>>>,
    field_indexes: Arc<RwLock<HashMap<String, BTreeMap<String, Vec<String>>>>>,
    analyzers: Arc<RwLock<HashMap<String, Analyzer>>>,
}

impl SearchEngine {
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
            documents: Arc::new(RwLock::new(HashMap::new())),
            inverted_index: Arc::new(RwLock::new(HashMap::new())),
            field_indexes: Arc::new(RwLock::new(HashMap::new())),
            analyzers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Create a new search index
    pub async fn create_index(&self, name: String, settings: IndexSettings) -> Result<()> {
        let mut indexes = self.indexes.write().await;
        
        if indexes.contains_key(&name) {
            return Err(NimbuxError::Search(format!("Index '{}' already exists", name)));
        }
        
        let index = SearchIndex {
            name: name.clone(),
            fields: HashMap::new(),
            settings,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        
        indexes.insert(name, index);
        Ok(())
    }
    
    /// Add field mapping to an index
    pub async fn add_field_mapping(
        &self,
        index_name: &str,
        field_name: String,
        mapping: FieldMapping,
    ) -> Result<()> {
        let mut indexes = self.indexes.write().await;
        
        if let Some(index) = indexes.get_mut(index_name) {
            index.fields.insert(field_name, mapping);
            index.updated_at = Utc::now();
            Ok(())
        } else {
            Err(NimbuxError::Search(format!("Index '{}' not found", index_name)))
        }
    }
    
    /// Index a document
    pub async fn index_document(&self, index_name: &str, document: IndexedDocument) -> Result<()> {
        // Validate index exists
        {
            let indexes = self.indexes.read().await;
            if !indexes.contains_key(index_name) {
                return Err(NimbuxError::Search(format!("Index '{}' not found", index_name)));
            }
        }
        
        // Store document
        {
            let mut documents = self.documents.write().await;
            documents.insert(document.id.clone(), document.clone());
        }
        
        // Build inverted index
        self.build_inverted_index(&document).await?;
        
        // Build field indexes
        self.build_field_indexes(&document).await?;
        
        Ok(())
    }
    
    /// Search documents
    pub async fn search(&self, index_name: &str, query: SearchQuery) -> Result<SearchResponse> {
        let start_time = std::time::Instant::now();
        
        // Validate index exists
        {
            let indexes = self.indexes.read().await;
            if !indexes.contains_key(index_name) {
                return Err(NimbuxError::Search(format!("Index '{}' not found", index_name)));
            }
        }
        
        // Execute search
        let mut results = Vec::new();
        let mut total = 0u64;
        
        if let Some(text) = &query.text {
            // Text search
            let text_results = self.text_search(text, &query).await?;
            results.extend(text_results);
        } else {
            // Filter-only search
            let filter_results = self.filter_search(&query).await?;
            results.extend(filter_results);
        }
        
        // Apply sorting
        if let Some(sort) = &query.sort {
            self.sort_results(&mut results, sort).await?;
        }
        
        // Calculate total
        total = results.len() as u64;
        
        // Apply pagination
        let (page, per_page) = if let Some(pagination) = &query.pagination {
            (pagination.page, pagination.per_page)
        } else {
            (1, 20)
        };
        
        let start = ((page - 1) * per_page) as usize;
        let end = std::cmp::min(start + per_page as usize, results.len());
        
        if start < results.len() {
            results = results[start..end].to_vec();
        } else {
            results.clear();
        }
        
        // Calculate aggregations
        let aggregations = if let Some(agg_specs) = &query.aggregations {
            self.calculate_aggregations(agg_specs, &results).await?
        } else {
            HashMap::new()
        };
        
        // Calculate facets
        let facets = if let Some(facet_fields) = &query.facets {
            self.calculate_facets(facet_fields, &results).await?
        } else {
            HashMap::new()
        };
        
        // Generate suggestions
        let suggestions = self.generate_suggestions(&query).await?;
        
        let query_time = start_time.elapsed();
        let max_score = results.iter().map(|r| r.score).fold(0.0, f64::max);
        
        Ok(SearchResponse {
            results,
            total,
            page,
            per_page,
            total_pages: (total + per_page - 1) / per_page,
            aggregations,
            facets,
            suggestions,
            query_time_ms: query_time.as_millis() as u64,
            max_score,
        })
    }
    
    /// Delete a document from the index
    pub async fn delete_document(&self, index_name: &str, document_id: &str) -> Result<()> {
        // Remove from documents
        {
            let mut documents = self.documents.write().await;
            documents.remove(document_id);
        }
        
        // Remove from inverted index
        self.remove_from_inverted_index(document_id).await?;
        
        // Remove from field indexes
        self.remove_from_field_indexes(document_id).await?;
        
        Ok(())
    }
    
    /// Update a document in the index
    pub async fn update_document(&self, index_name: &str, document: IndexedDocument) -> Result<()> {
        // Delete old version
        self.delete_document(index_name, &document.id).await?;
        
        // Index new version
        self.index_document(index_name, document).await?;
        
        Ok(())
    }
    
    /// Get document by ID
    pub async fn get_document(&self, document_id: &str) -> Result<Option<IndexedDocument>> {
        let documents = self.documents.read().await;
        Ok(documents.get(document_id).cloned())
    }
    
    /// Get index statistics
    pub async fn get_index_stats(&self, index_name: &str) -> Result<IndexStats> {
        let indexes = self.indexes.read().await;
        let documents = self.documents.read().await;
        
        if !indexes.contains_key(index_name) {
            return Err(NimbuxError::Search(format!("Index '{}' not found", index_name)));
        }
        
        let document_count = documents.len() as u64;
        let field_count = indexes.get(index_name).map(|i| i.fields.len()).unwrap_or(0) as u64;
        
        Ok(IndexStats {
            name: index_name.to_string(),
            document_count,
            field_count,
            created_at: indexes.get(index_name).map(|i| i.created_at).unwrap_or(Utc::now()),
            updated_at: indexes.get(index_name).map(|i| i.updated_at).unwrap_or(Utc::now()),
        })
    }
    
    // Private helper methods
    
    async fn text_search(&self, text: &str, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        let mut results = Vec::new();
        
        // Simple text search implementation
        // In a real implementation, this would use the inverted index
        let documents = self.documents.read().await;
        
        for (doc_id, document) in documents.iter() {
            let mut score = 0.0;
            let mut matched_fields = Vec::new();
            let mut highlights = HashMap::new();
            
            for (field_name, field) in &document.fields {
                if let FieldValue::Text(content) = &field.value {
                    if content.to_lowercase().contains(&text.to_lowercase()) {
                        score += field.boost;
                        matched_fields.push(field_name.clone());
                        
                        // Simple highlighting
                        let highlighted = self.highlight_text(content, text);
                        highlights.insert(field_name.clone(), vec![highlighted]);
                    }
                }
            }
            
            if score > 0.0 {
                let mut metadata = HashMap::new();
                metadata.insert("object_id".to_string(), serde_json::Value::String(document.metadata.object_id.clone()));
                metadata.insert("bucket".to_string(), serde_json::Value::String(document.metadata.bucket.clone()));
                metadata.insert("size".to_string(), serde_json::Value::Number(serde_json::Number::from(document.metadata.size)));
                
                results.push(SearchResult {
                    object_id: document.metadata.object_id.clone(),
                    score,
                    highlights,
                    metadata,
                    matched_fields,
                });
            }
        }
        
        // Sort by score
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(results)
    }
    
    async fn filter_search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        let mut results = Vec::new();
        let documents = self.documents.read().await;
        
        for (doc_id, document) in documents.iter() {
            let mut matches = true;
            
            for filter in &query.filters {
                if !self.matches_filter(document, filter)? {
                    matches = false;
                    break;
                }
            }
            
            if matches {
                let mut metadata = HashMap::new();
                metadata.insert("object_id".to_string(), serde_json::Value::String(document.metadata.object_id.clone()));
                metadata.insert("bucket".to_string(), serde_json::Value::String(document.metadata.bucket.clone()));
                metadata.insert("size".to_string(), serde_json::Value::Number(serde_json::Number::from(document.metadata.size)));
                
                results.push(SearchResult {
                    object_id: document.metadata.object_id.clone(),
                    score: 1.0,
                    highlights: HashMap::new(),
                    metadata,
                    matched_fields: Vec::new(),
                });
            }
        }
        
        Ok(results)
    }
    
    fn matches_filter(&self, document: &IndexedDocument, filter: &SearchFilter) -> Result<bool> {
        match filter.operator {
            FilterOperator::Equals => {
                if let Some(field) = document.fields.get(&filter.field) {
                    Ok(self.field_value_equals(&field.value, &filter.value))
                } else {
                    Ok(false)
                }
            }
            FilterOperator::NotEquals => {
                if let Some(field) = document.fields.get(&filter.field) {
                    Ok(!self.field_value_equals(&field.value, &filter.value))
                } else {
                    Ok(true)
                }
            }
            FilterOperator::Exists => {
                Ok(document.fields.contains_key(&filter.field))
            }
            FilterOperator::NotExists => {
                Ok(!document.fields.contains_key(&filter.field))
            }
            _ => {
                // TODO: Implement other operators
                Ok(true)
            }
        }
    }
    
    fn field_value_equals(&self, field_value: &FieldValue, filter_value: &FilterValue) -> bool {
        match (field_value, filter_value) {
            (FieldValue::Text(a), FilterValue::String(b)) => a == b,
            (FieldValue::Number(a), FilterValue::Number(b)) => (a - b).abs() < f64::EPSILON,
            (FieldValue::Integer(a), FilterValue::Integer(b)) => a == b,
            (FieldValue::Boolean(a), FilterValue::Boolean(b)) => a == b,
            (FieldValue::Date(a), FilterValue::Date(b)) => a == b,
            _ => false,
        }
    }
    
    async fn sort_results(&self, results: &mut [SearchResult], sort: &SortSpec) -> Result<()> {
        results.sort_by(|a, b| {
            let a_val = a.metadata.get(&sort.field);
            let b_val = b.metadata.get(&sort.field);
            
            match (a_val, b_val) {
                (Some(a), Some(b)) => {
                    let ordering = match sort.order {
                        SortOrder::Asc => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
                        SortOrder::Desc => b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal),
                    };
                    ordering
                }
                _ => std::cmp::Ordering::Equal,
            }
        });
        Ok(())
    }
    
    async fn calculate_aggregations(
        &self,
        _agg_specs: &[AggregationSpec],
        _results: &[SearchResult],
    ) -> Result<HashMap<String, AggregationResult>> {
        // TODO: Implement aggregations
        Ok(HashMap::new())
    }
    
    async fn calculate_facets(
        &self,
        _facet_fields: &[String],
        _results: &[SearchResult],
    ) -> Result<HashMap<String, FacetResult>> {
        // TODO: Implement facets
        Ok(HashMap::new())
    }
    
    async fn generate_suggestions(&self, _query: &SearchQuery) -> Result<Vec<String>> {
        // TODO: Implement suggestions
        Ok(Vec::new())
    }
    
    async fn build_inverted_index(&self, document: &IndexedDocument) -> Result<()> {
        let mut inverted_index = self.inverted_index.write().await;
        
        for (field_name, field) in &document.fields {
            if field.analyzed {
                if let FieldValue::Text(text) = &field.value {
                    let tokens = self.tokenize_text(text);
                    for token in tokens {
                        inverted_index
                            .entry(token)
                            .or_insert_with(HashMap::new)
                            .entry(field_name.clone())
                            .or_insert_with(Vec::new)
                            .push(document.id.clone());
                    }
                }
            }
        }
        
        Ok(())
    }
    
    async fn build_field_indexes(&self, document: &IndexedDocument) -> Result<()> {
        let mut field_indexes = self.field_indexes.write().await;
        
        for (field_name, field) in &document.fields {
            if field.indexed {
                let key = self.field_value_to_string(&field.value);
                field_indexes
                    .entry(field_name.clone())
                    .or_insert_with(BTreeMap::new)
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(document.id.clone());
            }
        }
        
        Ok(())
    }
    
    async fn remove_from_inverted_index(&self, document_id: &str) -> Result<()> {
        let mut inverted_index = self.inverted_index.write().await;
        
        for (_, field_map) in inverted_index.iter_mut() {
            for (_, doc_list) in field_map.iter_mut() {
                doc_list.retain(|id| id != document_id);
            }
        }
        
        Ok(())
    }
    
    async fn remove_from_field_indexes(&self, document_id: &str) -> Result<()> {
        let mut field_indexes = self.field_indexes.write().await;
        
        for (_, field_map) in field_indexes.iter_mut() {
            for (_, doc_list) in field_map.iter_mut() {
                doc_list.retain(|id| id != document_id);
            }
        }
        
        Ok(())
    }
    
    fn tokenize_text(&self, text: &str) -> Vec<String> {
        // Simple tokenization - split on whitespace and convert to lowercase
        text.split_whitespace()
            .map(|s| s.to_lowercase())
            .collect()
    }
    
    fn field_value_to_string(&self, value: &FieldValue) -> String {
        match value {
            FieldValue::Text(s) => s.clone(),
            FieldValue::Keyword(s) => s.clone(),
            FieldValue::Number(n) => n.to_string(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Date(d) => d.to_rfc3339(),
            FieldValue::GeoPoint { lat, lon } => format!("{},{}", lat, lon),
            FieldValue::Array(arr) => {
                arr.iter()
                    .map(|v| self.field_value_to_string(v))
                    .collect::<Vec<_>>()
                    .join(",")
            }
            FieldValue::Object(obj) => {
                obj.iter()
                    .map(|(k, v)| format!("{}:{}", k, self.field_value_to_string(v)))
                    .collect::<Vec<_>>()
                    .join(",")
            }
        }
    }
    
    fn highlight_text(&self, text: &str, query: &str) -> String {
        // Simple highlighting - wrap matches with <em> tags
        text.replace(query, &format!("<em>{}</em>", query))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub name: String,
    pub document_count: u64,
    pub field_count: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}