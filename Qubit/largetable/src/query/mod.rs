// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Advanced query engine with multiple query types

pub mod document;
pub mod executor;
pub mod graph;
pub mod joins;
pub mod optimizer;
pub mod parser;
pub mod streaming;
pub mod timeseries;
pub mod vector;
pub mod aggregation;

use crate::{Result, DocumentId, Document, LargetableError};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tracing::{debug, error};

/// Query builder for creating complex queries
pub struct QueryBuilder {
    filter: Option<JsonValue>,
    sort: Vec<SortField>,
    limit: Option<usize>,
    skip: Option<usize>,
    projection: Option<Vec<String>>,
}

/// Sort field specification
#[derive(Debug, Clone)]
pub struct SortField {
    pub field: String,
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Query result with metadata
#[derive(Debug)]
pub struct QueryResult {
    pub documents: Vec<(DocumentId, Document)>,
    pub total_count: usize,
    pub has_more: bool,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self {
            filter: None,
            sort: Vec::new(),
            limit: None,
            skip: None,
            projection: None,
        }
    }

    /// Add a filter to the query
    pub fn filter(mut self, filter: JsonValue) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Add a sort field to the query
    pub fn sort(mut self, field: String, direction: SortDirection) -> Self {
        self.sort.push(SortField { field, direction });
        self
    }

    /// Set the limit for the query
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the skip count for the query
    pub fn skip(mut self, skip: usize) -> Self {
        self.skip = Some(skip);
        self
    }

    /// Set the projection fields for the query
    pub fn projection(mut self, fields: Vec<String>) -> Self {
        self.projection = Some(fields);
        self
    }

    /// Build the query
    pub fn build(self) -> Query {
        Query {
            filter: self.filter,
            sort: self.sort,
            limit: self.limit,
            skip: self.skip,
            projection: self.projection,
        }
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Query representation
#[derive(Debug, Clone)]
pub struct Query {
    pub filter: Option<JsonValue>,
    pub sort: Vec<SortField>,
    pub limit: Option<usize>,
    pub skip: Option<usize>,
    pub projection: Option<Vec<String>>,
}

impl Query {
    /// Create a new query
    pub fn new() -> Self {
        Self {
            filter: None,
            sort: Vec::new(),
            limit: None,
            skip: None,
            projection: None,
        }
    }

    /// Execute the query against a collection of documents
    pub async fn execute(&self, documents: Vec<(DocumentId, Document)>) -> Result<QueryResult> {
        let mut filtered_docs = documents;

        // Apply filter
        if let Some(filter) = &self.filter {
            filtered_docs = self.apply_filter(filtered_docs, filter).await?;
        }

        // Apply sorting
        if !self.sort.is_empty() {
            filtered_docs = self.apply_sorting(filtered_docs).await?;
        }

        // Apply skip
        let total_count = filtered_docs.len();
        let skip_count = self.skip.unwrap_or(0);
        if skip_count > 0 && skip_count < filtered_docs.len() {
            filtered_docs = filtered_docs.into_iter().skip(skip_count).collect();
        }

        // Apply limit
        let has_more = if let Some(limit) = self.limit {
            if filtered_docs.len() > limit {
                filtered_docs = filtered_docs.into_iter().take(limit).collect();
                true
            } else {
                false
            }
        } else {
            false
        };

        // Apply projection
        if let Some(projection) = &self.projection {
            filtered_docs = self.apply_projection(filtered_docs, projection).await?;
        }

        debug!("Query executed: {} documents returned, {} total, has_more: {}", 
               filtered_docs.len(), total_count, has_more);

        Ok(QueryResult {
            documents: filtered_docs,
            total_count,
            has_more,
        })
    }

    /// Apply filter to documents
    async fn apply_filter(&self, mut documents: Vec<(DocumentId, Document)>, filter: &JsonValue) -> Result<Vec<(DocumentId, Document)>> {
        use crate::document::DocumentUtils;
        
        let mut filtered = Vec::new();
        for (id, doc) in documents {
            if DocumentUtils::matches_filter(&doc, filter)? {
                filtered.push((id, doc));
            }
        }
        
        Ok(filtered)
    }

    /// Apply sorting to documents
    async fn apply_sorting(&self, mut documents: Vec<(DocumentId, Document)>) -> Result<Vec<(DocumentId, Document)>> {
        use crate::document::DocumentUtils;
        
        documents.sort_by(|a, b| {
            for sort_field in &self.sort {
                let a_value = DocumentUtils::get_field(&a.1, &sort_field.field);
                let b_value = DocumentUtils::get_field(&b.1, &sort_field.field);
                
                let comparison = match (a_value, b_value) {
                    (Some(a_val), Some(b_val)) => {
                        match (a_val, b_val) {
                            (crate::Value::String(a_str), crate::Value::String(b_str)) => a_str.cmp(b_str),
                            (crate::Value::Int64(a_int), crate::Value::Int64(b_int)) => a_int.cmp(b_int),
                            (crate::Value::Float64(a_float), crate::Value::Float64(b_float)) => a_float.partial_cmp(b_float).unwrap_or(std::cmp::Ordering::Equal),
                            (crate::Value::Bool(a_bool), crate::Value::Bool(b_bool)) => a_bool.cmp(b_bool),
                            (crate::Value::Timestamp(a_ts), crate::Value::Timestamp(b_ts)) => a_ts.cmp(b_ts),
                            _ => std::cmp::Ordering::Equal,
                        }
                    }
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                
                match sort_field.direction {
                    SortDirection::Ascending => comparison,
                    SortDirection::Descending => comparison.reverse(),
                }
            }
            std::cmp::Ordering::Equal
        });
        
        Ok(documents)
    }

    /// Apply projection to documents
    async fn apply_projection(&self, documents: Vec<(DocumentId, Document)>, projection: &[String]) -> Result<Vec<(DocumentId, Document)>> {
        let mut projected = Vec::new();
        
        for (id, mut doc) in documents {
            let mut new_fields = HashMap::new();
            
            // Always include _id, _version, _created_at, _updated_at
            new_fields.insert("_id".to_string(), crate::Value::String(id.to_string()));
            new_fields.insert("_version".to_string(), crate::Value::UInt64(doc.version));
            new_fields.insert("_created_at".to_string(), crate::Value::Timestamp(doc.created_at));
            new_fields.insert("_updated_at".to_string(), crate::Value::Timestamp(doc.updated_at));
            
            // Add projected fields
            for field in projection {
                if let Some(value) = doc.fields.get(field) {
                    new_fields.insert(field.clone(), value.clone());
                }
            }
            
            doc.fields = new_fields;
            projected.push((id, doc));
        }
        
        Ok(projected)
    }
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregation pipeline for complex data processing
pub struct AggregationPipeline {
    stages: Vec<AggregationStage>,
}

/// Aggregation stage types
#[derive(Debug, Clone)]
pub enum AggregationStage {
    Match(JsonValue),
    Group {
        by: String,
        accumulators: HashMap<String, Accumulator>,
    },
    Sort(Vec<SortField>),
    Limit(usize),
    Skip(usize),
    Project(Vec<String>),
    Unwind(String),
    Lookup {
        from: String,
        local_field: String,
        foreign_field: String,
        as_field: String,
    },
}

/// Aggregation accumulator functions
#[derive(Debug, Clone)]
pub enum Accumulator {
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    Count,
    First(String),
    Last(String),
}

impl AggregationPipeline {
    /// Create a new aggregation pipeline
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
        }
    }

    /// Add a match stage
    pub fn match_stage(mut self, filter: JsonValue) -> Self {
        self.stages.push(AggregationStage::Match(filter));
        self
    }

    /// Add a group stage
    pub fn group(mut self, by: String, accumulators: HashMap<String, Accumulator>) -> Self {
        self.stages.push(AggregationStage::Group { by, accumulators });
        self
    }

    /// Add a sort stage
    pub fn sort(mut self, sort_fields: Vec<SortField>) -> Self {
        self.stages.push(AggregationStage::Sort(sort_fields));
        self
    }

    /// Add a limit stage
    pub fn limit(mut self, limit: usize) -> Self {
        self.stages.push(AggregationStage::Limit(limit));
        self
    }

    /// Add a skip stage
    pub fn skip(mut self, skip: usize) -> Self {
        self.stages.push(AggregationStage::Skip(skip));
        self
    }

    /// Add a project stage
    pub fn project(mut self, fields: Vec<String>) -> Self {
        self.stages.push(AggregationStage::Project(fields));
        self
    }

    /// Add an unwind stage
    pub fn unwind(mut self, field: String) -> Self {
        self.stages.push(AggregationStage::Unwind(field));
        self
    }

    /// Add a lookup stage
    pub fn lookup(mut self, from: String, local_field: String, foreign_field: String, as_field: String) -> Self {
        self.stages.push(AggregationStage::Lookup {
            from,
            local_field,
            foreign_field,
            as_field,
        });
        self
    }

    /// Execute the aggregation pipeline
    pub async fn execute(&self, documents: Vec<(DocumentId, Document)>) -> Result<Vec<JsonValue>> {
        let mut current_docs = documents;
        
        for stage in &self.stages {
            current_docs = self.execute_stage(stage, current_docs).await?;
        }
        
        // Convert documents to JSON
        let mut results = Vec::new();
        for (_, doc) in current_docs {
            results.push(crate::document::DocumentUtils::to_json(&doc)?);
        }
        
        Ok(results)
    }

    /// Execute a single aggregation stage
    async fn execute_stage(&self, stage: &AggregationStage, documents: Vec<(DocumentId, Document)>) -> Result<Vec<(DocumentId, Document)>> {
        match stage {
            AggregationStage::Match(filter) => {
                let query = Query::new().filter(filter.clone());
                let result = query.execute(documents).await?;
                Ok(result.documents)
            }
            AggregationStage::Group { by, accumulators } => {
                self.execute_group_stage(by, accumulators, documents).await
            }
            AggregationStage::Sort(sort_fields) => {
                let query = Query::new().sort(sort_fields.clone());
                let result = query.execute(documents).await?;
                Ok(result.documents)
            }
            AggregationStage::Limit(limit) => {
                let query = Query::new().limit(*limit);
                let result = query.execute(documents).await?;
                Ok(result.documents)
            }
            AggregationStage::Skip(skip) => {
                let query = Query::new().skip(*skip);
                let result = query.execute(documents).await?;
                Ok(result.documents)
            }
            AggregationStage::Project(fields) => {
                let query = Query::new().projection(fields.clone());
                let result = query.execute(documents).await?;
                Ok(result.documents)
            }
            AggregationStage::Unwind(field) => {
                self.execute_unwind_stage(field, documents).await
            }
            AggregationStage::Lookup { .. } => {
                // Lookup stage would require access to other collections
                // For now, just return the documents as-is
                Ok(documents)
            }
        }
    }

    /// Execute group stage
    async fn execute_group_stage(
        &self,
        by: &str,
        accumulators: &HashMap<String, Accumulator>,
        documents: Vec<(DocumentId, Document)>,
    ) -> Result<Vec<(DocumentId, Document)>> {
        use crate::document::DocumentUtils;
        
        let mut groups: HashMap<String, Vec<(DocumentId, Document)>> = HashMap::new();
        
        // Group documents by the specified field
        for (id, doc) in documents {
            let group_key = if let Some(value) = DocumentUtils::get_field(&doc, by) {
                match value {
                    crate::Value::String(s) => s.clone(),
                    crate::Value::Int64(i) => i.to_string(),
                    crate::Value::Float64(f) => f.to_string(),
                    crate::Value::Bool(b) => b.to_string(),
                    _ => "null".to_string(),
                }
            } else {
                "null".to_string()
            };
            
            groups.entry(group_key).or_default().push((id, doc));
        }
        
        // Apply accumulators to each group
        let mut result = Vec::new();
        for (group_key, group_docs) in groups {
            let mut aggregated_doc = DocumentBuilder::new()
                .string("_id", group_key.clone())
                .build();
            
            for (field, accumulator) in accumulators {
                let value = match accumulator {
                    Accumulator::Sum(field_name) => {
                        let sum: f64 = group_docs.iter()
                            .filter_map(|(_, doc)| {
                                DocumentUtils::get_field(doc, field_name)
                                    .and_then(|v| match v {
                                        crate::Value::Int64(i) => Some(*i as f64),
                                        crate::Value::Float64(f) => Some(*f),
                                        _ => None,
                                    })
                            })
                            .sum();
                        crate::Value::Float64(sum)
                    }
                    Accumulator::Count => {
                        crate::Value::Int64(group_docs.len() as i64)
                    }
                    Accumulator::Avg(field_name) => {
                        let values: Vec<f64> = group_docs.iter()
                            .filter_map(|(_, doc)| {
                                DocumentUtils::get_field(doc, field_name)
                                    .and_then(|v| match v {
                                        crate::Value::Int64(i) => Some(*i as f64),
                                        crate::Value::Float64(f) => Some(*f),
                                        _ => None,
                                    })
                            })
                            .collect();
                        
                        if values.is_empty() {
                            crate::Value::Null
                        } else {
                            let avg = values.iter().sum::<f64>() / values.len() as f64;
                            crate::Value::Float64(avg)
                        }
                    }
                    Accumulator::Min(field_name) => {
                        let min = group_docs.iter()
                            .filter_map(|(_, doc)| {
                                DocumentUtils::get_field(doc, field_name)
                                    .and_then(|v| match v {
                                        crate::Value::Int64(i) => Some(*i as f64),
                                        crate::Value::Float64(f) => Some(*f),
                                        _ => None,
                                    })
                            })
                            .fold(f64::INFINITY, f64::min);
                        
                        if min == f64::INFINITY {
                            crate::Value::Null
                        } else {
                            crate::Value::Float64(min)
                        }
                    }
                    Accumulator::Max(field_name) => {
                        let max = group_docs.iter()
                            .filter_map(|(_, doc)| {
                                DocumentUtils::get_field(doc, field_name)
                                    .and_then(|v| match v {
                                        crate::Value::Int64(i) => Some(*i as f64),
                                        crate::Value::Float64(f) => Some(*f),
                                        _ => None,
                                    })
                            })
                            .fold(f64::NEG_INFINITY, f64::max);
                        
                        if max == f64::NEG_INFINITY {
                            crate::Value::Null
                        } else {
                            crate::Value::Float64(max)
                        }
                    }
                    Accumulator::First(field_name) => {
                        group_docs.first()
                            .and_then(|(_, doc)| DocumentUtils::get_field(doc, field_name))
                            .cloned()
                            .unwrap_or(crate::Value::Null)
                    }
                    Accumulator::Last(field_name) => {
                        group_docs.last()
                            .and_then(|(_, doc)| DocumentUtils::get_field(doc, field_name))
                            .cloned()
                            .unwrap_or(crate::Value::Null)
                    }
                };
                
                aggregated_doc.fields.insert(field.clone(), value);
            }
            
            result.push((uuid::Uuid::now_v7(), aggregated_doc));
        }
        
        Ok(result)
    }

    /// Execute unwind stage
    async fn execute_unwind_stage(&self, field: &str, documents: Vec<(DocumentId, Document)>) -> Result<Vec<(DocumentId, Document)>> {
        use crate::document::DocumentUtils;
        
        let mut result = Vec::new();
        
        for (id, doc) in documents {
            if let Some(value) = DocumentUtils::get_field(&doc, field) {
                match value {
                    crate::Value::Array(arr) => {
                        for item in arr {
                            let mut new_doc = doc.clone();
                            DocumentUtils::set_field(&mut new_doc, field, item.clone())?;
                            result.push((uuid::Uuid::now_v7(), new_doc));
                        }
                    }
                    _ => {
                        result.push((id, doc));
                    }
                }
            } else {
                result.push((id, doc));
            }
        }
        
        Ok(result)
    }
}

impl Default for AggregationPipeline {
    fn default() -> Self {
        Self::new()
    }
}

// Re-export DocumentBuilder for convenience
pub use crate::document::DocumentBuilder;