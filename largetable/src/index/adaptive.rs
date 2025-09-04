// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Adaptive indexing system that learns from query patterns

use crate::{Result, Document, DocumentId, Value, IndexType};
use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::{Utc, Duration};

/// Adaptive index manager that learns from query patterns
pub struct AdaptiveIndexManager {
    indexes: Arc<RwLock<HashMap<String, AdaptiveIndex>>>,
    query_stats: Arc<RwLock<QueryStatistics>>,
    learning_engine: Arc<RwLock<LearningEngine>>,
}

/// Adaptive index that can modify itself based on usage patterns
pub struct AdaptiveIndex {
    pub name: String,
    pub index_type: IndexType,
    pub fields: Vec<String>,
    pub usage_stats: IndexUsageStats,
    pub performance_metrics: PerformanceMetrics,
    pub last_updated: chrono::DateTime<Utc>,
    pub auto_optimize: bool,
}

/// Index usage statistics
#[derive(Debug, Clone)]
pub struct IndexUsageStats {
    pub total_queries: u64,
    pub successful_queries: u64,
    pub average_query_time: f64,
    pub last_used: chrono::DateTime<Utc>,
    pub hit_rate: f32,
    pub memory_usage: usize,
}

/// Performance metrics for an index
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub build_time: Duration,
    pub query_time: Duration,
    pub memory_efficiency: f32,
    pub selectivity: f32,
    pub cardinality: u64,
}

/// Query statistics for learning
pub struct QueryStatistics {
    pub query_patterns: HashMap<String, QueryPattern>,
    pub field_usage: HashMap<String, FieldUsageStats>,
    pub performance_history: Vec<PerformanceSnapshot>,
}

/// Query pattern analysis
#[derive(Debug, Clone)]
pub struct QueryPattern {
    pub pattern_id: String,
    pub frequency: u64,
    pub average_execution_time: f64,
    pub fields_used: Vec<String>,
    pub filters: Vec<FilterPattern>,
    pub sort_fields: Vec<String>,
    pub last_seen: chrono::DateTime<Utc>,
}

/// Filter pattern analysis
#[derive(Debug, Clone)]
pub struct FilterPattern {
    pub field: String,
    pub operator: String,
    pub value_type: String,
    pub frequency: u64,
}

/// Field usage statistics
#[derive(Debug, Clone)]
pub struct FieldUsageStats {
    pub field_name: String,
    pub query_count: u64,
    pub filter_count: u64,
    pub sort_count: u64,
    pub projection_count: u64,
    pub average_selectivity: f32,
}

/// Performance snapshot
#[derive(Debug, Clone)]
pub struct PerformanceSnapshot {
    pub timestamp: chrono::DateTime<Utc>,
    pub query_time: f64,
    pub memory_usage: usize,
    pub index_hits: u64,
    pub index_misses: u64,
}

/// Learning engine for adaptive optimization
pub struct LearningEngine {
    pub learning_rate: f32,
    pub optimization_threshold: f32,
    pub memory_threshold: f32,
    pub performance_window: Duration,
}

impl AdaptiveIndexManager {
    /// Create a new adaptive index manager
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
            query_stats: Arc::new(RwLock::new(QueryStatistics::new())),
            learning_engine: Arc::new(RwLock::new(LearningEngine::new())),
        }
    }

    /// Create an adaptive index
    pub async fn create_index(
        &self,
        name: String,
        index_type: IndexType,
        fields: Vec<String>,
    ) -> Result<()> {
        let mut indexes = self.indexes.write().await;
        
        let adaptive_index = AdaptiveIndex {
            name: name.clone(),
            index_type,
            fields: fields.clone(),
            usage_stats: IndexUsageStats::new(),
            performance_metrics: PerformanceMetrics::new(),
            last_updated: Utc::now(),
            auto_optimize: true,
        };

        indexes.insert(name, adaptive_index);
        Ok(())
    }

    /// Record a query execution for learning
    pub async fn record_query(
        &self,
        query_id: String,
        fields_used: Vec<String>,
        filters: Vec<FilterPattern>,
        sort_fields: Vec<String>,
        execution_time: f64,
        index_hits: u64,
        index_misses: u64,
    ) -> Result<()> {
        let mut query_stats = self.query_stats.write().await;
        
        // Update query pattern
        let pattern = query_stats.query_patterns.entry(query_id.clone())
            .or_insert_with(|| QueryPattern::new(query_id));
        
        pattern.frequency += 1;
        pattern.average_execution_time = (pattern.average_execution_time * (pattern.frequency - 1) as f64 + execution_time) / pattern.frequency as f64;
        pattern.fields_used = fields_used;
        pattern.filters = filters;
        pattern.sort_fields = sort_fields;
        pattern.last_seen = Utc::now();

        // Update field usage statistics
        for field in &pattern.fields_used {
            let field_stats = query_stats.field_usage.entry(field.clone())
                .or_insert_with(|| FieldUsageStats::new(field.clone()));
            
            field_stats.query_count += 1;
        }

        // Record performance snapshot
        let snapshot = PerformanceSnapshot {
            timestamp: Utc::now(),
            query_time: execution_time,
            memory_usage: 0, // Placeholder
            index_hits,
            index_misses,
        };
        query_stats.performance_history.push(snapshot);

        // Trigger learning if needed
        self.trigger_learning().await?;

        Ok(())
    }

    /// Get index recommendations based on query patterns
    pub async fn get_index_recommendations(&self) -> Result<Vec<IndexRecommendation>> {
        let query_stats = self.query_stats.read().await;
        let learning_engine = self.learning_engine.read().await;
        
        let mut recommendations = Vec::new();
        
        // Analyze field usage patterns
        for (field_name, field_stats) in &query_stats.field_usage {
            if field_stats.query_count > learning_engine.optimization_threshold as u64 {
                let recommendation = IndexRecommendation {
                    field_name: field_name.clone(),
                    index_type: self.recommend_index_type(field_stats),
                    priority: self.calculate_priority(field_stats),
                    estimated_benefit: self.estimate_benefit(field_stats),
                    reason: format!("High usage: {} queries", field_stats.query_count),
                };
                recommendations.push(recommendation);
            }
        }

        // Analyze query patterns for composite indexes
        for (_, pattern) in &query_stats.query_patterns {
            if pattern.fields_used.len() > 1 && pattern.frequency > 10 {
                let recommendation = IndexRecommendation {
                    field_name: pattern.fields_used.join(", "),
                    index_type: IndexType::BTree, // Default for composite
                    priority: self.calculate_composite_priority(pattern),
                    estimated_benefit: self.estimate_composite_benefit(pattern),
                    reason: format!("Frequent multi-field query: {} occurrences", pattern.frequency),
                };
                recommendations.push(recommendation);
            }
        }

        // Sort by priority
        recommendations.sort_by(|a, b| b.priority.partial_cmp(&a.priority).unwrap());
        
        Ok(recommendations)
    }

    /// Optimize indexes based on current usage patterns
    pub async fn optimize_indexes(&self) -> Result<Vec<OptimizationAction>> {
        let mut actions = Vec::new();
        let indexes = self.indexes.read().await;
        let query_stats = self.query_stats.read().await;
        
        for (name, index) in indexes.iter() {
            if !index.auto_optimize {
                continue;
            }

            // Check if index should be dropped due to low usage
            if index.usage_stats.total_queries < 10 && index.usage_stats.last_used < Utc::now() - Duration::days(30) {
                actions.push(OptimizationAction::DropIndex {
                    name: name.clone(),
                    reason: "Low usage and old".to_string(),
                });
                continue;
            }

            // Check if index needs to be rebuilt due to poor performance
            if index.performance_metrics.query_time > Duration::milliseconds(100) {
                actions.push(OptimizationAction::RebuildIndex {
                    name: name.clone(),
                    reason: "Poor performance".to_string(),
                });
            }

            // Check if index needs additional fields
            let field_usage = query_stats.field_usage.get(&index.fields[0]);
            if let Some(usage) = field_usage {
                if usage.average_selectivity < 0.1 && index.usage_stats.hit_rate < 0.8 {
                    actions.push(OptimizationAction::AddFields {
                        name: name.clone(),
                        fields: vec!["additional_field".to_string()], // Placeholder
                        reason: "Low selectivity".to_string(),
                    });
                }
            }
        }

        Ok(actions)
    }

    async fn trigger_learning(&self) -> Result<()> {
        let learning_engine = self.learning_engine.read().await;
        let query_stats = self.query_stats.read().await;
        
        // Check if we have enough data to trigger learning
        if query_stats.performance_history.len() < 100 {
            return Ok(());
        }

        // Analyze recent performance trends
        let recent_snapshots: Vec<&PerformanceSnapshot> = query_stats.performance_history
            .iter()
            .filter(|s| s.timestamp > Utc::now() - learning_engine.performance_window)
            .collect();

        if recent_snapshots.is_empty() {
            return Ok(());
        }

        let average_query_time = recent_snapshots.iter()
            .map(|s| s.query_time)
            .sum::<f64>() / recent_snapshots.len() as f64;

        // If performance is degrading, trigger optimization
        if average_query_time > 1000.0 { // 1 second threshold
            self.optimize_indexes().await?;
        }

        Ok(())
    }

    fn recommend_index_type(&self, field_stats: &FieldUsageStats) -> IndexType {
        // Simple heuristic - in practice, use more sophisticated analysis
        if field_stats.filter_count > field_stats.sort_count {
            IndexType::Hash
        } else if field_stats.sort_count > field_stats.filter_count {
            IndexType::BTree
        } else {
            IndexType::BTree
        }
    }

    fn calculate_priority(&self, field_stats: &FieldUsageStats) -> f32 {
        let usage_score = field_stats.query_count as f32;
        let selectivity_score = 1.0 - field_stats.average_selectivity;
        usage_score * selectivity_score
    }

    fn calculate_composite_priority(&self, pattern: &QueryPattern) -> f32 {
        pattern.frequency as f32 * (1.0 / pattern.average_execution_time)
    }

    fn estimate_benefit(&self, field_stats: &FieldUsageStats) -> f32 {
        // Estimate based on query count and selectivity
        field_stats.query_count as f32 * (1.0 - field_stats.average_selectivity)
    }

    fn estimate_composite_benefit(&self, pattern: &QueryPattern) -> f32 {
        pattern.frequency as f32 * 0.5 // Placeholder
    }
}

/// Index recommendation
#[derive(Debug, Clone)]
pub struct IndexRecommendation {
    pub field_name: String,
    pub index_type: IndexType,
    pub priority: f32,
    pub estimated_benefit: f32,
    pub reason: String,
}

/// Optimization action
#[derive(Debug, Clone)]
pub enum OptimizationAction {
    CreateIndex {
        name: String,
        index_type: IndexType,
        fields: Vec<String>,
        reason: String,
    },
    DropIndex {
        name: String,
        reason: String,
    },
    RebuildIndex {
        name: String,
        reason: String,
    },
    AddFields {
        name: String,
        fields: Vec<String>,
        reason: String,
    },
}

impl IndexUsageStats {
    fn new() -> Self {
        Self {
            total_queries: 0,
            successful_queries: 0,
            average_query_time: 0.0,
            last_used: Utc::now(),
            hit_rate: 0.0,
            memory_usage: 0,
        }
    }
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            build_time: Duration::zero(),
            query_time: Duration::zero(),
            memory_efficiency: 1.0,
            selectivity: 1.0,
            cardinality: 0,
        }
    }
}

impl QueryStatistics {
    fn new() -> Self {
        Self {
            query_patterns: HashMap::new(),
            field_usage: HashMap::new(),
            performance_history: Vec::new(),
        }
    }
}

impl QueryPattern {
    fn new(pattern_id: String) -> Self {
        Self {
            pattern_id,
            frequency: 0,
            average_execution_time: 0.0,
            fields_used: Vec::new(),
            filters: Vec::new(),
            sort_fields: Vec::new(),
            last_seen: Utc::now(),
        }
    }
}

impl FieldUsageStats {
    fn new(field_name: String) -> Self {
        Self {
            field_name,
            query_count: 0,
            filter_count: 0,
            sort_count: 0,
            projection_count: 0,
            average_selectivity: 1.0,
        }
    }
}

impl LearningEngine {
    fn new() -> Self {
        Self {
            learning_rate: 0.1,
            optimization_threshold: 10.0,
            memory_threshold: 0.8,
            performance_window: Duration::hours(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_adaptive_index_manager() {
        let manager = AdaptiveIndexManager::new();
        
        manager.create_index(
            "test_index".to_string(),
            IndexType::BTree,
            vec!["field1".to_string()],
        ).await.unwrap();
        
        manager.record_query(
            "query1".to_string(),
            vec!["field1".to_string()],
            vec![],
            vec![],
            100.0,
            1,
            0,
        ).await.unwrap();
        
        let recommendations = manager.get_index_recommendations().await.unwrap();
        assert!(!recommendations.is_empty());
    }
}