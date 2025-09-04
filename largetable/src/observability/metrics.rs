// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Advanced metrics collection and monitoring

use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use chrono::{Utc, DateTime};

/// Metrics collector for performance monitoring
pub struct MetricsCollector {
    pub counters: HashMap<String, Counter>,
    pub gauges: HashMap<String, Gauge>,
    pub histograms: HashMap<String, Histogram>,
}

/// Counter metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Counter {
    pub name: String,
    pub value: u64,
    pub labels: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

/// Gauge metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gauge {
    pub name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

/// Histogram metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub name: String,
    pub buckets: Vec<HistogramBucket>,
    pub count: u64,
    pub sum: f64,
    pub labels: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

/// Histogram bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub upper_bound: f64,
    pub count: u64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
        }
    }

    pub async fn increment_counter(
        &mut self,
        name: String,
        labels: HashMap<String, String>,
    ) -> Result<()> {
        let counter = self.counters.entry(name.clone()).or_insert_with(|| Counter {
            name: name.clone(),
            value: 0,
            labels,
            created_at: Utc::now(),
            last_updated: Utc::now(),
        });
        counter.value += 1;
        counter.last_updated = Utc::now();
        Ok(())
    }

    pub async fn record_gauge(
        &mut self,
        name: String,
        value: f64,
        labels: HashMap<String, String>,
    ) -> Result<()> {
        let gauge = Gauge {
            name: name.clone(),
            value,
            labels,
            created_at: Utc::now(),
            last_updated: Utc::now(),
        };
        self.gauges.insert(name, gauge);
        Ok(())
    }

    pub async fn record_histogram(
        &mut self,
        name: String,
        value: f64,
        labels: HashMap<String, String>,
    ) -> Result<()> {
        let histogram = self.histograms.entry(name.clone()).or_insert_with(|| Histogram {
            name: name.clone(),
            buckets: vec![],
            count: 0,
            sum: 0.0,
            labels,
            created_at: Utc::now(),
            last_updated: Utc::now(),
        });
        histogram.count += 1;
        histogram.sum += value;
        histogram.last_updated = Utc::now();
        Ok(())
    }

    pub fn get_counter(&self, name: &str) -> Option<&Counter> {
        self.counters.get(name)
    }

    pub fn get_gauge(&self, name: &str) -> Option<&Gauge> {
        self.gauges.get(name)
    }

    pub fn get_histogram(&self, name: &str) -> Option<&Histogram> {
        self.histograms.get(name)
    }

    pub fn get_all_metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            counters: self.counters.clone(),
            gauges: self.gauges.clone(),
            histograms: self.histograms.clone(),
            timestamp: Utc::now(),
        }
    }
}

/// Metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub counters: HashMap<String, Counter>,
    pub gauges: HashMap<String, Gauge>,
    pub histograms: HashMap<String, Histogram>,
    pub timestamp: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collector() {
        let mut collector = MetricsCollector::new();
        
        let mut labels = HashMap::new();
        labels.insert("service".to_string(), "test".to_string());
        
        collector.increment_counter("test_counter".to_string(), labels.clone()).await.unwrap();
        collector.record_gauge("test_gauge".to_string(), 42.0, labels.clone()).await.unwrap();
        collector.record_histogram("test_histogram".to_string(), 1.5, labels).await.unwrap();
        
        assert_eq!(collector.get_counter("test_counter").unwrap().value, 1);
        assert_eq!(collector.get_gauge("test_gauge").unwrap().value, 42.0);
        assert_eq!(collector.get_histogram("test_histogram").unwrap().count, 1);
    }
}