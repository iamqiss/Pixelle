// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Time-series aggregation operations

use crate::{Result, Value, Timestamp};
use std::collections::HashMap;

/// Time-series aggregation types
#[derive(Debug, Clone)]
pub enum AggregationType {
    Sum,
    Mean,
    Min,
    Max,
    Count,
    Median,
    StdDev,
    Variance,
}

/// Time-series aggregation result
#[derive(Debug)]
pub struct AggregationResult {
    pub value: Value,
    pub count: u64,
    pub timestamp_range: (Timestamp, Timestamp),
}

/// Time-series aggregator
pub struct TimeSeriesAggregator {
    aggregation_type: AggregationType,
    values: Vec<f64>,
    timestamps: Vec<Timestamp>,
}

impl TimeSeriesAggregator {
    pub fn new(aggregation_type: AggregationType) -> Self {
        Self {
            aggregation_type,
            values: Vec::new(),
            timestamps: Vec::new(),
        }
    }

    pub fn add_value(&mut self, value: Value, timestamp: Timestamp) -> Result<()> {
        if let Some(numeric_value) = self.extract_numeric_value(&value) {
            self.values.push(numeric_value);
            self.timestamps.push(timestamp);
        }
        Ok(())
    }

    pub fn compute(&self) -> Result<AggregationResult> {
        if self.values.is_empty() {
            return Ok(AggregationResult {
                value: Value::Null,
                count: 0,
                timestamp_range: (0, 0),
            });
        }

        let count = self.values.len() as u64;
        let start_time = *self.timestamps.first().unwrap();
        let end_time = *self.timestamps.last().unwrap();

        let result_value = match self.aggregation_type {
            AggregationType::Sum => Value::Float64(self.values.iter().sum()),
            AggregationType::Mean => Value::Float64(self.values.iter().sum::<f64>() / count as f64),
            AggregationType::Min => Value::Float64(self.values.iter().fold(f64::INFINITY, |a, &b| a.min(b))),
            AggregationType::Max => Value::Float64(self.values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))),
            AggregationType::Count => Value::Int64(count as i64),
            AggregationType::Median => {
                let mut sorted = self.values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let median = if sorted.len() % 2 == 0 {
                    let mid = sorted.len() / 2;
                    (sorted[mid - 1] + sorted[mid]) / 2.0
                } else {
                    sorted[sorted.len() / 2]
                };
                Value::Float64(median)
            },
            AggregationType::StdDev => {
                let mean = self.values.iter().sum::<f64>() / count as f64;
                let variance = self.values.iter()
                    .map(|v| (v - mean).powi(2))
                    .sum::<f64>() / count as f64;
                Value::Float64(variance.sqrt())
            },
            AggregationType::Variance => {
                let mean = self.values.iter().sum::<f64>() / count as f64;
                let variance = self.values.iter()
                    .map(|v| (v - mean).powi(2))
                    .sum::<f64>() / count as f64;
                Value::Float64(variance)
            },
        };

        Ok(AggregationResult {
            value: result_value,
            count,
            timestamp_range: (start_time, end_time),
        })
    }

    fn extract_numeric_value(&self, value: &Value) -> Option<f64> {
        match value {
            Value::Float64(f) => Some(*f),
            Value::Float32(f) => Some(*f as f64),
            Value::Int64(i) => Some(*i as f64),
            Value::Int32(i) => Some(*i as f64),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregation() {
        let mut aggregator = TimeSeriesAggregator::new(AggregationType::Mean);
        
        aggregator.add_value(Value::Float64(10.0), 1000).unwrap();
        aggregator.add_value(Value::Float64(20.0), 2000).unwrap();
        aggregator.add_value(Value::Float64(30.0), 3000).unwrap();
        
        let result = aggregator.compute().unwrap();
        assert_eq!(result.count, 3);
        assert_eq!(result.timestamp_range, (1000, 3000));
    }
}