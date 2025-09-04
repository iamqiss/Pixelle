use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use prometheus::{Counter, Histogram, Gauge, IntCounter, IntGauge, HistogramOpts, Opts};
use crate::core::classifier::{ClassificationLabel, ClassificationResult};
use crate::core::signals::{Signal, PipelineResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enable_prometheus: bool,
    pub enable_jaeger: bool,
    pub enable_structured_logging: bool,
    pub metrics_export_interval: Duration,
    pub alerting_enabled: bool,
    pub alert_thresholds: AlertThresholds,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    pub high_error_rate: f64,
    pub high_latency_ms: u64,
    pub low_throughput: u64,
    pub high_memory_usage: f64,
    pub high_cpu_usage: f64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            high_error_rate: 0.05, // 5%
            high_latency_ms: 1000, // 1 second
            low_throughput: 100,    // 100 requests per second
            high_memory_usage: 0.8, // 80%
            high_cpu_usage: 0.8,    // 80%
        }
    }
}

pub struct MetricsCollector {
    // Prometheus metrics
    pub requests_total: Counter,
    pub requests_duration: Histogram,
    pub requests_errors: Counter,
    pub classification_total: Counter,
    pub classification_duration: Histogram,
    pub classification_confidence: Histogram,
    pub ml_inference_total: Counter,
    pub ml_inference_duration: Histogram,
    pub cache_hits: Counter,
    pub cache_misses: Counter,
    pub signal_total: Counter,
    pub signal_processed: Counter,
    pub pipeline_executions: Counter,
    pub pipeline_duration: Histogram,
    pub report_total: Counter,
    pub report_created: Counter,
    pub report_resolved: Counter,
    
    // Custom metrics
    pub active_connections: IntGauge,
    pub queue_size: IntGauge,
    pub memory_usage_bytes: Gauge,
    pub cpu_usage_percent: Gauge,
    
    // Internal state
    metrics: Arc<RwLock<HashMap<String, MetricValue>>>,
    alerts: Arc<RwLock<Vec<Alert>>>,
    config: MetricsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricValue {
    pub value: f64,
    pub timestamp: DateTime<Utc>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: Uuid,
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub message: String,
    pub metric_name: String,
    pub metric_value: f64,
    pub threshold: f64,
    pub timestamp: DateTime<Utc>,
    pub acknowledged: bool,
    pub acknowledged_by: Option<String>,
    pub acknowledged_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertType {
    HighErrorRate,
    HighLatency,
    LowThroughput,
    HighMemoryUsage,
    HighCpuUsage,
    ServiceUnhealthy,
    ModelDegradation,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl MetricsCollector {
    pub fn new(config: MetricsConfig) -> Result<Self> {
        let requests_total = Counter::new(
            "moderation_requests_total",
            "Total number of moderation requests"
        )?;
        
        let requests_duration = Histogram::new(
            HistogramOpts::new(
                "moderation_requests_duration_seconds",
                "Duration of moderation requests"
            )
        )?;
        
        let requests_errors = Counter::new(
            "moderation_requests_errors_total",
            "Total number of request errors"
        )?;
        
        let classification_total = Counter::new(
            "moderation_classification_total",
            "Total number of content classifications"
        )?;
        
        let classification_duration = Histogram::new(
            HistogramOpts::new(
                "moderation_classification_duration_seconds",
                "Duration of content classification"
            )
        )?;
        
        let classification_confidence = Histogram::new(
            HistogramOpts::new(
                "moderation_classification_confidence",
                "Confidence scores of classifications"
            )
        )?;
        
        let ml_inference_total = Counter::new(
            "moderation_ml_inference_total",
            "Total number of ML inference requests"
        )?;
        
        let ml_inference_duration = Histogram::new(
            HistogramOpts::new(
                "moderation_ml_inference_duration_seconds",
                "Duration of ML inference"
            )
        )?;
        
        let cache_hits = Counter::new(
            "moderation_cache_hits_total",
            "Total number of cache hits"
        )?;
        
        let cache_misses = Counter::new(
            "moderation_cache_misses_total",
            "Total number of cache misses"
        )?;
        
        let signal_total = Counter::new(
            "moderation_signal_total",
            "Total number of signals received"
        )?;
        
        let signal_processed = Counter::new(
            "moderation_signal_processed_total",
            "Total number of signals processed"
        )?;
        
        let pipeline_executions = Counter::new(
            "moderation_pipeline_executions_total",
            "Total number of pipeline executions"
        )?;
        
        let pipeline_duration = Histogram::new(
            HistogramOpts::new(
                "moderation_pipeline_duration_seconds",
                "Duration of pipeline executions"
            )
        )?;
        
        let report_total = Counter::new(
            "moderation_report_total",
            "Total number of user reports"
        )?;
        
        let report_created = Counter::new(
            "moderation_report_created_total",
            "Total number of reports created"
        )?;
        
        let report_resolved = Counter::new(
            "moderation_report_resolved_total",
            "Total number of reports resolved"
        )?;
        
        let active_connections = IntGauge::new(
            "moderation_active_connections",
            "Number of active connections"
        )?;
        
        let queue_size = IntGauge::new(
            "moderation_queue_size",
            "Current queue size"
        )?;
        
        let memory_usage_bytes = Gauge::new(
            "moderation_memory_usage_bytes",
            "Current memory usage in bytes"
        )?;
        
        let cpu_usage_percent = Gauge::new(
            "moderation_cpu_usage_percent",
            "Current CPU usage percentage"
        )?;

        Ok(Self {
            requests_total,
            requests_duration,
            requests_errors,
            classification_total,
            classification_duration,
            classification_confidence,
            ml_inference_total,
            ml_inference_duration,
            cache_hits,
            cache_misses,
            signal_total,
            signal_processed,
            pipeline_executions,
            pipeline_duration,
            report_total,
            report_created,
            report_resolved,
            active_connections,
            queue_size,
            memory_usage_bytes,
            cpu_usage_percent,
            metrics: Arc::new(RwLock::new(HashMap::new())),
            alerts: Arc::new(RwLock::new(Vec::new())),
            config,
        })
    }

    pub async fn start(&self) -> Result<()> {
        tracing::info!("Starting Metrics Collector");
        
        // Start metrics collection loop
        self.start_metrics_collection_loop();
        
        // Start alerting loop
        if self.config.alerting_enabled {
            self.start_alerting_loop();
        }
        
        tracing::info!("Metrics Collector started successfully");
        Ok(())
    }

    // Request metrics
    pub fn record_request(&self, duration: Duration, success: bool) {
        self.requests_total.inc();
        self.requests_duration.observe(duration.as_secs_f64());
        
        if !success {
            self.requests_errors.inc();
        }
    }

    // Classification metrics
    pub fn record_classification_time(&self, processing_time_ms: u64) {
        self.classification_total.inc();
        self.classification_duration.observe(processing_time_ms as f64 / 1000.0);
    }

    pub fn record_classification_result(&self, result: &ClassificationResult) {
        self.classification_confidence.observe(result.confidence as f64);
        
        // Record by label
        let label = format!("{:?}", result.label);
        self.record_custom_metric(
            "classification_by_label",
            result.confidence as f64,
            &[("label", &label)],
        );
    }

    // ML inference metrics
    pub fn record_ml_inference_time(&self, processing_time_ms: u64) {
        self.ml_inference_total.inc();
        self.ml_inference_duration.observe(processing_time_ms as f64 / 1000.0);
    }

    pub fn record_inference_result(&self, label: &ClassificationLabel, confidence: f32) {
        let label_str = format!("{:?}", label);
        self.record_custom_metric(
            "ml_inference_by_label",
            confidence as f64,
            &[("label", &label_str)],
        );
    }

    pub fn record_batch_inference_time(&self, processing_time_ms: u64, batch_size: usize) {
        self.record_custom_metric(
            "batch_inference_duration",
            processing_time_ms as f64 / 1000.0,
            &[("batch_size", &batch_size.to_string())],
        );
    }

    pub fn record_batch_inference_results(&self, results: &[(ClassificationLabel, f32)]) {
        for (label, confidence) in results {
            self.record_inference_result(label, *confidence);
        }
    }

    // Cache metrics
    pub fn record_cache_hit(&self) {
        self.cache_hits.inc();
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses.inc();
    }

    // Signal metrics
    pub fn record_signal_received(&self, signal: &Signal) {
        self.signal_total.inc();
        
        let signal_type = format!("{:?}", signal.signal_type);
        let severity = format!("{:?}", signal.severity);
        
        self.record_custom_metric(
            "signals_by_type",
            1.0,
            &[("type", &signal_type), ("severity", &severity)],
        );
    }

    pub fn record_signal_processed(&self, signal: &Signal) {
        self.signal_processed.inc();
    }

    // Pipeline metrics
    pub fn record_pipeline_execution(&self, result: &PipelineResult) {
        self.pipeline_executions.inc();
        self.pipeline_duration.observe(result.processing_time_ms as f64 / 1000.0);
        
        // Record pipeline-specific metrics
        self.record_custom_metric(
            "pipeline_duration_by_id",
            result.processing_time_ms as f64 / 1000.0,
            &[("pipeline_id", &result.pipeline_id)],
        );
    }

    // Report metrics
    pub fn record_report_created(&self, report: &crate::core::reports::UserReport) {
        self.report_total.inc();
        self.report_created.inc();
        
        let report_type = format!("{:?}", report.report_type);
        let priority = format!("{:?}", report.priority);
        
        self.record_custom_metric(
            "reports_by_type",
            1.0,
            &[("type", &report_type), ("priority", &priority)],
        );
    }

    pub fn record_report_resolved(&self, report: &crate::core::reports::UserReport) {
        self.report_resolved.inc();
        
        if let Some(resolved_at) = report.resolved_at {
            let resolution_time = resolved_at - report.created_at;
            self.record_custom_metric(
                "report_resolution_time_minutes",
                resolution_time.num_minutes() as f64,
                &[("type", &format!("{:?}", report.report_type))],
            );
        }
    }

    // System metrics
    pub fn record_active_connections(&self, count: i64) {
        self.active_connections.set(count);
    }

    pub fn record_queue_size(&self, size: i64) {
        self.queue_size.set(size);
    }

    pub fn record_memory_usage(&self, bytes: f64) {
        self.memory_usage_bytes.set(bytes);
    }

    pub fn record_cpu_usage(&self, percent: f64) {
        self.cpu_usage_percent.set(percent);
    }

    // Custom metrics
    pub fn record_custom_metric(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let mut labels_map = HashMap::new();
        for (key, value) in labels {
            labels_map.insert(key.to_string(), value.to_string());
        }
        
        let metric_value = MetricValue {
            value,
            timestamp: Utc::now(),
            labels: labels_map,
        };
        
        // Store in internal metrics
        tokio::spawn({
            let metrics = self.metrics.clone();
            let name = name.to_string();
            async move {
                metrics.write().await.insert(name, metric_value);
            }
        });
    }

    // Alerting
    pub async fn check_alerts(&self) -> Result<()> {
        let metrics = self.metrics.read().await;
        
        // Check error rate
        let error_rate = self.calculate_error_rate().await;
        if error_rate > self.config.alert_thresholds.high_error_rate {
            self.create_alert(
                AlertType::HighErrorRate,
                AlertSeverity::Warning,
                format!("High error rate: {:.2}%", error_rate * 100.0),
                "error_rate",
                error_rate,
                self.config.alert_thresholds.high_error_rate,
            ).await;
        }
        
        // Check latency
        let avg_latency = self.calculate_average_latency().await;
        if avg_latency > self.config.alert_thresholds.high_latency_ms as f64 {
            self.create_alert(
                AlertType::HighLatency,
                AlertSeverity::Warning,
                format!("High average latency: {:.2}ms", avg_latency),
                "avg_latency_ms",
                avg_latency,
                self.config.alert_thresholds.high_latency_ms as f64,
            ).await;
        }
        
        // Check throughput
        let throughput = self.calculate_throughput().await;
        if throughput < self.config.alert_thresholds.low_throughput as f64 {
            self.create_alert(
                AlertType::LowThroughput,
                AlertSeverity::Warning,
                format!("Low throughput: {:.2} req/s", throughput),
                "throughput_req_s",
                throughput,
                self.config.alert_thresholds.low_throughput as f64,
            ).await;
        }
        
        Ok(())
    }

    async fn create_alert(
        &self,
        alert_type: AlertType,
        severity: AlertSeverity,
        message: String,
        metric_name: &str,
        metric_value: f64,
        threshold: f64,
    ) {
        let alert = Alert {
            id: Uuid::new_v4(),
            alert_type,
            severity,
            message,
            metric_name: metric_name.to_string(),
            metric_value,
            threshold,
            timestamp: Utc::now(),
            acknowledged: false,
            acknowledged_by: None,
            acknowledged_at: None,
        };
        
        self.alerts.write().await.push(alert.clone());
        
        // Log alert
        match severity {
            AlertSeverity::Info => tracing::info!("ALERT: {}", message),
            AlertSeverity::Warning => tracing::warn!("ALERT: {}", message),
            AlertSeverity::Error => tracing::error!("ALERT: {}", message),
            AlertSeverity::Critical => tracing::error!("CRITICAL ALERT: {}", message),
        }
        
        // In production, you would send alerts to external systems
        // like PagerDuty, Slack, email, etc.
    }

    // Metric calculations
    async fn calculate_error_rate(&self) -> f64 {
        let errors = self.requests_errors.get();
        let total = self.requests_total.get();
        
        if total == 0.0 {
            0.0
        } else {
            errors / total
        }
    }

    async fn calculate_average_latency(&self) -> f64 {
        // This is a simplified calculation
        // In production, you'd use proper histogram quantiles
        let duration = self.requests_duration.get_sample_sum();
        let count = self.requests_duration.get_sample_count();
        
        if count == 0.0 {
            0.0
        } else {
            duration / count
        }
    }

    async fn calculate_throughput(&self) -> f64 {
        // Calculate requests per second over the last minute
        // This is a simplified implementation
        let total = self.requests_total.get();
        let now = Instant::now();
        
        // In production, you'd track this over time windows
        total / 60.0
    }

    // Getters
    pub async fn get_metrics(&self) -> HashMap<String, MetricValue> {
        self.metrics.read().await.clone()
    }

    pub async fn get_alerts(&self) -> Vec<Alert> {
        self.alerts.read().await.clone()
    }

    pub async fn acknowledge_alert(&self, alert_id: Uuid, acknowledged_by: String) -> Result<()> {
        let mut alerts = self.alerts.write().await;
        
        if let Some(alert) = alerts.iter_mut().find(|a| a.id == alert_id) {
            alert.acknowledged = true;
            alert.acknowledged_by = Some(acknowledged_by);
            alert.acknowledged_at = Some(Utc::now());
        }
        
        Ok(())
    }

    // Internal loops
    fn start_metrics_collection_loop(&self) {
        let metrics = self.metrics.clone();
        let interval = self.config.metrics_export_interval;
        
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                // Collect system metrics
                if let Ok(memory_info) = sysinfo::System::new_all() {
                    // Record memory usage
                    let memory_usage = memory_info.used_memory() as f64;
                    // Record CPU usage
                    let cpu_usage = memory_info.global_cpu_info().cpu_usage() as f64 / 100.0;
                    
                    // In production, you'd update the Prometheus gauges here
                    tracing::debug!("Memory: {} bytes, CPU: {:.2}%", memory_info.used_memory(), cpu_usage);
                }
            }
        });
    }

    fn start_alerting_loop(&self) {
        let metrics_collector = self.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Check every minute
            
            loop {
                interval.tick().await;
                
                if let Err(e) = metrics_collector.check_alerts().await {
                    tracing::error!("Error checking alerts: {}", e);
                }
            }
        });
    }
}

impl Clone for MetricsCollector {
    fn clone(&self) -> Self {
        Self {
            requests_total: self.requests_total.clone(),
            requests_duration: self.requests_duration.clone(),
            requests_errors: self.requests_errors.clone(),
            classification_total: self.classification_total.clone(),
            classification_duration: self.classification_duration.clone(),
            classification_confidence: self.classification_confidence.clone(),
            ml_inference_total: self.ml_inference_total.clone(),
            ml_inference_duration: self.ml_inference_duration.clone(),
            cache_hits: self.cache_hits.clone(),
            cache_misses: self.cache_misses.clone(),
            signal_total: self.signal_total.clone(),
            signal_processed: self.signal_processed.clone(),
            pipeline_executions: self.pipeline_executions.clone(),
            pipeline_duration: self.pipeline_duration.clone(),
            report_total: self.report_total.clone(),
            report_created: self.report_created.clone(),
            report_resolved: self.report_resolved.clone(),
            active_connections: self.active_connections.clone(),
            queue_size: self.queue_size.clone(),
            memory_usage_bytes: self.memory_usage_bytes.clone(),
            cpu_usage_percent: self.cpu_usage_percent.clone(),
            metrics: self.metrics.clone(),
            alerts: self.alerts.clone(),
            config: self.config.clone(),
        }
    }
}

// Health check endpoint
pub async fn health_check() -> serde_json::Value {
    serde_json::json!({
        "status": "healthy",
        "timestamp": Utc::now().to_rfc3339(),
        "version": env!("CARGO_PKG_VERSION"),
        "uptime": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    })
}

// Add missing dependency
use sysinfo;