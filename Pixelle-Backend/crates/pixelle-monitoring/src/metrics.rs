use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    http_requests_total: IntCounter,
    http_request_duration: Histogram,
    active_connections: IntGauge,
    database_queries_total: IntCounter,
    database_query_duration: Histogram,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        
        let http_requests_total = IntCounter::new(
            "http_requests_total",
            "Total number of HTTP requests",
        ).unwrap();
        
        let http_request_duration = Histogram::with_opts(
            HistogramOpts::new(
                "http_request_duration_seconds",
                "HTTP request duration in seconds",
            )
        ).unwrap();
        
        let active_connections = IntGauge::new(
            "active_connections",
            "Number of active connections",
        ).unwrap();
        
        let database_queries_total = IntCounter::new(
            "database_queries_total",
            "Total number of database queries",
        ).unwrap();
        
        let database_query_duration = Histogram::with_opts(
            HistogramOpts::new(
                "database_query_duration_seconds",
                "Database query duration in seconds",
            )
        ).unwrap();

        registry.register(Box::new(http_requests_total.clone())).unwrap();
        registry.register(Box::new(http_request_duration.clone())).unwrap();
        registry.register(Box::new(active_connections.clone())).unwrap();
        registry.register(Box::new(database_queries_total.clone())).unwrap();
        registry.register(Box::new(database_query_duration.clone())).unwrap();

        Self {
            registry: Arc::new(registry),
            http_requests_total,
            http_request_duration,
            active_connections,
            database_queries_total,
            database_query_duration,
        }
    }

    pub fn increment_http_requests(&self) {
        self.http_requests_total.inc();
    }

    pub fn observe_http_duration(&self, duration: f64) {
        self.http_request_duration.observe(duration);
    }

    pub fn set_active_connections(&self, count: i64) {
        self.active_connections.set(count);
    }

    pub fn increment_database_queries(&self) {
        self.database_queries_total.inc();
    }

    pub fn observe_database_duration(&self, duration: f64) {
        self.database_query_duration.observe(duration);
    }

    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }
}
