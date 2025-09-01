use prometheus::{Counter, Histogram, IntCounter, IntGauge, Opts, Registry};
use std::sync::Arc;

#[derive(Clone)]
pub struct AnalyticsMetrics {
    registry: Arc<Registry>,
    user_events_total: IntCounter,
    post_events_total: IntCounter,
    engagement_events_total: IntCounter,
}

impl AnalyticsMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        
        let user_events_total = IntCounter::new(
            "user_events_total",
            "Total number of user events",
        ).unwrap();
        
        let post_events_total = IntCounter::new(
            "post_events_total",
            "Total number of post events",
        ).unwrap();
        
        let engagement_events_total = IntCounter::new(
            "engagement_events_total",
            "Total number of engagement events",
        ).unwrap();

        registry.register(Box::new(user_events_total.clone())).unwrap();
        registry.register(Box::new(post_events_total.clone())).unwrap();
        registry.register(Box::new(engagement_events_total.clone())).unwrap();

        Self {
            registry: Arc::new(registry),
            user_events_total,
            post_events_total,
            engagement_events_total,
        }
    }

    pub fn increment_user_events(&self) {
        self.user_events_total.inc();
    }

    pub fn increment_post_events(&self) {
        self.post_events_total.inc();
    }

    pub fn increment_engagement_events(&self) {
        self.engagement_events_total.inc();
    }

    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }
}
