// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Observability and monitoring

pub mod tracing;
pub mod metrics;

pub use tracing::init_tracing;