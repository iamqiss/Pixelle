// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Network layer for client-server communication

pub mod async_server;
pub mod connection_pool;
pub mod protocol;
pub mod wire_protocol;
pub mod load_balancer;
pub mod circuit_breaker;

use crate::Result;
