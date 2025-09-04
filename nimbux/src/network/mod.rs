// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Networking module

// pub mod http;  // Commented out due to axum handler issues
pub mod simple_http;
pub mod tcp;
pub mod s3_api;
pub mod connection_pool;

// Re-export commonly used types
pub use simple_http::SimpleHttpServer;
pub use tcp::{TcpServer, ProtocolHeader, OpCode, TcpRequest, TcpResponse};
pub use s3_api::{S3ApiServer, S3ApiState};
pub use connection_pool::{
    ConnectionPool, HttpConnectionPool, BufferPool, PerformanceMonitor,
    PoolStats, BufferPoolStats, PerformanceStats
};
