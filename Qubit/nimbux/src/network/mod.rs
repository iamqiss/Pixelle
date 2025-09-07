// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Networking module - NO S3 COMPATIBILITY

// pub mod http;  // Commented out due to axum handler issues
pub mod simple_http;
pub mod tcp;
pub mod nimbux_api;  // Custom Nimbux API - NO S3 COMPATIBILITY
pub mod binary_protocol;  // Custom binary protocol for high-performance operations
pub mod connection_pool;

// Re-export commonly used types
pub use simple_http::SimpleHttpServer;
pub use tcp::{TcpServer, ProtocolHeader, OpCode, TcpRequest, TcpResponse};
pub use nimbux_api::{NimbuxApiServer, NimbuxApiState};
pub use binary_protocol::{BinaryCodec, BinaryMessage, BinaryRequest, BinaryResponse, OpCode, CompressionType, EncryptionType, Priority};
pub use connection_pool::{
    ConnectionPool, HttpConnectionPool, BufferPool, PerformanceMonitor,
    PoolStats, BufferPoolStats, PerformanceStats
};
