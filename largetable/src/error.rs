// ===========================================
// Largetable - Distributed NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LargetableError {
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Query error: {0}")]
    Query(String),
    #[error("Network error: {0}")]
    Network(String),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, LargetableError>;
