// ===========================================
// Largetable - Distributed NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
//! Largetable - Distributed NoSQL Database
//! 
//! A high-performance, MongoDB-compatible document database built in Rust.

pub mod engine;
pub mod storage;
pub mod index;
pub mod query;
pub mod document;
pub mod collection;
pub mod database;
pub mod replication;
pub mod sharding;
pub mod network;
pub mod auth;
pub mod config;
pub mod monitoring;
pub mod utils;
pub mod drivers;
pub mod tools;

pub mod error;
pub mod types;

pub use error::{LargetableError, Result};
pub use types::*;
