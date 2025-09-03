// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Largetable - Next-Generation NoSQL Database
//! 
//! Outperforming MongoDB with:
//! - Async-first architecture
//! - Zero-copy serialization
//! - Pluggable storage engines
//! - Multi-model support
//! - Built-in observability

#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

// === CORE MODULES ===
pub mod engine;
pub mod storage;
pub mod index;
pub mod query;
pub mod document;
pub mod collection;
pub mod database;

// === DISTRIBUTED SYSTEMS ===
pub mod replication;
pub mod sharding;

// === NETWORK & CLIENT ===
pub mod network;
pub mod drivers;

// === MULTI-MODEL SUPPORT ===
pub mod models;

// === SECURITY & AUTH ===
pub mod auth;

// === CONFIGURATION ===
pub mod config;

// === OBSERVABILITY ===
pub mod observability;

// === UTILITIES & FFI ===
pub mod utils;
pub mod ffi;
pub mod tools;

// === ERROR HANDLING ===
pub mod error;
pub mod types;

// === PUBLIC API ===
pub use error::{LargetableError, Result};
pub use types::*;
pub use drivers::native::Client;
pub use document::Document;
pub use query::Query;

// === FFI EXPORTS FOR C BINDINGS ===
#[no_mangle]
pub extern "C" fn largetable_version() -> *const std::os::raw::c_char {
    std::ffi::CString::new(env!("CARGO_PKG_VERSION")).unwrap().into_raw()
}
