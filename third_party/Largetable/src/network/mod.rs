// ===========================================
// Largetable - Distributed NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
//! Network layer for client-server communication

pub mod protocol;
pub mod server;
pub mod client;
pub mod connection;
pub mod wire_protocol;

use crate::Result;
