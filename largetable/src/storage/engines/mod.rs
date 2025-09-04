// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Storage engine implementations

pub mod lsm;
pub mod btree;
pub mod columnar;
pub mod graph;

use crate::storage::StorageEngine;
use crate::Result;

pub fn create_storage_engine(engine_type: crate::StorageEngine) -> Result<Box<dyn StorageEngine>> {
    match engine_type {
        crate::StorageEngine::Lsm => Ok(Box::new(lsm::LsmEngine::new()?)),
        crate::StorageEngine::BTree => Ok(Box::new(btree::BTreeEngine::new()?)),
        crate::StorageEngine::Columnar => Ok(Box::new(columnar::ColumnarEngine::new()?)),
        crate::StorageEngine::Graph => Ok(Box::new(graph::GraphEngine::new()?)),
    }
}
