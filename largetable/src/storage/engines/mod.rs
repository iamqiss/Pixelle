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

pub fn create_storage_engine(engine_type: crate::StorageEngine) -> Box<dyn StorageEngine> {
    match engine_type {
        crate::StorageEngine::Lsm => Box::new(lsm::LsmEngine::new()),
        crate::StorageEngine::BTree => Box::new(btree::BTreeEngine::new()),
        crate::StorageEngine::Columnar => Box::new(columnar::ColumnarEngine::new()),
        crate::StorageEngine::Graph => Box::new(graph::GraphEngine::new()),
    }
}
