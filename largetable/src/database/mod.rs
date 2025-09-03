// ===========================================
// Largetable - Distributed NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
//! Database management

pub mod catalog;
pub mod namespace;
pub mod admin;

use crate::{Result, Collection, CollectionName, DatabaseName};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Database {
    pub name: DatabaseName,
    collections: Arc<RwLock<HashMap<CollectionName, Arc<Collection>>>>,
}

impl Database {
    pub fn new(name: DatabaseName) -> Self {
        Self {
            name,
            collections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn create_collection(&self, name: CollectionName) -> Result<Arc<Collection>> {
        let collection = Arc::new(Collection::new(name.clone()));
        self.collections.write().unwrap().insert(name, collection.clone());
        Ok(collection)
    }
    
    pub fn get_collection(&self, name: &CollectionName) -> Option<Arc<Collection>> {
        self.collections.read().unwrap().get(name).cloned()
    }
    
    pub fn list_collections(&self) -> Vec<CollectionName> {
        self.collections.read().unwrap().keys().cloned().collect()
    }
}
