// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Graph storage engine - relationship-optimized

use crate::storage::StorageEngine;
use crate::{Result, DocumentId, Document, LargetableError};
use async_trait::async_trait;
use petgraph::{Graph, Directed, NodeIndex, EdgeIndex};
use petgraph::graph::Node;
use rkyv::{to_bytes, from_bytes};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Graph storage engine using Petgraph
pub struct GraphEngine {
    graph: Arc<RwLock<Graph<Document, String, Directed>>>,
    node_map: Arc<RwLock<HashMap<DocumentId, NodeIndex>>>,
}

impl GraphEngine {
    /// Create a new Graph engine
    pub fn new() -> Result<Self> {
        info!("Graph Engine initialized with Petgraph backend");
        
        Ok(Self {
            graph: Arc::new(RwLock::new(Graph::new())),
            node_map: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Serialize document to bytes using zero-copy serialization
    fn serialize_document(&self, doc: &Document) -> Result<Vec<u8>> {
        to_bytes::<_, 1024>(doc)
            .map_err(|e| LargetableError::Serialization(format!("Failed to serialize document: {}", e)))
    }

    /// Deserialize bytes to document using zero-copy deserialization
    fn deserialize_document(&self, data: &[u8]) -> Result<Document> {
        from_bytes::<Document>(data)
            .map_err(|e| LargetableError::Serialization(format!("Failed to deserialize document: {}", e)))
    }

    /// Get node index for a document ID
    async fn get_node_index(&self, id: &DocumentId) -> Option<NodeIndex> {
        let node_map = self.node_map.read().await;
        node_map.get(id).copied()
    }

    /// Add node to graph and update mapping
    async fn add_node(&self, id: DocumentId, doc: Document) -> Result<NodeIndex> {
        let mut graph = self.graph.write().await;
        let mut node_map = self.node_map.write().await;
        
        let node_index = graph.add_node(doc);
        node_map.insert(id, node_index);
        
        Ok(node_index)
    }

    /// Remove node from graph and update mapping
    async fn remove_node(&self, id: &DocumentId) -> Result<bool> {
        let mut graph = self.graph.write().await;
        let mut node_map = self.node_map.write().await;
        
        if let Some(node_index) = node_map.remove(id) {
            graph.remove_node(node_index);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait]
impl StorageEngine for GraphEngine {
    async fn get(&self, id: &DocumentId) -> Result<Option<Document>> {
        let graph = self.graph.read().await;
        
        if let Some(node_index) = self.get_node_index(id).await {
            if let Some(node) = graph.node_weight(node_index) {
                debug!("Retrieved document with ID: {}", id);
                Ok(Some(node.clone()))
            } else {
                debug!("Document not found with ID: {}", id);
                Ok(None)
            }
        } else {
            debug!("Document not found with ID: {}", id);
            Ok(None)
        }
    }
    
    async fn put(&self, id: DocumentId, doc: Document) -> Result<()> {
        if let Some(_) = self.get_node_index(&id).await {
            // Update existing node
            let mut graph = self.graph.write().await;
            if let Some(node_index) = self.get_node_index(&id).await {
                if let Some(node) = graph.node_weight_mut(node_index) {
                    *node = doc;
                }
            }
        } else {
            // Add new node
            self.add_node(id.clone(), doc).await?;
        }
        
        debug!("Stored document with ID: {}", id);
        Ok(())
    }
    
    async fn delete(&self, id: &DocumentId) -> Result<bool> {
        let result = self.remove_node(id).await?;
        
        if result {
            debug!("Deleted document with ID: {}", id);
        }
        
        Ok(result)
    }
    
    async fn scan(&self, start: Option<DocumentId>, limit: usize) -> Result<Vec<(DocumentId, Document)>> {
        let graph = self.graph.read().await;
        let node_map = self.node_map.read().await;
        
        let mut results = Vec::new();
        let mut count = 0;
        let mut started = start.is_none();
        
        // Convert node map to sorted vector for consistent ordering
        let mut entries: Vec<_> = node_map.iter().collect();
        entries.sort_by_key(|(id, _)| *id);
        
        for (doc_id, node_index) in entries {
            if count >= limit {
                break;
            }
            
            if !started {
                if let Some(start_id) = start {
                    if doc_id == &start_id {
                        started = true;
                    } else {
                        continue;
                    }
                }
            }
            
            if let Some(node) = graph.node_weight(*node_index) {
                results.push((*doc_id, node.clone()));
                count += 1;
            }
        }
        
        debug!("Scanned {} documents", results.len());
        Ok(results)
    }
}

impl GraphEngine {
    /// Add an edge between two documents
    pub async fn add_edge(&self, from_id: DocumentId, to_id: DocumentId, relationship: String) -> Result<EdgeIndex> {
        let graph = self.graph.write().await;
        
        let from_node = self.get_node_index(&from_id).await
            .ok_or_else(|| LargetableError::Storage(format!("Source document {} not found", from_id)))?;
        
        let to_node = self.get_node_index(&to_id).await
            .ok_or_else(|| LargetableError::Storage(format!("Target document {} not found", to_id)))?;
        
        let edge_index = graph.add_edge(from_node, to_node, relationship);
        
        debug!("Added edge from {} to {} with relationship: {}", from_id, to_id, relationship);
        Ok(edge_index)
    }
    
    /// Remove an edge between two documents
    pub async fn remove_edge(&self, from_id: DocumentId, to_id: DocumentId) -> Result<bool> {
        let mut graph = self.graph.write().await;
        
        let from_node = self.get_node_index(&from_id).await
            .ok_or_else(|| LargetableError::Storage(format!("Source document {} not found", from_id)))?;
        
        let to_node = self.get_node_index(&to_id).await
            .ok_or_else(|| LargetableError::Storage(format!("Target document {} not found", to_id)))?;
        
        // Find and remove the edge
        if let Some(edge) = graph.find_edge(from_node, to_node) {
            graph.remove_edge(edge);
            debug!("Removed edge from {} to {}", from_id, to_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    /// Get all neighbors of a document
    pub async fn get_neighbors(&self, id: DocumentId) -> Result<Vec<(DocumentId, String)>> {
        let graph = self.graph.read().await;
        
        let node_index = self.get_node_index(&id).await
            .ok_or_else(|| LargetableError::Storage(format!("Document {} not found", id)))?;
        
        let mut neighbors = Vec::new();
        
        for edge in graph.edges(node_index) {
            if let Some(target_node) = graph.node_weight(edge.target()) {
                // Find the document ID for the target node
                let node_map = self.node_map.read().await;
                for (doc_id, node_idx) in node_map.iter() {
                    if *node_idx == edge.target() {
                        neighbors.push((*doc_id, edge.weight().clone()));
                        break;
                    }
                }
            }
        }
        
        debug!("Found {} neighbors for document {}", neighbors.len(), id);
        Ok(neighbors)
    }
    
    /// Perform graph traversal (BFS)
    pub async fn traverse(&self, start_id: DocumentId, max_depth: usize) -> Result<Vec<DocumentId>> {
        let graph = self.graph.read().await;
        let node_map = self.node_map.read().await;
        
        let start_node = node_map.get(&start_id)
            .ok_or_else(|| LargetableError::Storage(format!("Start document {} not found", start_id)))?;
        
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        let mut result = Vec::new();
        
        queue.push_back((*start_node, 0));
        visited.insert(*start_node);
        
        while let Some((node_index, depth)) = queue.pop_front() {
            if depth > max_depth {
                continue;
            }
            
            // Find document ID for this node
            for (doc_id, node_idx) in node_map.iter() {
                if *node_idx == node_index {
                    result.push(*doc_id);
                    break;
                }
            }
            
            if depth < max_depth {
                for edge in graph.edges(node_index) {
                    if !visited.contains(&edge.target()) {
                        visited.insert(edge.target());
                        queue.push_back((edge.target(), depth + 1));
                    }
                }
            }
        }
        
        debug!("Traversed {} documents from {} with max depth {}", result.len(), start_id, max_depth);
        Ok(result)
    }
}