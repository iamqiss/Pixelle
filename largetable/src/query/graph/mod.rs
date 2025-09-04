// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Advanced graph algorithms and operations

pub mod traversal;
pub mod algorithms;
pub mod indexing;
pub mod analytics;

use crate::{Result, Document, DocumentId, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use petgraph::{Graph, Directed, Undirected, NodeIndex, EdgeIndex};
use petgraph::algo::{dijkstra, astar, connected_components, strongly_connected_components};
use petgraph::visit::{Dfs, Bfs, EdgeRef};

/// Graph database engine for relationship queries
pub struct GraphEngine {
    graph: Arc<RwLock<Graph<GraphNode, GraphEdge, Directed>>>,
    node_map: Arc<RwLock<HashMap<DocumentId, NodeIndex>>>,
    edge_map: Arc<RwLock<HashMap<(DocumentId, DocumentId), EdgeIndex>>>,
}

/// Graph node representing a document
#[derive(Debug, Clone)]
pub struct GraphNode {
    pub id: DocumentId,
    pub document: Document,
    pub properties: HashMap<String, Value>,
}

/// Graph edge representing a relationship
#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub from: DocumentId,
    pub to: DocumentId,
    pub relationship_type: String,
    pub weight: f32,
    pub properties: HashMap<String, Value>,
}

/// Graph query result
#[derive(Debug)]
pub struct GraphQueryResult {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub paths: Vec<GraphPath>,
    pub statistics: GraphStatistics,
}

/// Graph path between nodes
#[derive(Debug, Clone)]
pub struct GraphPath {
    pub nodes: Vec<DocumentId>,
    pub edges: Vec<(DocumentId, DocumentId)>,
    pub total_weight: f32,
    pub length: usize,
}

/// Graph statistics
#[derive(Debug)]
pub struct GraphStatistics {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub connected_components: usize,
    pub average_degree: f32,
    pub density: f32,
}

impl GraphEngine {
    /// Create a new graph engine
    pub fn new() -> Self {
        Self {
            graph: Arc::new(RwLock::new(Graph::new())),
            node_map: Arc::new(HashMap::new()),
            edge_map: Arc::new(HashMap::new()),
        }
    }

    /// Add a node to the graph
    pub async fn add_node(&self, document: Document, properties: HashMap<String, Value>) -> Result<NodeIndex> {
        let mut graph = self.graph.write().await;
        let mut node_map = self.node_map.write().await;

        let node = GraphNode {
            id: document.id,
            document,
            properties,
        };

        let node_index = graph.add_node(node.clone());
        node_map.insert(document.id, node_index);

        Ok(node_index)
    }

    /// Add an edge between two nodes
    pub async fn add_edge(
        &self,
        from: DocumentId,
        to: DocumentId,
        relationship_type: String,
        weight: f32,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeIndex> {
        let mut graph = self.graph.write().await;
        let mut node_map = self.node_map.read().await;
        let mut edge_map = self.edge_map.write().await;

        let from_index = node_map.get(&from)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Source node not found".to_string()))?;
        let to_index = node_map.get(&to)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Target node not found".to_string()))?;

        let edge = GraphEdge {
            from,
            to,
            relationship_type,
            weight,
            properties,
        };

        let edge_index = graph.add_edge(*from_index, *to_index, edge.clone());
        edge_map.insert((from, to), edge_index);

        Ok(edge_index)
    }

    /// Find shortest path between two nodes
    pub async fn shortest_path(&self, from: DocumentId, to: DocumentId) -> Result<Option<GraphPath>> {
        let graph = self.graph.read().await;
        let node_map = self.node_map.read().await;

        let from_index = node_map.get(&from)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Source node not found".to_string()))?;
        let to_index = node_map.get(&to)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Target node not found".to_string()))?;

        // Use Dijkstra's algorithm
        let path = dijkstra(&*graph, *from_index, Some(*to_index), |e| e.weight());

        if let Some((_, total_weight)) = path.get(to_index) {
            // Reconstruct path
            let mut current = *to_index;
            let mut path_nodes = vec![current];
            let mut path_edges = Vec::new();

            while current != *from_index {
                if let Some(edge) = graph.edges_directed(current, petgraph::Direction::Incoming)
                    .min_by(|a, b| a.weight().partial_cmp(&b.weight()).unwrap()) {
                    current = edge.source();
                    path_nodes.push(current);
                    path_edges.push((graph[edge.source()].id, graph[edge.target()].id));
                } else {
                    return Ok(None);
                }
            }

            path_nodes.reverse();
            path_edges.reverse();

            Ok(Some(GraphPath {
                nodes: path_nodes.into_iter().map(|i| graph[i].id).collect(),
                edges: path_edges,
                total_weight: *total_weight,
                length: path_nodes.len() - 1,
            }))
        } else {
            Ok(None)
        }
    }

    /// Find all paths between two nodes within a maximum depth
    pub async fn find_paths(&self, from: DocumentId, to: DocumentId, max_depth: usize) -> Result<Vec<GraphPath>> {
        let graph = self.graph.read().await;
        let node_map = self.node_map.read().await;

        let from_index = node_map.get(&from)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Source node not found".to_string()))?;
        let to_index = node_map.get(&to)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Target node not found".to_string()))?;

        let mut paths = Vec::new();
        let mut stack = VecDeque::new();
        stack.push_back((*from_index, vec![*from_index], 0.0));

        while let Some((current, path, weight)) = stack.pop_front() {
            if current == *to_index {
                let path_nodes: Vec<DocumentId> = path.iter().map(|&i| graph[i].id).collect();
                let path_edges: Vec<(DocumentId, DocumentId)> = path.windows(2)
                    .map(|w| (graph[w[0]].id, graph[w[1]].id))
                    .collect();

                paths.push(GraphPath {
                    nodes: path_nodes,
                    edges: path_edges,
                    total_weight: weight,
                    length: path.len() - 1,
                });
                continue;
            }

            if path.len() >= max_depth {
                continue;
            }

            for edge in graph.edges(current) {
                let next = edge.target();
                if !path.contains(&next) {
                    let mut new_path = path.clone();
                    new_path.push(next);
                    let new_weight = weight + edge.weight();
                    stack.push_back((next, new_path, new_weight));
                }
            }
        }

        Ok(paths)
    }

    /// Find neighbors of a node
    pub async fn find_neighbors(&self, node_id: DocumentId, max_depth: usize) -> Result<Vec<GraphNode>> {
        let graph = self.graph.read().await;
        let node_map = self.node_map.read().await;

        let node_index = node_map.get(&node_id)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Node not found".to_string()))?;

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back((*node_index, 0));
        visited.insert(*node_index);

        let mut neighbors = Vec::new();

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for edge in graph.edges(current) {
                let neighbor = edge.target();
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    neighbors.push(graph[neighbor].clone());
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        Ok(neighbors)
    }

    /// Find strongly connected components
    pub async fn find_strongly_connected_components(&self) -> Result<Vec<Vec<DocumentId>>> {
        let graph = self.graph.read().await;
        let components = strongly_connected_components(&*graph);

        let mut result = Vec::new();
        for component in components {
            let nodes: Vec<DocumentId> = component.into_iter()
                .map(|i| graph[i].id)
                .collect();
            result.push(nodes);
        }

        Ok(result)
    }

    /// Find connected components
    pub async fn find_connected_components(&self) -> Result<Vec<Vec<DocumentId>>> {
        let graph = self.graph.read().await;
        let components = connected_components(&*graph);

        let mut result = Vec::new();
        for component in components {
            let nodes: Vec<DocumentId> = component.into_iter()
                .map(|i| graph[i].id)
                .collect();
            result.push(nodes);
        }

        Ok(result)
    }

    /// Calculate graph statistics
    pub async fn get_statistics(&self) -> Result<GraphStatistics> {
        let graph = self.graph.read().await;
        
        let total_nodes = graph.node_count();
        let total_edges = graph.edge_count();
        
        let mut total_degree = 0;
        for node in graph.node_indices() {
            total_degree += graph.edges(node).count();
        }
        
        let average_degree = if total_nodes > 0 {
            total_degree as f32 / total_nodes as f32
        } else {
            0.0
        };

        let density = if total_nodes > 1 {
            total_edges as f32 / (total_nodes as f32 * (total_nodes as f32 - 1.0))
        } else {
            0.0
        };

        let connected_components = connected_components(&*graph).len();

        Ok(GraphStatistics {
            total_nodes,
            total_edges,
            connected_components,
            average_degree,
            density,
        })
    }

    /// Query nodes by properties
    pub async fn query_nodes(&self, properties: HashMap<String, Value>) -> Result<Vec<GraphNode>> {
        let graph = self.graph.read().await;
        let mut results = Vec::new();

        for node in graph.node_indices() {
            let graph_node = &graph[node];
            let mut matches = true;

            for (key, value) in &properties {
                if let Some(node_value) = graph_node.properties.get(key) {
                    if node_value != value {
                        matches = false;
                        break;
                    }
                } else {
                    matches = false;
                    break;
                }
            }

            if matches {
                results.push(graph_node.clone());
            }
        }

        Ok(results)
    }

    /// Query edges by relationship type
    pub async fn query_edges(&self, relationship_type: &str) -> Result<Vec<GraphEdge>> {
        let graph = self.graph.read().await;
        let mut results = Vec::new();

        for edge in graph.edge_indices() {
            let graph_edge = &graph[edge];
            if graph_edge.relationship_type == relationship_type {
                results.push(graph_edge.clone());
            }
        }

        Ok(results)
    }
}

/// Graph traversal algorithms
pub struct GraphTraversal {
    engine: Arc<GraphEngine>,
}

impl GraphTraversal {
    pub fn new(engine: Arc<GraphEngine>) -> Self {
        Self { engine }
    }

    /// Breadth-first search from a node
    pub async fn bfs(&self, start: DocumentId, max_depth: Option<usize>) -> Result<Vec<GraphNode>> {
        let graph = self.engine.graph.read().await;
        let node_map = self.engine.node_map.read().await;

        let start_index = node_map.get(&start)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Start node not found".to_string()))?;

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        queue.push_back((*start_index, 0));
        visited.insert(*start_index);

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(max_d) = max_depth {
                if depth > max_d {
                    continue;
                }
            }

            result.push(graph[current].clone());

            for edge in graph.edges(current) {
                let neighbor = edge.target();
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        Ok(result)
    }

    /// Depth-first search from a node
    pub async fn dfs(&self, start: DocumentId, max_depth: Option<usize>) -> Result<Vec<GraphNode>> {
        let graph = self.engine.graph.read().await;
        let node_map = self.engine.node_map.read().await;

        let start_index = node_map.get(&start)
            .ok_or_else(|| crate::LargetableError::InvalidInput("Start node not found".to_string()))?;

        let mut visited = HashSet::new();
        let mut stack = Vec::new();
        let mut result = Vec::new();

        stack.push((*start_index, 0));
        visited.insert(*start_index);

        while let Some((current, depth)) = stack.pop() {
            if let Some(max_d) = max_depth {
                if depth > max_d {
                    continue;
                }
            }

            result.push(graph[current].clone());

            for edge in graph.edges(current) {
                let neighbor = edge.target();
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    stack.push((neighbor, depth + 1));
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_graph_engine() {
        let engine = GraphEngine::new();
        
        let mut doc1 = Document::new();
        doc1.fields.insert("name".to_string(), Value::String("Alice".to_string()));
        
        let mut doc2 = Document::new();
        doc2.fields.insert("name".to_string(), Value::String("Bob".to_string()));

        let node1 = engine.add_node(doc1, HashMap::new()).await.unwrap();
        let node2 = engine.add_node(doc2, HashMap::new()).await.unwrap();

        engine.add_edge(
            doc1.id,
            doc2.id,
            "knows".to_string(),
            1.0,
            HashMap::new(),
        ).await.unwrap();

        let stats = engine.get_statistics().await.unwrap();
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.total_edges, 1);
    }
}