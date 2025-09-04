// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Consensus management for distributed coordination

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::{NimbuxError, Result};
use super::node::{Node, NodeStatus, NodeRole};

/// Consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    pub election_timeout: u64, // milliseconds
    pub heartbeat_interval: u64, // milliseconds
    pub max_log_entries: usize,
    pub snapshot_interval: usize,
}

/// Consensus state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConsensusState {
    Follower,
    Candidate,
    Leader,
    Observer,
}

/// Consensus manager for distributed coordination
pub struct ConsensusManager {
    config: ConsensusConfig,
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    current_state: Arc<RwLock<ConsensusState>>,
    current_leader: Arc<RwLock<Option<String>>>,
    term: Arc<RwLock<u64>>,
    voted_for: Arc<RwLock<Option<String>>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
    commit_index: Arc<RwLock<usize>>,
    last_applied: Arc<RwLock<usize>>,
    heartbeat_timer: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    election_timer: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

/// Log entry for consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: usize,
    pub command: ConsensusCommand,
    pub timestamp: u64,
}

/// Consensus command types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusCommand {
    AddNode { node_id: String, node_info: Node },
    RemoveNode { node_id: String },
    UpdateNode { node_id: String, node_info: Node },
    ReplicateObject { object_id: String, data: Vec<u8> },
    DeleteObject { object_id: String },
    UpdateConfig { config: ConsensusConfig },
}

/// Vote request for leader election
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

/// Vote response for leader election
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub reason: Option<String>,
}

/// Append entries request for log replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: usize,
}

/// Append entries response for log replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub next_index: usize,
    pub reason: Option<String>,
}

impl ConsensusManager {
    pub fn new(config: ConsensusConfig, nodes: Arc<RwLock<HashMap<String, Node>>>) -> Result<Self> {
        Ok(Self {
            config,
            nodes,
            current_state: Arc::new(RwLock::new(ConsensusState::Follower)),
            current_leader: Arc::new(RwLock::new(None)),
            term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(Vec::new())),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            heartbeat_timer: Arc::new(RwLock::new(None)),
            election_timer: Arc::new(RwLock::new(None)),
        })
    }
    
    /// Start the consensus manager
    pub async fn start(&self) -> Result<()> {
        // Start as follower
        self.start_election_timer().await?;
        Ok(())
    }
    
    /// Start election timer
    async fn start_election_timer(&self) -> Result<()> {
        let config = self.config.clone();
        let state = Arc::clone(&self.current_state);
        let term = Arc::clone(&self.term);
        let voted_for = Arc::clone(&self.voted_for);
        let nodes = Arc::clone(&self.nodes);
        let consensus_manager = self.clone();
        
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(config.election_timeout));
            
            loop {
                interval.tick().await;
                
                let current_state = *state.read().await;
                if current_state == ConsensusState::Follower {
                    // Start election
                    if let Err(e) = consensus_manager.start_election().await {
                        tracing::error!("Election failed: {}", e);
                    }
                }
            }
        });
        
        let mut timer = self.election_timer.write().await;
        *timer = Some(handle);
        
        Ok(())
    }
    
    /// Start election process
    async fn start_election(&self) -> Result<()> {
        // Increment term
        let mut term = self.term.write().await;
        *term += 1;
        let current_term = *term;
        drop(term);
        
        // Change to candidate state
        {
            let mut state = self.current_state.write().await;
            *state = ConsensusState::Candidate;
        }
        
        // Vote for self
        {
            let mut voted_for = self.voted_for.write().await;
            *voted_for = Some("self".to_string());
        }
        
        // Get nodes for voting
        let nodes = self.nodes.read().await;
        let mut votes_received = 1; // Self vote
        let mut total_votes = 1;
        
        // Request votes from other nodes
        for (node_id, node) in nodes.iter() {
            if node.status == NodeStatus::Healthy {
                total_votes += 1;
                
                let vote_request = VoteRequest {
                    term: current_term,
                    candidate_id: "self".to_string(),
                    last_log_index: self.get_last_log_index().await,
                    last_log_term: self.get_last_log_term().await,
                };
                
                match self.request_vote(node_id, &vote_request).await {
                    Ok(response) => {
                        if response.vote_granted {
                            votes_received += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to request vote from node {}: {}", node_id, e);
                    }
                }
            }
        }
        
        // Check if we won the election
        if votes_received > total_votes / 2 {
            self.become_leader().await?;
        } else {
            // Return to follower state
            let mut state = self.current_state.write().await;
            *state = ConsensusState::Follower;
        }
        
        Ok(())
    }
    
    /// Become leader
    async fn become_leader(&self) -> Result<()> {
        // Change to leader state
        {
            let mut state = self.current_state.write().await;
            *state = ConsensusState::Leader;
        }
        
        // Set self as leader
        {
            let mut leader = self.current_leader.write().await;
            *leader = Some("self".to_string());
        }
        
        // Start heartbeat
        self.start_heartbeat().await?;
        
        tracing::info!("Became leader for term {}", *self.term.read().await);
        Ok(())
    }
    
    /// Start heartbeat
    async fn start_heartbeat(&self) -> Result<()> {
        let config = self.config.clone();
        let nodes = Arc::clone(&self.nodes);
        let consensus_manager = self.clone();
        
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(config.heartbeat_interval));
            
            loop {
                interval.tick().await;
                
                // Send heartbeat to all nodes
                if let Err(e) = consensus_manager.send_heartbeat().await {
                    tracing::error!("Heartbeat failed: {}", e);
                }
            }
        });
        
        let mut timer = self.heartbeat_timer.write().await;
        *timer = Some(handle);
        
        Ok(())
    }
    
    /// Send heartbeat to all nodes
    async fn send_heartbeat(&self) -> Result<()> {
        let nodes = self.nodes.read().await;
        let term = *self.term.read().await;
        
        for (node_id, node) in nodes.iter() {
            if node.status == NodeStatus::Healthy && node_id != "self" {
                let request = AppendEntriesRequest {
                    term,
                    leader_id: "self".to_string(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(), // Empty for heartbeat
                    leader_commit: *self.commit_index.read().await,
                };
                
                if let Err(e) = self.append_entries(node_id, &request).await {
                    tracing::warn!("Failed to send heartbeat to node {}: {}", node_id, e);
                }
            }
        }
        
        Ok(())
    }
    
    /// Request vote from a node
    async fn request_vote(&self, node_id: &str, request: &VoteRequest) -> Result<VoteResponse> {
        // In a real implementation, this would send an RPC to the node
        // For now, we simulate the response
        
        // Simulate network delay
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        // Simulate vote decision
        let vote_granted = rand::random::<f64>() > 0.3; // 70% chance of granting vote
        
        Ok(VoteResponse {
            term: request.term,
            vote_granted,
            reason: if vote_granted { None } else { Some("Node busy".to_string()) },
        })
    }
    
    /// Append entries to a node
    async fn append_entries(&self, node_id: &str, request: &AppendEntriesRequest) -> Result<AppendEntriesResponse> {
        // In a real implementation, this would send an RPC to the node
        // For now, we simulate the response
        
        // Simulate network delay
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        
        // Simulate success
        let success = rand::random::<f64>() > 0.1; // 90% success rate
        
        Ok(AppendEntriesResponse {
            term: request.term,
            success,
            next_index: request.prev_log_index + request.entries.len(),
            reason: if success { None } else { Some("Log inconsistency".to_string()) },
        })
    }
    
    /// Get last log index
    async fn get_last_log_index(&self) -> usize {
        let log = self.log.read().await;
        log.len()
    }
    
    /// Get last log term
    async fn get_last_log_term(&self) -> u64 {
        let log = self.log.read().await;
        log.last().map(|entry| entry.term).unwrap_or(0)
    }
    
    /// Add log entry
    pub async fn add_log_entry(&self, command: ConsensusCommand) -> Result<()> {
        let mut log = self.log.write().await;
        let term = *self.term.read().await;
        let index = log.len() + 1;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let entry = LogEntry {
            term,
            index,
            command,
            timestamp,
        };
        
        log.push(entry);
        Ok(())
    }
    
    /// Get current state
    pub async fn get_state(&self) -> ConsensusState {
        *self.current_state.read().await
    }
    
    /// Get current leader
    pub async fn get_leader(&self) -> Option<String> {
        self.current_leader.read().await.clone()
    }
    
    /// Get current term
    pub async fn get_term(&self) -> u64 {
        *self.term.read().await
    }
    
    /// Get consensus statistics
    pub async fn get_stats(&self) -> Result<ConsensusStats> {
        let log = self.log.read().await;
        let state = *self.current_state.read().await;
        let leader = self.current_leader.read().await.clone();
        let term = *self.term.read().await;
        
        Ok(ConsensusStats {
            current_state: state,
            current_leader: leader,
            current_term: term,
            log_entries: log.len(),
            commit_index: *self.commit_index.read().await,
            last_applied: *self.last_applied.read().await,
        })
    }
}

impl Clone for ConsensusManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            nodes: Arc::clone(&self.nodes),
            current_state: Arc::clone(&self.current_state),
            current_leader: Arc::clone(&self.current_leader),
            term: Arc::clone(&self.term),
            voted_for: Arc::clone(&self.voted_for),
            log: Arc::clone(&self.log),
            commit_index: Arc::clone(&self.commit_index),
            last_applied: Arc::clone(&self.last_applied),
            heartbeat_timer: Arc::clone(&self.heartbeat_timer),
            election_timer: Arc::clone(&self.election_timer),
        }
    }
}

/// Consensus statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusStats {
    pub current_state: ConsensusState,
    pub current_leader: Option<String>,
    pub current_term: u64,
    pub log_entries: usize,
    pub commit_index: usize,
    pub last_applied: usize,
}