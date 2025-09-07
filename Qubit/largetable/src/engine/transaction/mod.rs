// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! ACID transaction management

use crate::{Result, DocumentId, Document, LargetableError, DatabaseName, CollectionName};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};
use uuid::Uuid;

/// Transaction ID
pub type TransactionId = Uuid;

/// Transaction state
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Transaction operation
#[derive(Debug, Clone)]
pub enum TransactionOperation {
    Insert {
        database: DatabaseName,
        collection: CollectionName,
        document: Document,
    },
    Update {
        database: DatabaseName,
        collection: CollectionName,
        id: DocumentId,
        document: Document,
    },
    Delete {
        database: DatabaseName,
        collection: CollectionName,
        id: DocumentId,
    },
}

/// Transaction with ACID properties
pub struct Transaction {
    id: TransactionId,
    state: TransactionState,
    operations: Vec<TransactionOperation>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl Transaction {
    /// Create a new transaction
    pub fn new() -> Self {
        Self {
            id: Uuid::now_v7(),
            state: TransactionState::Active,
            operations: Vec::new(),
            created_at: chrono::Utc::now(),
        }
    }

    /// Get transaction ID
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// Get transaction state
    pub fn state(&self) -> &TransactionState {
        &self.state
    }

    /// Add an operation to the transaction
    pub fn add_operation(&mut self, operation: TransactionOperation) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(LargetableError::ConcurrencyViolation(
                "Cannot add operations to non-active transaction".to_string()
            ));
        }
        
        self.operations.push(operation);
        debug!("Added operation to transaction {}", self.id);
        Ok(())
    }

    /// Get all operations in the transaction
    pub fn operations(&self) -> &[TransactionOperation] {
        &self.operations
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Check if transaction is committed
    pub fn is_committed(&self) -> bool {
        self.state == TransactionState::Committed
    }

    /// Check if transaction is aborted
    pub fn is_aborted(&self) -> bool {
        self.state == TransactionState::Aborted
    }
}

/// Transaction manager
pub struct TransactionManager {
    active_transactions: Arc<RwLock<HashMap<TransactionId, Arc<RwLock<Transaction>>>>>,
    max_transaction_age: chrono::Duration,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            max_transaction_age: chrono::Duration::minutes(30),
        }
    }

    /// Start a new transaction
    pub async fn begin_transaction(&self) -> Result<TransactionId> {
        let transaction = Transaction::new();
        let id = transaction.id();
        
        let mut transactions = self.active_transactions.write().await;
        transactions.insert(id, Arc::new(RwLock::new(transaction)));
        
        info!("Started transaction {}", id);
        Ok(id)
    }

    /// Get a transaction by ID
    pub async fn get_transaction(&self, id: TransactionId) -> Result<Arc<RwLock<Transaction>>> {
        let transactions = self.active_transactions.read().await;
        transactions.get(&id)
            .cloned()
            .ok_or_else(|| LargetableError::ConcurrencyViolation(
                format!("Transaction {} not found", id)
            ))
    }

    /// Add an operation to a transaction
    pub async fn add_operation(&self, id: TransactionId, operation: TransactionOperation) -> Result<()> {
        let transaction = self.get_transaction(id).await?;
        let mut tx = transaction.write().await;
        tx.add_operation(operation)?;
        Ok(())
    }

    /// Commit a transaction
    pub async fn commit_transaction(&self, id: TransactionId) -> Result<()> {
        let transaction = self.get_transaction(id).await?;
        let mut tx = transaction.write().await;
        
        if tx.state != TransactionState::Active {
            return Err(LargetableError::ConcurrencyViolation(
                format!("Transaction {} is not active", id)
            ));
        }
        
        // Mark transaction as committed
        tx.state = TransactionState::Committed;
        
        // Remove from active transactions
        let mut transactions = self.active_transactions.write().await;
        transactions.remove(&id);
        
        info!("Committed transaction {}", id);
        Ok(())
    }

    /// Abort a transaction
    pub async fn abort_transaction(&self, id: TransactionId) -> Result<()> {
        let transaction = self.get_transaction(id).await?;
        let mut tx = transaction.write().await;
        
        if tx.state != TransactionState::Active {
            return Err(LargetableError::ConcurrencyViolation(
                format!("Transaction {} is not active", id)
            ));
        }
        
        // Mark transaction as aborted
        tx.state = TransactionState::Aborted;
        
        // Remove from active transactions
        let mut transactions = self.active_transactions.write().await;
        transactions.remove(&id);
        
        info!("Aborted transaction {}", id);
        Ok(())
    }

    /// Clean up expired transactions
    pub async fn cleanup_expired_transactions(&self) -> Result<usize> {
        let now = chrono::Utc::now();
        let mut transactions = self.active_transactions.write().await;
        let mut expired_ids = Vec::new();
        
        for (id, transaction) in transactions.iter() {
            let tx = transaction.read().await;
            if now.signed_duration_since(tx.created_at) > self.max_transaction_age {
                expired_ids.push(*id);
            }
        }
        
        let count = expired_ids.len();
        for id in expired_ids {
            if let Some(transaction) = transactions.remove(&id) {
                let mut tx = transaction.write().await;
                tx.state = TransactionState::Aborted;
                debug!("Cleaned up expired transaction {}", id);
            }
        }
        
        if count > 0 {
            info!("Cleaned up {} expired transactions", count);
        }
        
        Ok(count)
    }

    /// Get active transaction count
    pub async fn active_transaction_count(&self) -> usize {
        let transactions = self.active_transactions.read().await;
        transactions.len()
    }

    /// List all active transactions
    pub async fn list_active_transactions(&self) -> Vec<TransactionId> {
        let transactions = self.active_transactions.read().await;
        transactions.keys().cloned().collect()
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
