// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// High durability and availability features

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH, Instant};

use crate::errors::{NimbuxError, Result};

pub mod replication;
pub mod checksum;
pub mod backup;
pub mod recovery;
pub mod health_check;
pub mod failover;

// Re-export commonly used types
pub use replication::{ReplicationManager, ReplicationConfig, ReplicationStats, ReplicaInfo};
pub use checksum::{ChecksumManager, ChecksumConfig, ChecksumStats, ChecksumResult};
pub use backup::{BackupManager, BackupConfig, BackupStats, BackupInfo};
pub use recovery::{RecoveryManager, RecoveryConfig, RecoveryStats, RecoveryPlan};
pub use health_check::{HealthChecker, HealthConfig, HealthStats, HealthStatus};
pub use failover::{FailoverManager, FailoverConfig, FailoverStats, FailoverEvent};

/// Durability configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityConfig {
    pub replication_factor: usize,
    pub checksum_algorithm: ChecksumAlgorithm,
    pub enable_backup: bool,
    pub backup_interval: u64, // seconds
    pub backup_retention: u64, // days
    pub enable_recovery: bool,
    pub recovery_timeout: u64, // seconds
    pub enable_health_check: bool,
    pub health_check_interval: u64, // seconds
    pub enable_failover: bool,
    pub failover_timeout: u64, // seconds
    pub min_healthy_replicas: usize,
}

/// Checksum algorithm
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChecksumAlgorithm {
    Blake3,
    SHA256,
    SHA512,
    CRC32,
    MD5,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            replication_factor: 3,
            checksum_algorithm: ChecksumAlgorithm::Blake3,
            enable_backup: true,
            backup_interval: 3600, // 1 hour
            backup_retention: 30, // 30 days
            enable_recovery: true,
            recovery_timeout: 300, // 5 minutes
            enable_health_check: true,
            health_check_interval: 30, // 30 seconds
            enable_failover: true,
            failover_timeout: 60, // 1 minute
            min_healthy_replicas: 2,
        }
    }
}

/// Durability manager for coordinating all durability features
pub struct DurabilityManager {
    config: DurabilityConfig,
    replication_manager: Arc<ReplicationManager>,
    checksum_manager: Arc<ChecksumManager>,
    backup_manager: Arc<BackupManager>,
    recovery_manager: Arc<RecoveryManager>,
    health_checker: Arc<HealthChecker>,
    failover_manager: Arc<FailoverManager>,
    durability_stats: Arc<RwLock<DurabilityStats>>,
}

/// Durability statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityStats {
    pub total_objects: u64,
    pub replicated_objects: u64,
    pub checksum_verified_objects: u64,
    pub backup_objects: u64,
    pub recovery_operations: u64,
    pub health_check_passes: u64,
    pub health_check_failures: u64,
    pub failover_events: u64,
    pub data_integrity_score: f64, // 0.0 to 1.0
    pub availability_score: f64, // 0.0 to 1.0
    pub durability_score: f64, // 0.0 to 1.0
}

impl DurabilityManager {
    pub fn new(config: DurabilityConfig) -> Result<Self> {
        let replication_manager = Arc::new(ReplicationManager::new(ReplicationConfig {
            factor: config.replication_factor,
            consistency_level: crate::cluster::distributed_storage::ConsistencyLevel::Strong,
            async_replication: true,
        })?);
        
        let checksum_manager = Arc::new(ChecksumManager::new(ChecksumConfig {
            algorithm: config.checksum_algorithm.clone(),
            enable_verification: true,
            enable_repair: true,
        })?);
        
        let backup_manager = Arc::new(BackupManager::new(BackupConfig {
            enable_backup: config.enable_backup,
            interval: config.backup_interval,
            retention: config.backup_retention,
            enable_compression: true,
        })?);
        
        let recovery_manager = Arc::new(RecoveryManager::new(RecoveryConfig {
            enable_recovery: config.enable_recovery,
            timeout: config.recovery_timeout,
            enable_auto_recovery: true,
        })?);
        
        let health_checker = Arc::new(HealthChecker::new(HealthConfig {
            enable_health_check: config.enable_health_check,
            interval: config.health_check_interval,
            timeout: 30, // 30 seconds
            enable_auto_repair: true,
        })?);
        
        let failover_manager = Arc::new(FailoverManager::new(FailoverConfig {
            enable_failover: config.enable_failover,
            timeout: config.failover_timeout,
            min_healthy_replicas: config.min_healthy_replicas,
        })?);
        
        Ok(Self {
            config,
            replication_manager,
            checksum_manager,
            backup_manager,
            recovery_manager,
            health_checker,
            failover_manager,
            durability_stats: Arc::new(RwLock::new(DurabilityStats {
                total_objects: 0,
                replicated_objects: 0,
                checksum_verified_objects: 0,
                backup_objects: 0,
                recovery_operations: 0,
                health_check_passes: 0,
                health_check_failures: 0,
                failover_events: 0,
                data_integrity_score: 1.0,
                availability_score: 1.0,
                durability_score: 1.0,
            })),
        })
    }
    
    /// Start durability monitoring and management
    pub async fn start(&self) -> Result<()> {
        // Start replication manager
        self.replication_manager.start().await?;
        
        // Start checksum manager
        self.checksum_manager.start().await?;
        
        // Start backup manager
        if self.config.enable_backup {
            self.backup_manager.start().await?;
        }
        
        // Start recovery manager
        if self.config.enable_recovery {
            self.recovery_manager.start().await?;
        }
        
        // Start health checker
        if self.config.enable_health_check {
            self.health_checker.start().await?;
        }
        
        // Start failover manager
        if self.config.enable_failover {
            self.failover_manager.start().await?;
        }
        
        tracing::info!("Durability manager started");
        Ok(())
    }
    
    /// Stop durability monitoring and management
    pub async fn stop(&self) -> Result<()> {
        // Stop all managers
        self.replication_manager.stop().await?;
        self.checksum_manager.stop().await?;
        self.backup_manager.stop().await?;
        self.recovery_manager.stop().await?;
        self.health_checker.stop().await?;
        self.failover_manager.stop().await?;
        
        tracing::info!("Durability manager stopped");
        Ok(())
    }
    
    /// Ensure object durability
    pub async fn ensure_durability(&self, object_id: &str, data: &[u8]) -> Result<DurabilityResult> {
        let start_time = Instant::now();
        
        // Calculate checksum
        let checksum = self.checksum_manager.calculate_checksum(data).await?;
        
        // Replicate object
        let replication_result = self.replication_manager.replicate_object(object_id, data).await?;
        
        // Verify checksum
        let checksum_result = self.checksum_manager.verify_checksum(data, &checksum).await?;
        
        // Create backup if enabled
        let backup_result = if self.config.enable_backup {
            self.backup_manager.create_backup(object_id, data).await?
        } else {
            BackupResult { success: true, backup_id: None }
        };
        
        let durability_time = start_time.elapsed().as_secs_f64();
        
        // Update statistics
        self.update_durability_stats(true, 1, durability_time).await;
        
        Ok(DurabilityResult {
            object_id: object_id.to_string(),
            checksum,
            replication_factor: replication_result.replication_factor,
            backup_created: backup_result.success,
            backup_id: backup_result.backup_id,
            durability_time,
            integrity_verified: checksum_result.valid,
        })
    }
    
    /// Verify object durability
    pub async fn verify_durability(&self, object_id: &str) -> Result<DurabilityVerification> {
        // Check replication status
        let replication_status = self.replication_manager.get_replication_status(object_id).await?;
        
        // Check checksum integrity
        let checksum_status = self.checksum_manager.verify_object_checksum(object_id).await?;
        
        // Check backup status
        let backup_status = if self.config.enable_backup {
            self.backup_manager.get_backup_status(object_id).await?
        } else {
            BackupStatus { exists: false, last_backup: None }
        };
        
        // Calculate durability score
        let durability_score = self.calculate_durability_score(
            &replication_status,
            &checksum_status,
            &backup_status,
        );
        
        Ok(DurabilityVerification {
            object_id: object_id.to_string(),
            replication_status,
            checksum_status,
            backup_status,
            durability_score,
            is_durable: durability_score >= 0.8, // 80% threshold
        })
    }
    
    /// Calculate durability score
    fn calculate_durability_score(
        &self,
        replication_status: &ReplicationStatus,
        checksum_status: &ChecksumStatus,
        backup_status: &BackupStatus,
    ) -> f64 {
        let mut score = 0.0;
        
        // Replication score (40% weight)
        let replication_score = if replication_status.healthy_replicas >= self.config.min_healthy_replicas {
            1.0
        } else {
            replication_status.healthy_replicas as f64 / self.config.min_healthy_replicas as f64
        };
        score += replication_score * 0.4;
        
        // Checksum score (30% weight)
        let checksum_score = if checksum_status.valid { 1.0 } else { 0.0 };
        score += checksum_score * 0.3;
        
        // Backup score (30% weight)
        let backup_score = if backup_status.exists { 1.0 } else { 0.0 };
        score += backup_score * 0.3;
        
        score.min(1.0)
    }
    
    /// Update durability statistics
    async fn update_durability_stats(&self, success: bool, object_count: u64, time: f64) {
        let mut stats = self.durability_stats.write().await;
        
        if success {
            stats.total_objects += object_count;
            stats.replicated_objects += object_count;
            stats.checksum_verified_objects += object_count;
            stats.backup_objects += object_count;
        }
        
        // Update scores
        stats.data_integrity_score = if stats.total_objects > 0 {
            stats.checksum_verified_objects as f64 / stats.total_objects as f64
        } else {
            1.0
        };
        
        stats.availability_score = if stats.total_objects > 0 {
            stats.replicated_objects as f64 / stats.total_objects as f64
        } else {
            1.0
        };
        
        stats.durability_score = (stats.data_integrity_score + stats.availability_score) / 2.0;
    }
    
    /// Get durability statistics
    pub async fn get_stats(&self) -> Result<DurabilityStats> {
        let stats = self.durability_stats.read().await;
        Ok(stats.clone())
    }
    
    /// Get replication manager
    pub fn get_replication_manager(&self) -> Arc<ReplicationManager> {
        Arc::clone(&self.replication_manager)
    }
    
    /// Get checksum manager
    pub fn get_checksum_manager(&self) -> Arc<ChecksumManager> {
        Arc::clone(&self.checksum_manager)
    }
    
    /// Get backup manager
    pub fn get_backup_manager(&self) -> Arc<BackupManager> {
        Arc::clone(&self.backup_manager)
    }
    
    /// Get recovery manager
    pub fn get_recovery_manager(&self) -> Arc<RecoveryManager> {
        Arc::clone(&self.recovery_manager)
    }
    
    /// Get health checker
    pub fn get_health_checker(&self) -> Arc<HealthChecker> {
        Arc::clone(&self.health_checker)
    }
    
    /// Get failover manager
    pub fn get_failover_manager(&self) -> Arc<FailoverManager> {
        Arc::clone(&self.failover_manager)
    }
}

/// Durability result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityResult {
    pub object_id: String,
    pub checksum: String,
    pub replication_factor: usize,
    pub backup_created: bool,
    pub backup_id: Option<String>,
    pub durability_time: f64,
    pub integrity_verified: bool,
}

/// Durability verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityVerification {
    pub object_id: String,
    pub replication_status: ReplicationStatus,
    pub checksum_status: ChecksumStatus,
    pub backup_status: BackupStatus,
    pub durability_score: f64,
    pub is_durable: bool,
}

/// Replication status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub total_replicas: usize,
    pub healthy_replicas: usize,
    pub failed_replicas: usize,
    pub replication_factor: usize,
}

/// Checksum status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecksumStatus {
    pub valid: bool,
    pub algorithm: ChecksumAlgorithm,
    pub checksum: String,
    pub last_verified: Option<u64>,
}

/// Backup status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupStatus {
    pub exists: bool,
    pub last_backup: Option<u64>,
}

/// Backup result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupResult {
    pub success: bool,
    pub backup_id: Option<String>,
}