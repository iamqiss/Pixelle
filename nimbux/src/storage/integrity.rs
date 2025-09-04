// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Advanced data integrity features with corruption detection and auto-repair

use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use blake3::{Hasher, Hash};
use crc32fast::Hasher as Crc32Hasher;
use sha2::{Sha256, Digest};

use crate::errors::{NimbuxError, Result};

/// Data integrity manager for corruption detection and repair
pub struct IntegrityManager {
    checksums: Arc<RwLock<HashMap<String, ChecksumRecord>>>,
    corruption_log: Arc<RwLock<Vec<CorruptionEvent>>>,
    repair_queue: Arc<RwLock<Vec<RepairTask>>>,
    integrity_config: IntegrityConfig,
    storage_backend: Arc<dyn crate::storage::StorageBackend>,
}

#[derive(Debug, Clone)]
pub struct IntegrityConfig {
    pub checksum_algorithm: ChecksumAlgorithm,
    pub verification_interval: Duration,
    pub repair_enabled: bool,
    pub auto_repair: bool,
    pub max_repair_attempts: u32,
    pub corruption_threshold: f64,
    pub backup_verification: bool,
    pub parallel_verification: bool,
    pub verification_batch_size: usize,
}

impl Default for IntegrityConfig {
    fn default() -> Self {
        Self {
            checksum_algorithm: ChecksumAlgorithm::Blake3,
            verification_interval: Duration::from_secs(3600), // 1 hour
            repair_enabled: true,
            auto_repair: true,
            max_repair_attempts: 3,
            corruption_threshold: 0.01, // 1% corruption threshold
            backup_verification: true,
            parallel_verification: true,
            verification_batch_size: 100,
        }
    }
}

/// Checksum algorithms supported by Nimbux
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Blake3,
    Sha256,
    Crc32,
    XxHash,
    Md5,
    Sha1,
    Custom(String),
}

impl ChecksumAlgorithm {
    pub fn as_str(&self) -> &str {
        match self {
            ChecksumAlgorithm::Blake3 => "blake3",
            ChecksumAlgorithm::Sha256 => "sha256",
            ChecksumAlgorithm::Crc32 => "crc32",
            ChecksumAlgorithm::XxHash => "xxhash",
            ChecksumAlgorithm::Md5 => "md5",
            ChecksumAlgorithm::Sha1 => "sha1",
            ChecksumAlgorithm::Custom(name) => name,
        }
    }
    
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "blake3" => ChecksumAlgorithm::Blake3,
            "sha256" => ChecksumAlgorithm::Sha256,
            "crc32" => ChecksumAlgorithm::Crc32,
            "xxhash" => ChecksumAlgorithm::XxHash,
            "md5" => ChecksumAlgorithm::Md5,
            "sha1" => ChecksumAlgorithm::Sha1,
            _ => ChecksumAlgorithm::Custom(s.to_string()),
        }
    }
}

/// Checksum record for an object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecksumRecord {
    pub object_id: String,
    pub algorithm: ChecksumAlgorithm,
    pub checksum: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub verified_at: Option<DateTime<Utc>>,
    pub verification_count: u32,
    pub last_verification_result: Option<VerificationResult>,
    pub backup_checksums: HashMap<ChecksumAlgorithm, String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    pub success: bool,
    pub timestamp: DateTime<Utc>,
    pub duration_ms: u64,
    pub error: Option<String>,
    pub corruption_detected: bool,
    pub corruption_type: Option<CorruptionType>,
    pub repair_attempted: bool,
    pub repair_successful: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CorruptionType {
    ChecksumMismatch,
    PartialDataLoss,
    CompleteDataLoss,
    MetadataCorruption,
    IndexCorruption,
    Unknown,
}

/// Corruption event log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorruptionEvent {
    pub id: String,
    pub object_id: String,
    pub event_type: CorruptionType,
    pub severity: CorruptionSeverity,
    pub detected_at: DateTime<Utc>,
    pub description: String,
    pub details: HashMap<String, serde_json::Value>,
    pub repair_attempted: bool,
    pub repair_successful: Option<bool>,
    pub resolved_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CorruptionSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Repair task for corrupted objects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairTask {
    pub id: String,
    pub object_id: String,
    pub corruption_type: CorruptionType,
    pub priority: RepairPriority,
    pub created_at: DateTime<Utc>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub status: RepairStatus,
    pub error: Option<String>,
    pub repair_strategy: RepairStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RepairPriority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RepairStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepairStrategy {
    ReplicateFromBackup,
    ReconstructFromParity,
    RebuildFromMetadata,
    ManualIntervention,
    DeleteAndRecreate,
}

/// Integrity verification report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub id: String,
    pub generated_at: DateTime<Utc>,
    pub total_objects: u64,
    pub verified_objects: u64,
    pub corrupted_objects: u64,
    pub repaired_objects: u64,
    pub failed_repairs: u64,
    pub verification_duration_ms: u64,
    pub corruption_rate: f64,
    pub repair_success_rate: f64,
    pub recommendations: Vec<String>,
    pub summary: IntegritySummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegritySummary {
    pub overall_health: HealthStatus,
    pub critical_issues: u32,
    pub warnings: u32,
    pub last_verification: Option<DateTime<Utc>>,
    pub next_verification: DateTime<Utc>,
    pub corruption_trend: TrendDirection,
    pub repair_trend: TrendDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TrendDirection {
    Improving,
    Stable,
    Degrading,
    Unknown,
}

/// Parity data for error correction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParityData {
    pub object_id: String,
    pub parity_blocks: Vec<ParityBlock>,
    pub algorithm: ParityAlgorithm,
    pub created_at: DateTime<Utc>,
    pub redundancy_level: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParityBlock {
    pub index: u32,
    pub data: Vec<u8>,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParityAlgorithm {
    ReedSolomon,
    ErasureCoding,
    RaptorQ,
    Custom(String),
}

impl IntegrityManager {
    pub fn new(
        config: IntegrityConfig,
        storage_backend: Arc<dyn crate::storage::StorageBackend>,
    ) -> Self {
        Self {
            checksums: Arc::new(RwLock::new(HashMap::new())),
            corruption_log: Arc::new(RwLock::new(Vec::new())),
            repair_queue: Arc::new(RwLock::new(Vec::new())),
            integrity_config: config,
            storage_backend,
        }
    }
    
    /// Calculate checksum for data
    pub fn calculate_checksum(&self, data: &[u8], algorithm: ChecksumAlgorithm) -> Result<String> {
        match algorithm {
            ChecksumAlgorithm::Blake3 => {
                let mut hasher = Hasher::new();
                hasher.update(data);
                let hash = hasher.finalize();
                Ok(hash.to_hex().to_string())
            }
            ChecksumAlgorithm::Sha256 => {
                let mut hasher = Sha256::new();
                hasher.update(data);
                let hash = hasher.finalize();
                Ok(hex::encode(hash))
            }
            ChecksumAlgorithm::Crc32 => {
                let mut hasher = Crc32Hasher::new();
                hasher.update(data);
                let crc = hasher.finalize();
                Ok(format!("{:08x}", crc))
            }
            ChecksumAlgorithm::XxHash => {
                // TODO: Implement XXHash
                Err(NimbuxError::Integrity("XXHash not implemented".to_string()))
            }
            ChecksumAlgorithm::Md5 => {
                // TODO: Implement MD5
                Err(NimbuxError::Integrity("MD5 not implemented".to_string()))
            }
            ChecksumAlgorithm::Sha1 => {
                // TODO: Implement SHA1
                Err(NimbuxError::Integrity("SHA1 not implemented".to_string()))
            }
            ChecksumAlgorithm::Custom(_) => {
                Err(NimbuxError::Integrity("Custom checksum algorithm not implemented".to_string()))
            }
        }
    }
    
    /// Store checksum for an object
    pub async fn store_checksum(
        &self,
        object_id: String,
        data: &[u8],
        algorithm: ChecksumAlgorithm,
    ) -> Result<()> {
        let checksum = self.calculate_checksum(data, algorithm)?;
        let now = Utc::now();
        
        let record = ChecksumRecord {
            object_id: object_id.clone(),
            algorithm,
            checksum,
            size: data.len() as u64,
            created_at: now,
            verified_at: None,
            verification_count: 0,
            last_verification_result: None,
            backup_checksums: HashMap::new(),
            metadata: HashMap::new(),
        };
        
        let mut checksums = self.checksums.write().await;
        checksums.insert(object_id, record);
        
        Ok(())
    }
    
    /// Verify object integrity
    pub async fn verify_object(&self, object_id: &str) -> Result<VerificationResult> {
        let start_time = Instant::now();
        
        // Get object from storage
        let object = match self.storage_backend.get(object_id).await {
            Ok(obj) => obj,
            Err(e) => {
                return Ok(VerificationResult {
                    success: false,
                    timestamp: Utc::now(),
                    duration_ms: start_time.elapsed().as_millis() as u64,
                    error: Some(format!("Failed to retrieve object: {}", e)),
                    corruption_detected: true,
                    corruption_type: Some(CorruptionType::CompleteDataLoss),
                    repair_attempted: false,
                    repair_successful: None,
                });
            }
        };
        
        // Get stored checksum
        let stored_checksum = {
            let checksums = self.checksums.read().await;
            checksums.get(object_id).cloned()
        };
        
        let stored_checksum = match stored_checksum {
            Some(record) => record,
            None => {
                return Ok(VerificationResult {
                    success: false,
                    timestamp: Utc::now(),
                    duration_ms: start_time.elapsed().as_millis() as u64,
                    error: Some("No checksum record found".to_string()),
                    corruption_detected: true,
                    corruption_type: Some(CorruptionType::MetadataCorruption),
                    repair_attempted: false,
                    repair_successful: None,
                });
            }
        };
        
        // Calculate current checksum
        let current_checksum = self.calculate_checksum(
            &object.data,
            stored_checksum.algorithm,
        )?;
        
        let checksum_match = current_checksum == stored_checksum.checksum;
        let corruption_detected = !checksum_match;
        
        let result = VerificationResult {
            success: checksum_match,
            timestamp: Utc::now(),
            duration_ms: start_time.elapsed().as_millis() as u64,
            error: if checksum_match { None } else { Some("Checksum mismatch".to_string()) },
            corruption_detected,
            corruption_type: if corruption_detected { Some(CorruptionType::ChecksumMismatch) } else { None },
            repair_attempted: false,
            repair_successful: None,
        };
        
        // Update checksum record
        {
            let mut checksums = self.checksums.write().await;
            if let Some(record) = checksums.get_mut(object_id) {
                record.verified_at = Some(result.timestamp);
                record.verification_count += 1;
                record.last_verification_result = Some(result.clone());
            }
        }
        
        // If corruption detected, log it and potentially queue for repair
        if corruption_detected {
            self.log_corruption(object_id, CorruptionType::ChecksumMismatch, &result).await?;
            
            if self.integrity_config.auto_repair {
                self.queue_repair(object_id, CorruptionType::ChecksumMismatch).await?;
            }
        }
        
        Ok(result)
    }
    
    /// Verify multiple objects in batch
    pub async fn verify_objects_batch(&self, object_ids: Vec<String>) -> Result<Vec<VerificationResult>> {
        let mut results = Vec::new();
        
        if self.integrity_config.parallel_verification {
            // Parallel verification
            let tasks: Vec<_> = object_ids.into_iter()
                .map(|id| {
                    let manager = self.clone();
                    tokio::spawn(async move {
                        manager.verify_object(&id).await
                    })
                })
                .collect();
            
            for task in tasks {
                match task.await {
                    Ok(Ok(result)) => results.push(result),
                    Ok(Err(e)) => {
                        results.push(VerificationResult {
                            success: false,
                            timestamp: Utc::now(),
                            duration_ms: 0,
                            error: Some(format!("Verification failed: {}", e)),
                            corruption_detected: true,
                            corruption_type: Some(CorruptionType::Unknown),
                            repair_attempted: false,
                            repair_successful: None,
                        });
                    }
                    Err(e) => {
                        results.push(VerificationResult {
                            success: false,
                            timestamp: Utc::now(),
                            duration_ms: 0,
                            error: Some(format!("Task failed: {}", e)),
                            corruption_detected: true,
                            corruption_type: Some(CorruptionType::Unknown),
                            repair_attempted: false,
                            repair_successful: None,
                        });
                    }
                }
            }
        } else {
            // Sequential verification
            for object_id in object_ids {
                match self.verify_object(&object_id).await {
                    Ok(result) => results.push(result),
                    Err(e) => {
                        results.push(VerificationResult {
                            success: false,
                            timestamp: Utc::now(),
                            duration_ms: 0,
                            error: Some(format!("Verification failed: {}", e)),
                            corruption_detected: true,
                            corruption_type: Some(CorruptionType::Unknown),
                            repair_attempted: false,
                            repair_successful: None,
                        });
                    }
                }
            }
        }
        
        Ok(results)
    }
    
    /// Run full integrity verification
    pub async fn run_full_verification(&self) -> Result<IntegrityReport> {
        let start_time = Instant::now();
        let report_id = Uuid::new_v4().to_string();
        
        // Get all object IDs
        let object_metadata = self.storage_backend.list(None, None).await?;
        let object_ids: Vec<String> = object_metadata.into_iter().map(|m| m.id).collect();
        let total_objects = object_ids.len() as u64;
        
        // Verify objects in batches
        let mut all_results = Vec::new();
        let batch_size = self.integrity_config.verification_batch_size;
        
        for chunk in object_ids.chunks(batch_size) {
            let batch_results = self.verify_objects_batch(chunk.to_vec()).await?;
            all_results.extend(batch_results);
        }
        
        // Analyze results
        let verified_objects = all_results.len() as u64;
        let corrupted_objects = all_results.iter().filter(|r| r.corruption_detected).count() as u64;
        let repaired_objects = all_results.iter().filter(|r| r.repair_successful == Some(true)).count() as u64;
        let failed_repairs = all_results.iter().filter(|r| r.repair_attempted && r.repair_successful == Some(false)).count() as u64;
        
        let corruption_rate = if verified_objects > 0 {
            corrupted_objects as f64 / verified_objects as f64
        } else {
            0.0
        };
        
        let repair_success_rate = if repaired_objects + failed_repairs > 0 {
            repaired_objects as f64 / (repaired_objects + failed_repairs) as f64
        } else {
            1.0
        };
        
        // Generate recommendations
        let mut recommendations = Vec::new();
        if corruption_rate > self.integrity_config.corruption_threshold {
            recommendations.push("High corruption rate detected. Consider increasing verification frequency.".to_string());
        }
        if repair_success_rate < 0.8 {
            recommendations.push("Low repair success rate. Review repair strategies and backup integrity.".to_string());
        }
        if corrupted_objects > 0 {
            recommendations.push("Corrupted objects detected. Manual intervention may be required.".to_string());
        }
        
        // Determine overall health
        let overall_health = if corruption_rate == 0.0 {
            HealthStatus::Healthy
        } else if corruption_rate < 0.01 {
            HealthStatus::Warning
        } else {
            HealthStatus::Critical
        };
        
        let summary = IntegritySummary {
            overall_health,
            critical_issues: if corruption_rate > 0.05 { 1 } else { 0 },
            warnings: if corruption_rate > 0.01 { 1 } else { 0 },
            last_verification: Some(Utc::now()),
            next_verification: Utc::now() + chrono::Duration::from_std(self.integrity_config.verification_interval).unwrap(),
            corruption_trend: TrendDirection::Unknown, // TODO: Implement trend analysis
            repair_trend: TrendDirection::Unknown,
        };
        
        let report = IntegrityReport {
            id: report_id,
            generated_at: Utc::now(),
            total_objects,
            verified_objects,
            corrupted_objects,
            repaired_objects,
            failed_repairs,
            verification_duration_ms: start_time.elapsed().as_millis() as u64,
            corruption_rate,
            repair_success_rate,
            recommendations,
            summary,
        };
        
        Ok(report)
    }
    
    /// Log corruption event
    async fn log_corruption(
        &self,
        object_id: &str,
        corruption_type: CorruptionType,
        verification_result: &VerificationResult,
    ) -> Result<()> {
        let event = CorruptionEvent {
            id: Uuid::new_v4().to_string(),
            object_id: object_id.to_string(),
            event_type: corruption_type.clone(),
            severity: self.determine_severity(&corruption_type),
            detected_at: Utc::now(),
            description: format!("Corruption detected: {:?}", corruption_type),
            details: HashMap::new(),
            repair_attempted: false,
            repair_successful: None,
            resolved_at: None,
        };
        
        let mut corruption_log = self.corruption_log.write().await;
        corruption_log.push(event);
        
        // Keep only last 1000 events
        if corruption_log.len() > 1000 {
            corruption_log.drain(0..corruption_log.len() - 1000);
        }
        
        Ok(())
    }
    
    /// Queue object for repair
    async fn queue_repair(&self, object_id: &str, corruption_type: CorruptionType) -> Result<()> {
        let task = RepairTask {
            id: Uuid::new_v4().to_string(),
            object_id: object_id.to_string(),
            corruption_type: corruption_type.clone(),
            priority: self.determine_repair_priority(&corruption_type),
            created_at: Utc::now(),
            attempts: 0,
            max_attempts: self.integrity_config.max_repair_attempts,
            status: RepairStatus::Pending,
            error: None,
            repair_strategy: self.determine_repair_strategy(&corruption_type),
        };
        
        let mut repair_queue = self.repair_queue.write().await;
        repair_queue.push(task);
        
        Ok(())
    }
    
    /// Process repair queue
    pub async fn process_repair_queue(&self) -> Result<RepairSummary> {
        let mut repair_queue = self.repair_queue.write().await;
        let mut completed = 0;
        let mut failed = 0;
        let mut in_progress = 0;
        
        for task in repair_queue.iter_mut() {
            if task.status == RepairStatus::Pending {
                task.status = RepairStatus::InProgress;
                in_progress += 1;
                
                // Attempt repair
                match self.attempt_repair(task).await {
                    Ok(success) => {
                        if success {
                            task.status = RepairStatus::Completed;
                            completed += 1;
                        } else {
                            task.attempts += 1;
                            if task.attempts >= task.max_attempts {
                                task.status = RepairStatus::Failed;
                                failed += 1;
                            } else {
                                task.status = RepairStatus::Pending;
                            }
                        }
                    }
                    Err(e) => {
                        task.attempts += 1;
                        task.error = Some(e.to_string());
                        if task.attempts >= task.max_attempts {
                            task.status = RepairStatus::Failed;
                            failed += 1;
                        } else {
                            task.status = RepairStatus::Pending;
                        }
                    }
                }
            }
        }
        
        // Remove completed and failed tasks
        repair_queue.retain(|task| {
            task.status == RepairStatus::Pending || task.status == RepairStatus::InProgress
        });
        
        Ok(RepairSummary {
            completed,
            failed,
            in_progress,
            remaining: repair_queue.len() as u32,
        })
    }
    
    /// Attempt to repair a corrupted object
    async fn attempt_repair(&self, task: &RepairTask) -> Result<bool> {
        match task.repair_strategy {
            RepairStrategy::ReplicateFromBackup => {
                // TODO: Implement backup replication
                Ok(false)
            }
            RepairStrategy::ReconstructFromParity => {
                // TODO: Implement parity reconstruction
                Ok(false)
            }
            RepairStrategy::RebuildFromMetadata => {
                // TODO: Implement metadata rebuild
                Ok(false)
            }
            RepairStrategy::ManualIntervention => {
                // TODO: Notify administrators
                Ok(false)
            }
            RepairStrategy::DeleteAndRecreate => {
                // Delete corrupted object
                self.storage_backend.delete(&task.object_id).await?;
                Ok(true)
            }
        }
    }
    
    /// Get integrity statistics
    pub async fn get_integrity_stats(&self) -> Result<IntegrityStats> {
        let checksums = self.checksums.read().await;
        let corruption_log = self.corruption_log.read().await;
        let repair_queue = self.repair_queue.read().await;
        
        let total_objects = checksums.len() as u64;
        let verified_objects = checksums.values()
            .filter(|r| r.verified_at.is_some())
            .count() as u64;
        
        let corrupted_objects = corruption_log.iter()
            .filter(|e| e.resolved_at.is_none())
            .count() as u64;
        
        let pending_repairs = repair_queue.iter()
            .filter(|t| t.status == RepairStatus::Pending)
            .count() as u64;
        
        Ok(IntegrityStats {
            total_objects,
            verified_objects,
            corrupted_objects,
            pending_repairs,
            last_verification: checksums.values()
                .filter_map(|r| r.verified_at)
                .max(),
            corruption_events_24h: corruption_log.iter()
                .filter(|e| e.detected_at > Utc::now() - chrono::Duration::hours(24))
                .count() as u64,
        })
    }
    
    // Helper methods
    
    fn determine_severity(&self, corruption_type: &CorruptionType) -> CorruptionSeverity {
        match corruption_type {
            CorruptionType::CompleteDataLoss => CorruptionSeverity::Critical,
            CorruptionType::PartialDataLoss => CorruptionSeverity::High,
            CorruptionType::ChecksumMismatch => CorruptionSeverity::Medium,
            CorruptionType::MetadataCorruption => CorruptionSeverity::Low,
            CorruptionType::IndexCorruption => CorruptionSeverity::Medium,
            CorruptionType::Unknown => CorruptionSeverity::High,
        }
    }
    
    fn determine_repair_priority(&self, corruption_type: &CorruptionType) -> RepairPriority {
        match corruption_type {
            CorruptionType::CompleteDataLoss => RepairPriority::Critical,
            CorruptionType::PartialDataLoss => RepairPriority::High,
            CorruptionType::ChecksumMismatch => RepairPriority::Normal,
            CorruptionType::MetadataCorruption => RepairPriority::Low,
            CorruptionType::IndexCorruption => RepairPriority::High,
            CorruptionType::Unknown => RepairPriority::Normal,
        }
    }
    
    fn determine_repair_strategy(&self, corruption_type: &CorruptionType) -> RepairStrategy {
        match corruption_type {
            CorruptionType::CompleteDataLoss => RepairStrategy::ReplicateFromBackup,
            CorruptionType::PartialDataLoss => RepairStrategy::ReconstructFromParity,
            CorruptionType::ChecksumMismatch => RepairStrategy::ReplicateFromBackup,
            CorruptionType::MetadataCorruption => RepairStrategy::RebuildFromMetadata,
            CorruptionType::IndexCorruption => RepairStrategy::RebuildFromMetadata,
            CorruptionType::Unknown => RepairStrategy::ManualIntervention,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairSummary {
    pub completed: u32,
    pub failed: u32,
    pub in_progress: u32,
    pub remaining: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityStats {
    pub total_objects: u64,
    pub verified_objects: u64,
    pub corrupted_objects: u64,
    pub pending_repairs: u64,
    pub last_verification: Option<DateTime<Utc>>,
    pub corruption_events_24h: u64,
}

impl Clone for IntegrityManager {
    fn clone(&self) -> Self {
        Self {
            checksums: Arc::clone(&self.checksums),
            corruption_log: Arc::clone(&self.corruption_log),
            repair_queue: Arc::clone(&self.repair_queue),
            integrity_config: self.integrity_config.clone(),
            storage_backend: Arc::clone(&self.storage_backend),
        }
    }
}