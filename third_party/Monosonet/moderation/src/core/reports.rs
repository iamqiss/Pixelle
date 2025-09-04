use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::core::classifier::{ClassificationLabel, ClassificationResult};
use crate::core::signals::{Signal, SignalType, SignalSeverity, SignalProcessor};
use crate::core::observability::MetricsCollector;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserReport {
    pub id: Uuid,
    pub reporter_id: Uuid,
    pub target_id: Uuid,
    pub content_id: Option<String>,
    pub report_type: ReportType,
    pub reason: String,
    pub description: Option<String>,
    pub evidence: Vec<Evidence>,
    pub status: ReportStatus,
    pub priority: ReportPriority,
    pub assigned_specialist: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ReportType {
    // Content violations
    HateSpeech,
    Harassment,
    Violence,
    Spam,
    Misinformation,
    Copyright,
    Nsfw,
    ChildSafety,
    
    // User behavior
    Impersonation,
    Bot,
    Troll,
    Harassment,
    Threats,
    
    // System abuse
    ReportAbuse,
    SpamReports,
    FalseReporting,
    
    // Custom types
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ReportStatus {
    Pending,
    UnderInvestigation,
    Escalated,
    Resolved,
    Dismissed,
    RequiresMoreInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ReportPriority {
    Low,
    Normal,
    High,
    Critical,
    Urgent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub id: Uuid,
    pub evidence_type: EvidenceType,
    pub content: String,
    pub url: Option<String>,
    pub screenshot: Option<Vec<u8>>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvidenceType {
    Text,
    Image,
    Link,
    Video,
    Audio,
    Screenshot,
    Log,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportInvestigation {
    pub id: Uuid,
    pub report_id: Uuid,
    pub investigator_id: Uuid,
    pub status: InvestigationStatus,
    pub findings: Vec<Finding>,
    pub actions_taken: Vec<InvestigationAction>,
    pub notes: Vec<InvestigationNote>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub time_spent_minutes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InvestigationStatus {
    NotStarted,
    InProgress,
    PendingReview,
    Completed,
    Escalated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub id: Uuid,
    pub finding_type: FindingType,
    pub description: String,
    pub confidence: f32,
    pub evidence_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FindingType {
    ViolationConfirmed,
    ViolationPartial,
    NoViolation,
    RequiresMoreContext,
    FalseReport,
    Ambiguous,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvestigationAction {
    pub id: Uuid,
    pub action_type: InvestigationActionType,
    pub description: String,
    pub parameters: HashMap<String, serde_json::Value>,
    pub executed_at: DateTime<Utc>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InvestigationActionType {
    ContentRemoval,
    UserSuspension,
    UserBan,
    ContentFlag,
    WarningIssued,
    ReportDismissed,
    Escalation,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvestigationNote {
    pub id: Uuid,
    pub author_id: Uuid,
    pub content: String,
    pub is_internal: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportMetrics {
    pub total_reports: u64,
    pub reports_by_type: HashMap<ReportType, u64>,
    pub reports_by_status: HashMap<ReportStatus, u64>,
    pub reports_by_priority: HashMap<ReportPriority, u64>,
    pub average_resolution_time_minutes: u64,
    pub false_report_rate: f32,
}

pub struct ReportManager {
    reports: Arc<RwLock<HashMap<Uuid, UserReport>>>,
    investigations: Arc<RwLock<HashMap<Uuid, ReportInvestigation>>>,
    signal_processor: Arc<SignalProcessor>,
    metrics: Arc<MetricsCollector>,
    config: ReportManagerConfig,
    datastores: Option<Arc<crate::storage::db::Datastores>>,
    event_tx: Option<broadcast::Sender<serde_json::Value>>,
}

#[derive(Debug, Clone)]
pub struct ReportManagerConfig {
    pub max_reports_per_user: u32,
    pub auto_escalation_threshold: u32,
    pub specialist_assignment_enabled: bool,
    pub enable_ml_assistance: bool,
    pub report_retention_days: u32,
    pub enable_auto_resolution: bool,
}

impl Default for ReportManagerConfig {
    fn default() -> Self {
        Self {
            max_reports_per_user: 10,
            auto_escalation_threshold: 5,
            specialist_assignment_enabled: true,
            enable_ml_assistance: true,
            report_retention_days: 365,
            enable_auto_resolution: false,
        }
    }
}

impl ReportManager {
    pub fn new(
        signal_processor: Arc<SignalProcessor>,
        metrics: Arc<MetricsCollector>,
        config: ReportManagerConfig,
        
    ) -> Self {
        Self {
            reports: Arc::new(RwLock::new(HashMap::new())),
            investigations: Arc::new(RwLock::new(HashMap::new())),
            signal_processor,
            metrics,
            config,
            datastores: None,
            event_tx: None,
        }
    }

    pub fn with_datastores(mut self, datastores: Arc<crate::storage::db::Datastores>) -> Self {
        self.datastores = Some(datastores);
        self
    }

    pub fn with_event_sender(mut self, tx: broadcast::Sender<serde_json::Value>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    pub async fn create_report(&self, report: CreateReportRequest) -> Result<UserReport> {
        // Validate report
        self.validate_report(&report).await?;
        
        // Check rate limiting
        self.check_rate_limits(&report.reporter_id).await?;
        
        // Create report
        let user_report = UserReport {
            id: Uuid::new_v4(),
            reporter_id: report.reporter_id,
            target_id: report.target_id,
            content_id: report.content_id,
            report_type: report.report_type,
            reason: report.reason,
            description: report.description,
            evidence: report.evidence,
            status: ReportStatus::Pending,
            priority: self.calculate_priority(&report),
            assigned_specialist: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            resolved_at: None,
            metadata: report.metadata,
        };
        
        // Store report (in memory)
        self.reports.write().await.insert(user_report.id, user_report.clone());
        // Persist to database if configured
        if let Some(ds) = &self.datastores {
            ds.insert_user_report(&user_report).await.ok();
        }
        
        // Generate signals
        self.generate_report_signals(&user_report).await?;
        
        // Auto-assign if high priority
        if matches!(user_report.priority, ReportPriority::Critical | ReportPriority::Urgent) {
            self.auto_assign_specialist(&user_report.id).await?;
        }
        
        // Record metrics
        self.metrics.record_report_created(&user_report);
        // Emit event
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(serde_json::json!({
                "type": "report.created",
                "report": {
                    "id": user_report.id,
                    "reporter_id": user_report.reporter_id,
                    "target_id": user_report.target_id,
                    "content_id": user_report.content_id,
                    "report_type": format!("{:?}", user_report.report_type),
                    "reason": user_report.reason,
                    "priority": format!("{:?}", user_report.priority),
                    "status": format!("{:?}", user_report.status),
                    "created_at": user_report.created_at,
                }
            }));
        }
        
        Ok(user_report)
    }

    pub async fn get_report(&self, report_id: Uuid) -> Option<UserReport> {
        if let Some(r) = self.reports.read().await.get(&report_id).cloned() { return Some(r); }
        if let Some(ds) = &self.datastores {
            if let Ok(opt) = ds.fetch_user_report(report_id).await { return opt; }
        }
        None
    }

    pub async fn get_reports_by_user(&self, user_id: Uuid) -> Vec<UserReport> {
        self.reports.read().await
            .values()
            .filter(|report| report.target_id == user_id)
            .cloned()
            .collect()
    }

    pub async fn get_reports_by_status(&self, status: ReportStatus) -> Vec<UserReport> {
        // Prefer in-memory for speed; fall back to DB
        let mut v: Vec<UserReport> = self.reports.read().await.values().filter(|r| r.status == status).cloned().collect();
        if v.is_empty() {
            if let Some(ds) = &self.datastores {
                let key = format!("{:?}", status);
                if let Ok(rows) = ds.list_user_reports(Some(key), None, 100, 0, None).await { v = rows; }
            }
        }
        v
    }

    pub async fn get_all_reports(&self) -> Vec<UserReport> {
        self.reports.read().await
            .values()
            .cloned()
            .collect()
    }

    pub async fn update_report_status(&self, report_id: Uuid, status: ReportStatus) -> Result<()> {
        let mut reports = self.reports.write().await;
        
        if let Some(report) = reports.get_mut(&report_id) {
            report.status = status;
            report.updated_at = Utc::now();
            
            if matches!(status, ReportStatus::Resolved | ReportStatus::Dismissed) {
                report.resolved_at = Some(Utc::now());
            }
            // Emit event
            if let Some(tx) = &self.event_tx {
                let _ = tx.send(serde_json::json!({
                    "type": "report.status_updated",
                    "report_id": report_id,
                    "status": format!("{:?}", status),
                    "updated_at": report.updated_at,
                }));
            }
        }
        if let Some(ds) = &self.datastores {
            let key = format!("{:?}", status);
            ds.update_user_report_status(report_id, key).await.ok();
        }
        
        Ok(())
    }

    pub async fn assign_specialist(&self, report_id: Uuid, specialist_id: Uuid) -> Result<()> {
        let mut reports = self.reports.write().await;
        
        if let Some(report) = reports.get_mut(&report_id) {
            report.assigned_specialist = Some(specialist_id);
            report.status = ReportStatus::UnderInvestigation;
            report.updated_at = Utc::now();
        }
        
        Ok(())
    }

    pub async fn start_investigation(&self, report_id: Uuid, investigator_id: Uuid) -> Result<ReportInvestigation> {
        let investigation = ReportInvestigation {
            id: Uuid::new_v4(),
            report_id,
            investigator_id,
            status: InvestigationStatus::InProgress,
            findings: Vec::new(),
            actions_taken: Vec::new(),
            notes: Vec::new(),
            started_at: Utc::now(),
            completed_at: None,
            time_spent_minutes: 0,
        };
        
        self.investigations.write().await.insert(investigation.id, investigation.clone());
        
        // Update report status
        self.update_report_status(report_id, ReportStatus::UnderInvestigation).await?;
        // Emit event
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(serde_json::json!({
                "type": "investigation.started",
                "investigation_id": investigation.id,
                "report_id": report_id,
                "investigator_id": investigator_id,
                "started_at": investigation.started_at,
            }));
        }
        
        Ok(investigation)
    }

    pub async fn add_investigation_finding(
        &self,
        investigation_id: Uuid,
        finding: Finding,
    ) -> Result<()> {
        let mut investigations = self.investigations.write().await;
        
        if let Some(investigation) = investigations.get_mut(&investigation_id) {
            investigation.findings.push(finding);
        }
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(serde_json::json!({
                "type": "investigation.finding_added",
                "investigation_id": investigation_id,
            }));
        }
        
        Ok(())
    }

    pub async fn add_investigation_note(
        &self,
        investigation_id: Uuid,
        note: InvestigationNote,
    ) -> Result<()> {
        let mut investigations = self.investigations.write().await;
        
        if let Some(investigation) = investigations.get_mut(&investigation_id) {
            investigation.notes.push(note);
        }
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(serde_json::json!({
                "type": "investigation.note_added",
                "investigation_id": investigation_id,
            }));
        }
        
        Ok(())
    }

    pub async fn complete_investigation(
        &self,
        investigation_id: Uuid,
        final_status: InvestigationStatus,
    ) -> Result<()> {
        let mut investigations = self.investigations.write().await;
        
        if let Some(investigation) = investigations.get_mut(&investigation_id) {
            investigation.status = final_status;
            investigation.completed_at = Some(Utc::now());
            
            // Calculate time spent
            if let Some(completed_at) = investigation.completed_at {
                let duration = completed_at - investigation.started_at;
                investigation.time_spent_minutes = duration.num_minutes() as u32;
            }
        }
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(serde_json::json!({
                "type": "investigation.completed",
                "investigation_id": investigation_id,
            }));
        }
        
        Ok(())
    }

    pub async fn get_report_metrics(&self, time_range: Option<(DateTime<Utc>, DateTime<Utc>)>) -> ReportMetrics {
        let reports = self.reports.read().await;
        let investigations = self.investigations.read().await;
        
        let mut metrics = ReportMetrics {
            total_reports: reports.len() as u64,
            reports_by_type: HashMap::new(),
            reports_by_status: HashMap::new(),
            reports_by_priority: HashMap::new(),
            average_resolution_time_minutes: 0,
            false_report_rate: 0.0,
        };
        
        // Calculate metrics
        for report in reports.values() {
            if let Some((start, end)) = time_range {
                if report.created_at < start || report.created_at > end {
                    continue;
                }
            }
            
            // Count by type
            *metrics.reports_by_type.entry(report.report_type.clone()).or_insert(0) += 1;
            
            // Count by status
            *metrics.reports_by_status.entry(report.status.clone()).or_insert(0) += 1;
            
            // Count by priority
            *metrics.reports_by_priority.entry(report.priority.clone()).or_insert(0) += 1;
        }
        
        // Calculate average resolution time
        let resolved_reports: Vec<_> = reports.values()
            .filter(|r| r.resolved_at.is_some())
            .collect();
        
        if !resolved_reports.is_empty() {
            let total_time: i64 = resolved_reports.iter()
                .map(|r| {
                    let duration = r.resolved_at.unwrap() - r.created_at;
                    duration.num_minutes()
                })
                .sum();
            
            metrics.average_resolution_time_minutes = (total_time / resolved_reports.len() as i64) as u64;
        }
        
        // Calculate false report rate
        let false_reports = investigations.values()
            .filter(|i| i.findings.iter().any(|f| matches!(f.finding_type, FindingType::FalseReport)))
            .count();
        
        if !investigations.is_empty() {
            metrics.false_report_rate = false_reports as f32 / investigations.len() as f32;
        }
        
        metrics
    }

    async fn validate_report(&self, report: &CreateReportRequest) -> Result<()> {
        // Check if reporter exists and is not banned
        // Check if target exists
        // Validate evidence
        // Check for duplicate reports
        
        if report.reason.trim().is_empty() {
            return Err(anyhow::anyhow!("Report reason cannot be empty"));
        }
        
        if report.evidence.is_empty() {
            return Err(anyhow::anyhow!("Report must include evidence"));
        }
        
        Ok(())
    }

    async fn check_rate_limits(&self, reporter_id: &Uuid) -> Result<()> {
        let reports = self.reports.read().await;
        let user_reports = reports.values()
            .filter(|r| r.reporter_id == *reporter_id)
            .filter(|r| {
                let cutoff = Utc::now() - chrono::Duration::days(1);
                r.created_at > cutoff
            })
            .count();
        
        if user_reports >= self.config.max_reports_per_user as usize {
            return Err(anyhow::anyhow!("Rate limit exceeded for reports"));
        }
        
        Ok(())
    }

    fn calculate_priority(&self, report: &CreateReportRequest) -> ReportPriority {
        match report.report_type {
            ReportType::ChildSafety => ReportPriority::Critical,
            ReportType::Violence | ReportType::Threats => ReportPriority::Urgent,
            ReportType::HateSpeech | ReportType::Harassment => ReportPriority::High,
            ReportType::Spam | ReportType::Misinformation => ReportPriority::Normal,
            _ => ReportPriority::Low,
        }
    }

    async fn generate_report_signals(&self, report: &UserReport) -> Result<()> {
        let signal = Signal {
            id: Uuid::new_v4(),
            signal_type: SignalType::UserReport,
            source: "user_report".to_string(),
            content_id: report.content_id.clone().unwrap_or_default(),
            user_id: report.target_id.to_string(),
            severity: self.map_priority_to_severity(&report.priority),
            confidence: 0.8, // User reports have high confidence
            metadata: {
                let mut meta = report.metadata.clone();
                meta.insert("report_id".to_string(), serde_json::json!(report.id));
                meta.insert("report_type".to_string(), serde_json::json!(report.report_type));
                meta.insert("reporter_id".to_string(), serde_json::json!(report.reporter_id));
                meta
            },
            timestamp: Utc::now(),
            expires_at: Some(Utc::now() + chrono::Duration::days(30)),
        };
        
        self.signal_processor.add_signal(signal).await?;
        Ok(())
    }

    async fn auto_assign_specialist(&self, _report_id: &Uuid) -> Result<()> {
        // Implement specialist auto-assignment logic
        // This would typically involve:
        // 1. Finding available specialists
        // 2. Checking workload
        // 3. Matching expertise
        // 4. Assigning the report
        
        Ok(())
    }

    fn map_priority_to_severity(&self, priority: &ReportPriority) -> SignalSeverity {
        match priority {
            ReportPriority::Low => SignalSeverity::Low,
            ReportPriority::Normal => SignalSeverity::Medium,
            ReportPriority::High => SignalSeverity::High,
            ReportPriority::Critical | ReportPriority::Urgent => SignalSeverity::Critical,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReportRequest {
    pub reporter_id: Uuid,
    pub target_id: Uuid,
    pub content_id: Option<String>,
    pub report_type: ReportType,
    pub reason: String,
    pub description: Option<String>,
    pub evidence: Vec<Evidence>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl UserReport {
    pub fn new(reporter_id: Uuid, target_id: Uuid, reason: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            reporter_id,
            target_id,
            content_id: None,
            report_type: ReportType::Custom("general".to_string()),
            reason,
            description: None,
            evidence: Vec::new(),
            status: ReportStatus::Pending,
            priority: ReportPriority::Normal,
            assigned_specialist: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            resolved_at: None,
            metadata: HashMap::new(),
        }
    }
}