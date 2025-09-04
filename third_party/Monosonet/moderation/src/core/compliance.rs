use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};
use anyhow::Result;

use crate::core::reports::{UserReport, ReportType, ReportStatus};
use crate::core::classifier::{ClassificationResult, ClassificationLabel};
use crate::core::signals::{Signal, SignalType, SignalSeverity};

/// Compliance and data export functionality for legal and regulatory requirements

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExportFormat {
    Json,
    Csv,
    Excel,
    Pdf,
    Xml,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExportScope {
    All,
    User(String),           // Specific user ID
    Content(String),         // Specific content ID
    DateRange(DateTime<Utc>, DateTime<Utc>),
    ReportType(ReportType),
    ClassificationLabel(ClassificationLabel),
    SignalType(SignalType),
    Severity(SignalSeverity),
    Custom(HashMap<String, String>), // Custom filters
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportRequest {
    pub id: Uuid,
    pub requester_id: String,
    pub requester_type: RequesterType,
    pub export_scope: ExportScope,
    pub format: ExportFormat,
    pub include_metadata: bool,
    pub include_evidence: bool,
    pub include_classifications: bool,
    pub include_signals: bool,
    pub include_reports: bool,
    pub include_audit_logs: bool,
    pub encryption_required: bool,
    pub legal_basis: String,
    pub urgency: Urgency,
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub status: ExportStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequesterType {
    LawEnforcement,
    RegulatoryBody,
    InternalAudit,
    LegalCounsel,
    BusinessReport,
    UserRequest,
    CourtOrder,
    Subpoena,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Urgency {
    Low,        // Quarterly reports
    Normal,     // Regular compliance
    High,       // Legal requests
    Critical,   // CSAM, terrorism, immediate threats
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExportStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    Expired,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportResult {
    pub id: Uuid,
    pub export_request_id: Uuid,
    pub file_path: String,
    pub file_size: u64,
    pub record_count: u64,
    pub checksum: String,
    pub encryption_key: Option<String>,
    pub download_url: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LawEnforcementRequest {
    pub id: Uuid,
    pub agency: String,
    pub officer_name: String,
    pub badge_number: String,
    pub case_number: String,
    pub legal_basis: String,
    pub request_type: LawEnforcementRequestType,
    pub urgency: Urgency,
    pub target_user: Option<String>,
    pub target_content: Option<String>,
    pub date_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub specific_evidence: Vec<String>,
    pub legal_documents: Vec<String>, // URLs to legal documents
    pub status: LawEnforcementRequestStatus,
    pub assigned_to: Option<String>,
    pub notes: Vec<LawEnforcementNote>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LawEnforcementRequestType {
    UserData,
    ContentData,
    CommunicationLogs,
    FinancialData,
    LocationData,
    DeviceData,
    NetworkData,
    FullAccount,
    EmergencyDisclosure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LawEnforcementRequestStatus {
    Received,
    UnderReview,
    LegalReview,
    Approved,
    Rejected,
    DataCollected,
    DataDelivered,
    Completed,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LawEnforcementNote {
    pub id: Uuid,
    pub author_id: String,
    pub content: String,
    pub is_internal: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceReport {
    pub id: Uuid,
    pub report_type: ComplianceReportType,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub generated_by: String,
    pub data: ComplianceReportData,
    pub summary: ComplianceSummary,
    pub attachments: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComplianceReportType {
    Quarterly,
    Annual,
    Incident,
    Audit,
    Regulatory,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceReportData {
    pub total_users: u64,
    pub total_content: u64,
    pub total_reports: u64,
    pub total_violations: u64,
    pub content_removed: u64,
    pub accounts_suspended: u64,
    pub accounts_terminated: u64,
    pub law_enforcement_requests: u64,
    pub regulatory_inquiries: u64,
    pub legal_actions: u64,
    pub violations_by_type: HashMap<String, u64>,
    pub violations_by_severity: HashMap<String, u64>,
    pub response_times: HashMap<String, f64>,
    pub false_positive_rate: f64,
    pub appeal_success_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceSummary {
    pub key_findings: Vec<String>,
    pub risk_assessment: String,
    pub recommendations: Vec<String>,
    pub compliance_status: ComplianceStatus,
    pub next_review_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComplianceStatus {
    Compliant,
    MinorIssues,
    MajorIssues,
    NonCompliant,
    UnderInvestigation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLog {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub user_id: Option<String>,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub details: HashMap<String, String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub session_id: Option<String>,
    pub outcome: AuditOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditOutcome {
    Success,
    Failure,
    Partial,
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRetentionPolicy {
    pub id: Uuid,
    pub data_type: String,
    pub retention_period: Duration,
    pub legal_hold: bool,
    pub auto_deletion: bool,
    pub backup_required: bool,
    pub encryption_required: bool,
    pub access_controls: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Main compliance manager for handling all compliance-related operations
pub struct ComplianceManager {
    export_requests: Arc<RwLock<HashMap<Uuid, ExportRequest>>>,
    export_results: Arc<RwLock<HashMap<Uuid, ExportResult>>>,
    law_enforcement_requests: Arc<RwLock<HashMap<Uuid, LawEnforcementRequest>>>,
    compliance_reports: Arc<RwLock<HashMap<Uuid, ComplianceReport>>>,
    audit_logs: Arc<RwLock<Vec<AuditLog>>>,
    data_retention_policies: Arc<RwLock<HashMap<String, DataRetentionPolicy>>>,
    config: ComplianceConfig,
}

#[derive(Debug, Clone)]
pub struct ComplianceConfig {
    pub max_export_size: u64,
    pub export_retention_days: u32,
    pub encryption_required: bool,
    pub legal_review_required: bool,
    pub emergency_response_time: Duration,
    pub audit_log_retention: Duration,
    pub compliance_reporting_enabled: bool,
}

impl Default for ComplianceConfig {
    fn default() -> Self {
        Self {
            max_export_size: 100 * 1024 * 1024, // 100MB
            export_retention_days: 90,
            encryption_required: true,
            legal_review_required: true,
            emergency_response_time: Duration::hours(24),
            audit_log_retention: Duration::days(2555), // 7 years
            compliance_reporting_enabled: true,
        }
    }
}

impl ComplianceManager {
    pub fn new(config: ComplianceConfig) -> Self {
        Self {
            export_requests: Arc::new(RwLock::new(HashMap::new())),
            export_results: Arc::new(RwLock::new(HashMap::new())),
            law_enforcement_requests: Arc::new(RwLock::new(HashMap::new())),
            compliance_reports: Arc::new(RwLock::new(HashMap::new())),
            audit_logs: Arc::new(RwLock::new(Vec::new())),
            data_retention_policies: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Create a new export request
    pub async fn create_export_request(
        &self,
        requester_id: String,
        requester_type: RequesterType,
        export_scope: ExportScope,
        format: ExportFormat,
        legal_basis: String,
        urgency: Urgency,
        notes: Option<String>,
    ) -> Result<ExportRequest> {
        let request = ExportRequest {
            id: Uuid::new_v4(),
            requester_id,
            requester_type,
            export_scope,
            format,
            include_metadata: true,
            include_evidence: true,
            include_classifications: true,
            include_signals: true,
            include_reports: true,
            include_audit_logs: true,
            encryption_required: self.config.encryption_required,
            legal_basis,
            urgency,
            notes,
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::days(30),
            status: ExportStatus::Pending,
        };

        // Log the export request
        self.log_audit_event(
            Some(request.requester_id.clone()),
            "export_request_created",
            "export_request",
            Some(request.id.to_string()),
            HashMap::new(),
            AuditOutcome::Success,
        ).await;

        self.export_requests.write().await.insert(request.id, request.clone());
        Ok(request)
    }

    /// Process an export request and generate the data export
    pub async fn process_export_request(&self, request_id: Uuid) -> Result<ExportResult> {
        let mut requests = self.export_requests.write().await;
        let request = requests.get_mut(&request_id)
            .ok_or_else(|| anyhow::anyhow!("Export request not found"))?;

        request.status = ExportStatus::Processing;

        // Generate the export based on scope and format
        let export_data = self.generate_export_data(&request.export_scope).await?;
        let file_path = self.save_export_file(request_id, &export_data, &request.format).await?;
        let checksum = self.calculate_checksum(&file_path).await?;

        let result = ExportResult {
            id: Uuid::new_v4(),
            export_request_id: request_id,
            file_path: file_path.clone(),
            file_size: std::fs::metadata(&file_path)?.len(),
            record_count: export_data.len() as u64,
            checksum,
            encryption_key: if request.encryption_required {
                Some(self.encrypt_file(&file_path).await?)
            } else {
                None
            },
            download_url: Some(self.generate_download_url(request_id).await?),
            expires_at: Utc::now() + Duration::days(self.config.export_retention_days as i64),
            created_at: Utc::now(),
        };

        request.status = ExportStatus::Completed;
        self.export_results.write().await.insert(result.id, result.clone());

        // Log successful export
        self.log_audit_event(
            Some(request.requester_id.clone()),
            "export_completed",
            "export_result",
            Some(result.id.to_string()),
            HashMap::new(),
            AuditOutcome::Success,
        ).await;

        Ok(result)
    }

    /// Create a law enforcement request
    pub async fn create_law_enforcement_request(
        &self,
        agency: String,
        officer_name: String,
        badge_number: String,
        case_number: String,
        legal_basis: String,
        request_type: LawEnforcementRequestType,
        urgency: Urgency,
        target_user: Option<String>,
        target_content: Option<String>,
        date_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
        specific_evidence: Vec<String>,
        legal_documents: Vec<String>,
    ) -> Result<LawEnforcementRequest> {
        let request = LawEnforcementRequest {
            id: Uuid::new_v4(),
            agency,
            officer_name,
            badge_number,
            case_number,
            legal_basis,
            request_type,
            urgency,
            target_user,
            target_content,
            date_range,
            specific_evidence,
            legal_documents,
            status: LawEnforcementRequestStatus::Received,
            assigned_to: None,
            notes: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            completed_at: None,
        };

        // Log the law enforcement request
        self.log_audit_event(
            None,
            "law_enforcement_request_received",
            "law_enforcement_request",
            Some(request.id.to_string()),
            HashMap::new(),
            AuditOutcome::Success,
        ).await;

        self.law_enforcement_requests.write().await.insert(request.id, request.clone());
        Ok(request)
    }

    /// Process a law enforcement request
    pub async fn process_law_enforcement_request(
        &self,
        request_id: Uuid,
        assigned_to: String,
        notes: String,
    ) -> Result<()> {
        let mut requests = self.law_enforcement_requests.write().await;
        let request = requests.get_mut(&request_id)
            .ok_or_else(|| anyhow::anyhow!("Law enforcement request not found"))?;

        request.assigned_to = Some(assigned_to.clone());
        request.status = LawEnforcementRequestStatus::UnderReview;
        request.updated_at = Utc::now();

        // Add note
        let note = LawEnforcementNote {
            id: Uuid::new_v4(),
            author_id: assigned_to,
            content: notes,
            is_internal: true,
            created_at: Utc::now(),
        };
        request.notes.push(note);

        // Log the processing
        self.log_audit_event(
            Some(assigned_to),
            "law_enforcement_request_processed",
            "law_enforcement_request",
            Some(request_id.to_string()),
            HashMap::new(),
            AuditOutcome::Success,
        ).await;

        Ok(())
    }

    /// Generate compliance report
    pub async fn generate_compliance_report(
        &self,
        report_type: ComplianceReportType,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
        generated_by: String,
    ) -> Result<ComplianceReport> {
        let data = self.collect_compliance_data(period_start, period_end).await?;
        let summary = self.generate_compliance_summary(&data).await?;

        let report = ComplianceReport {
            id: Uuid::new_v4(),
            report_type,
            period_start,
            period_end,
            generated_at: Utc::now(),
            generated_by,
            data,
            summary,
            attachments: Vec::new(),
        };

        self.compliance_reports.write().await.insert(report.id, report.clone());

        // Log report generation
        self.log_audit_event(
            Some(generated_by),
            "compliance_report_generated",
            "compliance_report",
            Some(report.id.to_string()),
            HashMap::new(),
            AuditOutcome::Success,
        ).await;

        Ok(report)
    }

    /// Export compliance report
    pub async fn export_compliance_report(
        &self,
        report_id: Uuid,
        format: ExportFormat,
    ) -> Result<ExportResult> {
        let reports = self.compliance_reports.read().await;
        let report = reports.get(&report_id)
            .ok_or_else(|| anyhow::anyhow!("Compliance report not found"))?;

        // Create export request for the report
        let export_request = ExportRequest {
            id: Uuid::new_v4(),
            requester_id: "compliance_system".to_string(),
            requester_type: RequesterType::InternalAudit,
            export_scope: ExportScope::Custom(HashMap::new()),
            format,
            include_metadata: true,
            include_evidence: false,
            include_classifications: false,
            include_signals: false,
            include_reports: false,
            include_audit_logs: false,
            encryption_required: false,
            legal_basis: "Internal compliance reporting".to_string(),
            urgency: Urgency::Low,
            notes: Some(format!("Export of compliance report {}", report_id)),
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::days(90),
            status: ExportStatus::Pending,
        };

        self.export_requests.write().await.insert(export_request.id, export_request.clone());
        self.process_export_request(export_request.id).await
    }

    /// Get all export requests
    pub async fn get_export_requests(&self) -> Vec<ExportRequest> {
        self.export_requests.read().await.values().cloned().collect()
    }

    /// Get export request by ID
    pub async fn get_export_request(&self, id: Uuid) -> Option<ExportRequest> {
        self.export_requests.read().await.get(&id).cloned()
    }

    /// Get export result by ID
    pub async fn get_export_result(&self, id: Uuid) -> Option<ExportResult> {
        self.export_results.read().await.get(&id).cloned()
    }

    /// Get all law enforcement requests
    pub async fn get_law_enforcement_requests(&self) -> Vec<LawEnforcementRequest> {
        self.law_enforcement_requests.read().await.values().cloned().collect()
    }

    /// Get law enforcement request by ID
    pub async fn get_law_enforcement_request(&self, id: Uuid) -> Option<LawEnforcementRequest> {
        self.law_enforcement_requests.read().await.get(&id).cloned()
    }

    /// Get compliance reports
    pub async fn get_compliance_reports(&self) -> Vec<ComplianceReport> {
        self.compliance_reports.read().await.values().cloned().collect()
    }

    /// Get audit logs with filtering
    pub async fn get_audit_logs(
        &self,
        user_id: Option<String>,
        action: Option<String>,
        resource_type: Option<String>,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> Vec<AuditLog> {
        let logs = self.audit_logs.read().await;
        let mut filtered_logs: Vec<AuditLog> = logs.iter()
            .filter(|log| {
                if let Some(uid) = &user_id {
                    if log.user_id.as_ref() != Some(uid) {
                        return false;
                    }
                }
                if let Some(act) = &action {
                    if &log.action != act {
                        return false;
                    }
                }
                if let Some(rt) = &resource_type {
                    if &log.resource_type != rt {
                        return false;
                    }
                }
                if let Some(start) = start_date {
                    if log.timestamp < start {
                        return false;
                    }
                }
                if let Some(end) = end_date {
                    if log.timestamp > end {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        filtered_logs.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        if let Some(lim) = limit {
            filtered_logs.truncate(lim);
        }

        filtered_logs
    }

    /// Log audit event
    pub async fn log_audit_event(
        &self,
        user_id: Option<String>,
        action: &str,
        resource_type: &str,
        resource_id: Option<String>,
        details: HashMap<String, String>,
        outcome: AuditOutcome,
    ) {
        let log = AuditLog {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            user_id,
            action: action.to_string(),
            resource_type: resource_type.to_string(),
            resource_id,
            details,
            ip_address: None, // Would be set by middleware
            user_agent: None, // Would be set by middleware
            session_id: None, // Would be set by middleware
            outcome,
        };

        self.audit_logs.write().await.push(log);
    }

    // Private helper methods

    async fn generate_export_data(&self, scope: &ExportScope) -> Result<Vec<serde_json::Value>> {
        // This would integrate with the actual data stores
        // For now, return mock data
        Ok(vec![serde_json::json!({
            "export_scope": format!("{:?}", scope),
            "generated_at": Utc::now().to_rfc3339(),
            "data": "Mock export data"
        })])
    }

    async fn save_export_file(
        &self,
        request_id: Uuid,
        data: &[serde_json::Value],
        format: &ExportFormat,
    ) -> Result<String> {
        let filename = format!("export_{}_{}.{:?}", 
            request_id, 
            Utc::now().timestamp(), 
            format
        ).to_lowercase();
        
        let file_path = format!("/tmp/exports/{}", filename);
        
        // Ensure directory exists
        std::fs::create_dir_all("/tmp/exports")?;
        
        // Save file based on format
        match format {
            ExportFormat::Json => {
                let content = serde_json::to_string_pretty(data)?;
                std::fs::write(&file_path, content)?;
            }
            ExportFormat::Csv => {
                // Convert to CSV format
                let content = self.convert_to_csv(data)?;
                std::fs::write(&file_path, content)?;
            }
            ExportFormat::Excel => {
                // Convert to Excel format
                let content = self.convert_to_excel(data)?;
                std::fs::write(&file_path, content)?;
            }
            ExportFormat::Pdf => {
                // Convert to PDF format
                let content = self.convert_to_pdf(data)?;
                std::fs::write(&file_path, content)?;
            }
            ExportFormat::Xml => {
                // Convert to XML format
                let content = self.convert_to_xml(data)?;
                std::fs::write(&file_path, content)?;
            }
        }

        Ok(file_path)
    }

    async fn calculate_checksum(&self, file_path: &str) -> Result<String> {
        use sha2::{Sha256, Digest};
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut hasher = Sha256::new();
        hasher.update(&buffer);
        let result = hasher.finalize();

        Ok(format!("{:x}", result))
    }

    async fn encrypt_file(&self, file_path: &str) -> Result<String> {
        // This would implement actual file encryption
        // For now, return a mock encryption key
        Ok("mock_encryption_key_12345".to_string())
    }

    async fn generate_download_url(&self, request_id: Uuid) -> Result<String> {
        Ok(format!("/api/v1/compliance/exports/{}/download", request_id))
    }

    async fn collect_compliance_data(
        &self,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
    ) -> Result<ComplianceReportData> {
        // This would collect actual data from the system
        // For now, return mock data
        Ok(ComplianceReportData {
            total_users: 10000,
            total_content: 50000,
            total_reports: 1000,
            total_violations: 500,
            content_removed: 300,
            accounts_suspended: 50,
            accounts_terminated: 25,
            law_enforcement_requests: 10,
            regulatory_inquiries: 5,
            legal_actions: 2,
            violations_by_type: HashMap::new(),
            violations_by_severity: HashMap::new(),
            response_times: HashMap::new(),
            false_positive_rate: 0.05,
            appeal_success_rate: 0.15,
        })
    }

    async fn generate_compliance_summary(
        &self,
        data: &ComplianceReportData,
    ) -> Result<ComplianceSummary> {
        Ok(ComplianceSummary {
            key_findings: vec![
                "System operating within compliance parameters".to_string(),
                "Response times meet regulatory requirements".to_string(),
                "False positive rate below threshold".to_string(),
            ],
            risk_assessment: "Low risk".to_string(),
            recommendations: vec![
                "Continue monitoring false positive rates".to_string(),
                "Review appeal processes quarterly".to_string(),
            ],
            compliance_status: ComplianceStatus::Compliant,
            next_review_date: Utc::now() + Duration::days(90),
        })
    }

    fn convert_to_csv(&self, data: &[serde_json::Value]) -> Result<String> {
        // Simple CSV conversion
        Ok("id,data,created_at\n1,mock_data,2024-01-01\n".to_string())
    }

    fn convert_to_excel(&self, data: &[serde_json::Value]) -> Result<Vec<u8>> {
        // Mock Excel data
        Ok(b"mock_excel_data".to_vec())
    }

    fn convert_to_pdf(&self, data: &[serde_json::Value]) -> Result<Vec<u8>> {
        // Mock PDF data
        Ok(b"mock_pdf_data".to_vec())
    }

    fn convert_to_xml(&self, data: &[serde_json::Value]) -> Result<String> {
        // Simple XML conversion
        Ok("<export><data>mock_xml_data</data></export>".to_string())
    }
}