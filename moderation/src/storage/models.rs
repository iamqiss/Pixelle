use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlaggedContent {
	pub id: Uuid,
	pub user_id: Uuid,
	pub content_id: Uuid,
	pub label: String,
	pub confidence: f32,
	pub created_at: DateTime<Utc>,
}

pub fn row_to_user_report(row: sqlx::postgres::PgRow) -> crate::core::reports::UserReport {
    use crate::core::reports::{UserReport, ReportType, ReportStatus, ReportPriority};
    let report_type_str: String = row.try_get("report_type").unwrap_or_else(|_| "Custom".to_string());
    let status_str: String = row.try_get("status").unwrap_or_else(|_| "Pending".to_string());
    let priority_str: String = row.try_get("priority").unwrap_or_else(|_| "Normal".to_string());
    let map_type = |s: &str| match s {
        "HateSpeech" | "hate_speech" => ReportType::HateSpeech,
        "Harassment" | "harassment" => ReportType::Harassment,
        "Violence" | "violence" => ReportType::Violence,
        "Spam" | "spam" => ReportType::Spam,
        "Misinformation" | "misinformation" => ReportType::Misinformation,
        "Copyright" | "copyright" => ReportType::Copyright,
        "Nsfw" | "nsfw" => ReportType::Nsfw,
        "ChildSafety" | "child_safety" => ReportType::ChildSafety,
        "Impersonation" | "impersonation" => ReportType::Impersonation,
        "Bot" | "bot" => ReportType::Bot,
        "Troll" | "troll" => ReportType::Troll,
        "Threats" | "threats" => ReportType::Threats,
        "ReportAbuse" | "report_abuse" => ReportType::ReportAbuse,
        "SpamReports" | "spam_reports" => ReportType::SpamReports,
        "FalseReporting" | "false_reporting" => ReportType::FalseReporting,
        other => ReportType::Custom(other.to_string()),
    };
    let map_status = |s: &str| match s {
        "Pending" | "pending" => ReportStatus::Pending,
        "UnderInvestigation" | "under_investigation" => ReportStatus::UnderInvestigation,
        "Escalated" | "escalated" => ReportStatus::Escalated,
        "Resolved" | "resolved" => ReportStatus::Resolved,
        "Dismissed" | "dismissed" => ReportStatus::Dismissed,
        "RequiresMoreInfo" | "requires_more_info" => ReportStatus::RequiresMoreInfo,
        _ => ReportStatus::Pending,
    };
    let map_priority = |s: &str| match s {
        "Low" | "low" => ReportPriority::Low,
        "Normal" | "normal" => ReportPriority::Normal,
        "High" | "high" => ReportPriority::High,
        "Critical" | "critical" => ReportPriority::Critical,
        "Urgent" | "urgent" => ReportPriority::Urgent,
        _ => ReportPriority::Normal,
    };

    UserReport {
        id: row.try_get("id").unwrap(),
        reporter_id: row.try_get("reporter_id").unwrap(),
        target_id: row.try_get("target_id").unwrap(),
        content_id: row.try_get("content_id").ok(),
        report_type: map_type(&report_type_str),
        reason: row.try_get("reason").unwrap_or_default(),
        description: row.try_get("description").ok(),
        evidence: Vec::new(),
        status: map_status(&status_str),
        priority: map_priority(&priority_str),
        assigned_specialist: row.try_get("assigned_specialist").ok(),
        created_at: row.try_get("created_at").unwrap(),
        updated_at: row.try_get("updated_at").unwrap(),
        resolved_at: row.try_get("resolved_at").ok(),
        metadata: std::collections::HashMap::new(),
    }
}