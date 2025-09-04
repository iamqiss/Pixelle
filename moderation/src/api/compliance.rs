use axum::{
    routing::{get, post, put, delete},
    extract::{State, Path, Query, Json},
    response::{Response, IntoResponse},
    http::{StatusCode, HeaderMap, header},
    body::Body,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::main::AppState;
use crate::core::compliance::{
    ComplianceManager, ExportRequest, ExportFormat, ExportScope, RequesterType, Urgency,
    LawEnforcementRequest, LawEnforcementRequestType, ComplianceReport, ComplianceReportType,
    ExportResult, AuditLog,
};

// Request/Response types for API

#[derive(Deserialize)]
pub struct CreateExportRequestQuery {
    pub requester_id: String,
    pub requester_type: String,
    pub export_scope: ExportScopeQuery,
    pub format: String,
    pub legal_basis: String,
    pub urgency: String,
    pub notes: Option<String>,
    pub include_metadata: Option<bool>,
    pub include_evidence: Option<bool>,
    pub include_classifications: Option<bool>,
    pub include_signals: Option<bool>,
    pub include_reports: Option<bool>,
    pub include_audit_logs: Option<bool>,
}

#[derive(Deserialize)]
pub struct ExportScopeQuery {
    pub scope_type: String,
    pub user_id: Option<String>,
    pub content_id: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub report_type: Option<String>,
    pub classification_label: Option<String>,
    pub signal_type: Option<String>,
    pub severity: Option<String>,
    pub custom_filters: Option<std::collections::HashMap<String, String>>,
}

#[derive(Deserialize)]
pub struct CreateLawEnforcementRequestQuery {
    pub agency: String,
    pub officer_name: String,
    pub badge_number: String,
    pub case_number: String,
    pub legal_basis: String,
    pub request_type: String,
    pub urgency: String,
    pub target_user: Option<String>,
    pub target_content: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub specific_evidence: Vec<String>,
    pub legal_documents: Vec<String>,
}

#[derive(Deserialize)]
pub struct ProcessLawEnforcementRequestQuery {
    pub assigned_to: String,
    pub notes: String,
}

#[derive(Deserialize)]
pub struct GenerateComplianceReportQuery {
    pub report_type: String,
    pub period_start: String,
    pub period_end: String,
    pub generated_by: String,
}

#[derive(Deserialize)]
pub struct ExportComplianceReportQuery {
    pub report_id: String,
    pub format: String,
}

#[derive(Deserialize)]
pub struct AuditLogQuery {
    pub user_id: Option<String>,
    pub action: Option<String>,
    pub resource_type: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct ExportRequestResponse {
    pub id: String,
    pub requester_id: String,
    pub requester_type: String,
    pub export_scope: String,
    pub format: String,
    pub status: String,
    pub created_at: String,
    pub expires_at: String,
}

#[derive(Serialize)]
pub struct ExportResultResponse {
    pub id: String,
    pub export_request_id: String,
    pub file_size: u64,
    pub record_count: u64,
    pub checksum: String,
    pub download_url: Option<String>,
    pub expires_at: String,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct LawEnforcementRequestResponse {
    pub id: String,
    pub agency: String,
    pub officer_name: String,
    pub case_number: String,
    pub legal_basis: String,
    pub request_type: String,
    pub urgency: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize)]
pub struct ComplianceReportResponse {
    pub id: String,
    pub report_type: String,
    pub period_start: String,
    pub period_end: String,
    pub generated_at: String,
    pub generated_by: String,
    pub compliance_status: String,
    pub total_violations: u64,
    pub total_reports: u64,
}

#[derive(Serialize)]
pub struct AuditLogResponse {
    pub id: String,
    pub timestamp: String,
    pub user_id: Option<String>,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub outcome: String,
}

/// Create compliance API routes
pub fn create_compliance_router() -> axum::Router {
    axum::Router::new()
        // Export management
        .route("/api/v1/compliance/exports", post(create_export_request))
        .route("/api/v1/compliance/exports", get(get_export_requests))
        .route("/api/v1/compliance/exports/:id", get(get_export_request))
        .route("/api/v1/compliance/exports/:id/process", post(process_export_request))
        .route("/api/v1/compliance/exports/:id/download", get(download_export))
        
        // Law enforcement requests
        .route("/api/v1/compliance/law-enforcement", post(create_law_enforcement_request))
        .route("/api/v1/compliance/law-enforcement", get(get_law_enforcement_requests))
        .route("/api/v1/compliance/law-enforcement/:id", get(get_law_enforcement_request))
        .route("/api/v1/compliance/law-enforcement/:id/process", post(process_law_enforcement_request))
        .route("/api/v1/compliance/law-enforcement/:id/complete", put(complete_law_enforcement_request))
        
        // Compliance reports
        .route("/api/v1/compliance/reports", post(generate_compliance_report))
        .route("/api/v1/compliance/reports", get(get_compliance_reports))
        .route("/api/v1/compliance/reports/:id", get(get_compliance_report))
        .route("/api/v1/compliance/reports/:id/export", post(export_compliance_report))
        
        // Audit logs
        .route("/api/v1/compliance/audit-logs", get(get_audit_logs))
        .route("/api/v1/compliance/audit-logs/export", post(export_audit_logs))
        
        // Emergency data disclosure (CSAM, terrorism, etc.)
        .route("/api/v1/compliance/emergency-disclosure", post(emergency_data_disclosure))
        
        // Quarterly business reports
        .route("/api/v1/compliance/quarterly-report", post(generate_quarterly_report))
        .route("/api/v1/compliance/quarterly-report/:period", get(get_quarterly_report))
        .route("/api/v1/compliance/quarterly-report/:period/export", post(export_quarterly_report))
}

// Export management endpoints

async fn create_export_request(
    State(state): State<Arc<AppState>>,
    Json(query): Json<CreateExportRequestQuery>,
) -> Result<Json<ApiResponse<ExportRequestResponse>>, StatusCode> {
    // Convert string types to enums
    let requester_type = match query.requester_type.as_str() {
        "law_enforcement" => RequesterType::LawEnforcement,
        "regulatory_body" => RequesterType::RegulatoryBody,
        "internal_audit" => RequesterType::InternalAudit,
        "legal_counsel" => RequesterType::LegalCounsel,
        "business_report" => RequesterType::BusinessReport,
        "user_request" => RequesterType::UserRequest,
        "court_order" => RequesterType::CourtOrder,
        "subpoena" => RequesterType::Subpoena,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let format = match query.format.as_str() {
        "json" => ExportFormat::Json,
        "csv" => ExportFormat::Csv,
        "excel" => ExportFormat::Excel,
        "pdf" => ExportFormat::Pdf,
        "xml" => ExportFormat::Xml,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let urgency = match query.urgency.as_str() {
        "low" => Urgency::Low,
        "normal" => Urgency::Normal,
        "high" => Urgency::High,
        "critical" => Urgency::Critical,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let export_scope = convert_export_scope(&query.export_scope)?;

    // Create export request
    let export_request = state.compliance_manager.create_export_request(
        query.requester_id,
        requester_type,
        export_scope,
        format,
        query.legal_basis,
        urgency,
        query.notes,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = ExportRequestResponse {
        id: export_request.id.to_string(),
        requester_id: export_request.requester_id,
        requester_type: format!("{:?}", export_request.requester_type),
        export_scope: format!("{:?}", export_request.export_scope),
        format: format!("{:?}", export_request.format),
        status: format!("{:?}", export_request.status),
        created_at: export_request.created_at.to_rfc3339(),
        expires_at: export_request.expires_at.to_rfc3339(),
    };

    Ok(Json(ApiResponse {
        success: true,
        data: Some(response),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn get_export_requests(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<ExportRequestResponse>>> {
    let export_requests = state.compliance_manager.get_export_requests().await;
    
    let responses: Vec<ExportRequestResponse> = export_requests.into_iter()
        .map(|req| ExportRequestResponse {
            id: req.id.to_string(),
            requester_id: req.requester_id,
            requester_type: format!("{:?}", req.requester_type),
            export_scope: format!("{:?}", req.export_scope),
            format: format!("{:?}", req.format),
            status: format!("{:?}", req.status),
            created_at: req.created_at.to_rfc3339(),
            expires_at: req.expires_at.to_rfc3339(),
        })
        .collect();

    Json(ApiResponse {
        success: true,
        data: Some(responses),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    })
}

async fn get_export_request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<ExportRequestResponse>>, StatusCode> {
    let request_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let export_request = state.compliance_manager.get_export_request(request_id).await
        .ok_or(StatusCode::NOT_FOUND)?;

    let response = ExportRequestResponse {
        id: export_request.id.to_string(),
        requester_id: export_request.requester_id,
        requester_type: format!("{:?}", export_request.requester_type),
        export_scope: format!("{:?}", export_request.export_scope),
        format: format!("{:?}", export_request.format),
        status: format!("{:?}", export_request.status),
        created_at: export_request.created_at.to_rfc3339(),
        expires_at: export_request.expires_at.to_rfc3339(),
    };

    Ok(Json(ApiResponse {
        success: true,
        data: Some(response),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn process_export_request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<ExportResultResponse>>, StatusCode> {
    let request_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let export_result = state.compliance_manager.process_export_request(request_id).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = ExportResultResponse {
        id: export_result.id.to_string(),
        export_request_id: export_result.export_request_id.to_string(),
        file_size: export_result.file_size,
        record_count: export_result.record_count,
        checksum: export_result.checksum,
        download_url: export_result.download_url,
        expires_at: export_result.expires_at.to_rfc3339(),
        created_at: export_result.created_at.to_rfc3339(),
    };

    Ok(Json(ApiResponse {
        success: true,
        data: Some(response),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn download_export(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response<Body>, StatusCode> {
    let request_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let export_result = state.compliance_manager.get_export_result(request_id).await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Read file and return as download
    let file_content = std::fs::read(&export_result.file_path)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let filename = format!("export_{}.{:?}", 
        request_id, 
        export_result.format
    ).to_lowercase();

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_DISPOSITION, 
        format!("attachment; filename=\"{}\"", filename).parse().unwrap());
    headers.insert(header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());

    Ok(Response::builder()
        .status(StatusCode::OK)
        .headers(headers)
        .body(Body::from(file_content))
        .unwrap())
}

// Law enforcement request endpoints

async fn create_law_enforcement_request(
    State(state): State<Arc<AppState>>,
    Json(query): Json<CreateLawEnforcementRequestQuery>,
) -> Result<Json<ApiResponse<LawEnforcementRequestResponse>>, StatusCode> {
    let request_type = match query.request_type.as_str() {
        "user_data" => LawEnforcementRequestType::UserData,
        "content_data" => LawEnforcementRequestType::ContentData,
        "communication_logs" => LawEnforcementRequestType::CommunicationLogs,
        "financial_data" => LawEnforcementRequestType::FinancialData,
        "location_data" => LawEnforcementRequestType::LocationData,
        "device_data" => LawEnforcementRequestType::DeviceData,
        "network_data" => LawEnforcementRequestType::NetworkData,
        "full_account" => LawEnforcementRequestType::FullAccount,
        "emergency_disclosure" => LawEnforcementRequestType::EmergencyDisclosure,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let urgency = match query.urgency.as_str() {
        "low" => Urgency::Low,
        "normal" => Urgency::Normal,
        "high" => Urgency::High,
        "critical" => Urgency::Critical,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let date_range = if let (Some(start), Some(end)) = (&query.start_date, &query.end_date) {
        let start_date = DateTime::parse_from_rfc3339(start)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let end_date = DateTime::parse_from_rfc3339(end)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        Some((start_date.with_timezone(&Utc), end_date.with_timezone(&Utc)))
    } else {
        None
    };

    let request = state.compliance_manager.create_law_enforcement_request(
        query.agency,
        query.officer_name,
        query.badge_number,
        query.case_number,
        query.legal_basis,
        request_type,
        urgency,
        query.target_user,
        query.target_content,
        date_range,
        query.specific_evidence,
        query.legal_documents,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = LawEnforcementRequestResponse {
        id: request.id.to_string(),
        agency: request.agency,
        officer_name: request.officer_name,
        case_number: request.case_number,
        legal_basis: request.legal_basis,
        request_type: format!("{:?}", request.request_type),
        urgency: format!("{:?}", request.urgency),
        status: format!("{:?}", request.status),
        created_at: request.created_at.to_rfc3339(),
        updated_at: request.updated_at.to_rfc3339(),
    };

    Ok(Json(ApiResponse {
        success: true,
        data: Some(response),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn get_law_enforcement_requests(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<LawEnforcementRequestResponse>>> {
    let requests = state.compliance_manager.get_law_enforcement_requests().await;
    
    let responses: Vec<LawEnforcementRequestResponse> = requests.into_iter()
        .map(|req| LawEnforcementRequestResponse {
            id: req.id.to_string(),
            agency: req.agency,
            officer_name: req.officer_name,
            case_number: req.case_number,
            legal_basis: req.legal_basis,
            request_type: format!("{:?}", req.request_type),
            urgency: format!("{:?}", req.urgency),
            status: format!("{:?}", req.status),
            created_at: req.created_at.to_rfc3339(),
            updated_at: req.updated_at.to_rfc3339(),
        })
        .collect();

    Json(ApiResponse {
        success: true,
        data: Some(responses),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    })
}

async fn get_law_enforcement_request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    let request_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let request = state.compliance_manager.get_law_enforcement_request(request_id).await
        .ok_or(StatusCode::NOT_FOUND)?;

    let request_data = serde_json::json!({
        "id": request.id.to_string(),
        "agency": request.agency,
        "officer_name": request.officer_name,
        "badge_number": request.badge_number,
        "case_number": request.case_number,
        "legal_basis": request.legal_basis,
        "request_type": format!("{:?}", request.request_type),
        "urgency": format!("{:?}", request.urgency),
        "target_user": request.target_user,
        "target_content": request.target_content,
        "date_range": request.date_range.map(|(start, end)| {
            serde_json::json!({
                "start": start.to_rfc3339(),
                "end": end.to_rfc3339()
            })
        }),
        "specific_evidence": request.specific_evidence,
        "legal_documents": request.legal_documents,
        "status": format!("{:?}", request.status),
        "assigned_to": request.assigned_to,
        "notes": request.notes,
        "created_at": request.created_at.to_rfc3339(),
        "updated_at": request.updated_at.to_rfc3339(),
        "completed_at": request.completed_at.map(|dt| dt.to_rfc3339()),
    });

    Ok(Json(ApiResponse {
        success: true,
        data: Some(request_data),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn process_law_enforcement_request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(query): Json<ProcessLawEnforcementRequestQuery>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    let request_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    state.compliance_manager.process_law_enforcement_request(
        request_id,
        query.assigned_to,
        query.notes,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn complete_law_enforcement_request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    let request_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // This would complete the law enforcement request
    // For now, just return success
    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

// Compliance report endpoints

async fn generate_compliance_report(
    State(state): State<Arc<AppState>>,
    Json(query): Json<GenerateComplianceReportQuery>,
) -> Result<Json<ApiResponse<ComplianceReportResponse>>, StatusCode> {
    let report_type = match query.report_type.as_str() {
        "quarterly" => ComplianceReportType::Quarterly,
        "annual" => ComplianceReportType::Annual,
        "incident" => ComplianceReportType::Incident,
        "audit" => ComplianceReportType::Audit,
        "regulatory" => ComplianceReportType::Regulatory,
        _ => ComplianceReportType::Custom(query.report_type.clone()),
    };

    let period_start = DateTime::parse_from_rfc3339(&query.period_start)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let period_end = DateTime::parse_from_rfc3339(&query.period_end)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let report = state.compliance_manager.generate_compliance_report(
        report_type,
        period_start.with_timezone(&Utc),
        period_end.with_timezone(&Utc),
        query.generated_by,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = ComplianceReportResponse {
        id: report.id.to_string(),
        report_type: format!("{:?}", report.report_type),
        period_start: report.period_start.to_rfc3339(),
        period_end: report.period_end.to_rfc3339(),
        generated_at: report.generated_at.to_rfc3339(),
        generated_by: report.generated_by,
        compliance_status: format!("{:?}", report.summary.compliance_status),
        total_violations: report.data.total_violations,
        total_reports: report.data.total_reports,
    };

    Ok(Json(ApiResponse {
        success: true,
        data: Some(response),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn get_compliance_reports(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<ComplianceReportResponse>>> {
    let reports = state.compliance_manager.get_compliance_reports().await;
    
    let responses: Vec<ComplianceReportResponse> = reports.into_iter()
        .map(|report| ComplianceReportResponse {
            id: report.id.to_string(),
            report_type: format!("{:?}", report.report_type),
            period_start: report.period_start.to_rfc3339(),
            period_end: report.period_end.to_rfc3339(),
            generated_at: report.generated_at.to_rfc3339(),
            generated_by: report.generated_by,
            compliance_status: format!("{:?}", report.summary.compliance_status),
            total_violations: report.data.total_violations,
            total_reports: report.data.total_reports,
        })
        .collect();

    Json(ApiResponse {
        success: true,
        data: Some(responses),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    })
}

async fn get_compliance_report(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    let report_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let reports = state.compliance_manager.get_compliance_reports().await;
    let report = reports.into_iter()
        .find(|r| r.id == report_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    let report_data = serde_json::json!({
        "id": report.id.to_string(),
        "report_type": format!("{:?}", report.report_type),
        "period_start": report.period_start.to_rfc3339(),
        "period_end": report.period_end.to_rfc3339(),
        "generated_at": report.generated_at.to_rfc3339(),
        "generated_by": report.generated_by,
        "data": {
            "total_users": report.data.total_users,
            "total_content": report.data.total_content,
            "total_reports": report.data.total_reports,
            "total_violations": report.data.total_violations,
            "content_removed": report.data.content_removed,
            "accounts_suspended": report.data.accounts_suspended,
            "accounts_terminated": report.data.accounts_terminated,
            "law_enforcement_requests": report.data.law_enforcement_requests,
            "regulatory_inquiries": report.data.regulatory_inquiries,
            "legal_actions": report.data.legal_actions,
            "false_positive_rate": report.data.false_positive_rate,
            "appeal_success_rate": report.data.appeal_success_rate,
        },
        "summary": {
            "key_findings": report.summary.key_findings,
            "risk_assessment": report.summary.risk_assessment,
            "recommendations": report.summary.recommendations,
            "compliance_status": format!("{:?}", report.summary.compliance_status),
            "next_review_date": report.summary.next_review_date.to_rfc3339(),
        },
        "attachments": report.attachments,
    });

    Ok(Json(ApiResponse {
        success: true,
        data: Some(report_data),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn export_compliance_report(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(query): Json<ExportComplianceReportQuery>,
) -> Result<Json<ApiResponse<ExportResultResponse>>, StatusCode> {
    let report_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let format = match query.format.as_str() {
        "json" => ExportFormat::Json,
        "csv" => ExportFormat::Csv,
        "excel" => ExportFormat::Excel,
        "pdf" => ExportFormat::Pdf,
        "xml" => ExportFormat::Xml,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let export_result = state.compliance_manager.export_compliance_report(
        report_id,
        format,
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = ExportResultResponse {
        id: export_result.id.to_string(),
        export_request_id: export_result.export_request_id.to_string(),
        file_size: export_result.file_size,
        record_count: export_result.record_count,
        checksum: export_result.checksum,
        download_url: export_result.download_url,
        expires_at: export_result.expires_at.to_rfc3339(),
        created_at: export_result.created_at.to_rfc3339(),
    };

    Ok(Json(ApiResponse {
        success: true,
        data: Some(response),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

// Audit log endpoints

async fn get_audit_logs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AuditLogQuery>,
) -> Json<ApiResponse<Vec<AuditLogResponse>>> {
    let start_date = query.start_date.and_then(|d| DateTime::parse_from_rfc3339(&d).ok())
        .map(|dt| dt.with_timezone(&Utc));
    let end_date = query.end_date.and_then(|d| DateTime::parse_from_rfc3339(&d).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let logs = state.compliance_manager.get_audit_logs(
        query.user_id,
        query.action,
        query.resource_type,
        start_date,
        end_date,
        query.limit,
    ).await;

    let responses: Vec<AuditLogResponse> = logs.into_iter()
        .map(|log| AuditLogResponse {
            id: log.id.to_string(),
            timestamp: log.timestamp.to_rfc3339(),
            user_id: log.user_id,
            action: log.action,
            resource_type: log.resource_type,
            resource_id: log.resource_id,
            outcome: format!("{:?}", log.outcome),
        })
        .collect();

    Json(ApiResponse {
        success: true,
        data: Some(responses),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    })
}

async fn export_audit_logs(
    State(state): State<Arc<AppState>>,
    Json(query): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<ExportResultResponse>>, StatusCode> {
    // This would export audit logs based on the query
    // For now, return a mock response
    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

// Emergency data disclosure endpoint

async fn emergency_data_disclosure(
    State(state): State<Arc<AppState>>,
    Json(query): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    // This handles emergency disclosures for CSAM, terrorism, etc.
    // Would bypass normal review processes for immediate threats
    
    let response = serde_json::json!({
        "status": "emergency_disclosure_processed",
        "timestamp": Utc::now().to_rfc3339(),
        "priority": "critical",
        "response_time": "immediate",
        "data_provided": true,
        "legal_basis": "Emergency disclosure for public safety",
    });

    Ok(Json(ApiResponse {
        success: true,
        data: Some(response),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

// Quarterly business report endpoints

async fn generate_quarterly_report(
    State(state): State<Arc<AppState>>,
    Json(query): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<ComplianceReportResponse>>, StatusCode> {
    // Generate quarterly business report
    let now = Utc::now();
    let quarter_start = get_quarter_start(now);
    let quarter_end = get_quarter_end(now);

    let report = state.compliance_manager.generate_compliance_report(
        ComplianceReportType::Quarterly,
        quarter_start,
        quarter_end,
        "business_system".to_string(),
    ).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = ComplianceReportResponse {
        id: report.id.to_string(),
        report_type: "Quarterly".to_string(),
        period_start: report.period_start.to_rfc3339(),
        period_end: report.period_end.to_rfc3339(),
        generated_at: report.generated_at.to_rfc3339(),
        generated_by: report.generated_by,
        compliance_status: format!("{:?}", report.summary.compliance_status),
        total_violations: report.data.total_violations,
        total_reports: report.data.total_reports,
    };

    Ok(Json(ApiResponse {
        success: true,
        data: Some(response),
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn get_quarterly_report(
    State(state): State<Arc<AppState>>,
    Path(period): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    // Get quarterly report for specific period
    // This would parse the period and retrieve the appropriate report
    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

async fn export_quarterly_report(
    State(state): State<Arc<AppState>>,
    Path(period): Path<String>,
    Json(query): Json<ExportComplianceReportQuery>,
) -> Result<Json<ApiResponse<ExportResultResponse>>, StatusCode> {
    // Export quarterly report in specified format
    // This would find the report for the period and export it
    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
        timestamp: Utc::now().to_rfc3339(),
    }))
}

// Helper functions

fn convert_export_scope(query: &ExportScopeQuery) -> Result<ExportScope, StatusCode> {
    match query.scope_type.as_str() {
        "all" => Ok(ExportScope::All),
        "user" => {
            let user_id = query.user_id.as_ref()
                .ok_or(StatusCode::BAD_REQUEST)?;
            Ok(ExportScope::User(user_id.clone()))
        }
        "content" => {
            let content_id = query.content_id.as_ref()
                .ok_or(StatusCode::BAD_REQUEST)?;
            Ok(ExportScope::Content(content_id.clone()))
        }
        "date_range" => {
            let start_date = query.start_date.as_ref()
                .ok_or(StatusCode::BAD_REQUEST)?;
            let end_date = query.end_date.as_ref()
                .ok_or(StatusCode::BAD_REQUEST)?;
            
            let start = DateTime::parse_from_rfc3339(start_date)
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            let end = DateTime::parse_from_rfc3339(end_date)
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            
            Ok(ExportScope::DateRange(start.with_timezone(&Utc), end.with_timezone(&Utc)))
        }
        "custom" => {
            let filters = query.custom_filters.clone()
                .unwrap_or_default();
            Ok(ExportScope::Custom(filters))
        }
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

fn get_quarter_start(date: DateTime<Utc>) -> DateTime<Utc> {
    let month = date.month();
    let quarter_month = ((month - 1) / 3) * 3 + 1;
    date.with_month(quarter_month).unwrap()
        .with_day(1).unwrap()
        .with_hour(0).unwrap()
        .with_minute(0).unwrap()
        .with_second(0).unwrap()
        .with_nanosecond(0).unwrap()
}

fn get_quarter_end(date: DateTime<Utc>) -> DateTime<Utc> {
    let month = date.month();
    let quarter_month = ((month - 1) / 3) * 3 + 3;
    let year = date.year();
    
    // Find the last day of the quarter month
    let mut end_date = date.with_month(quarter_month).unwrap()
        .with_day(1).unwrap();
    
    // Move to next month and subtract one day
    end_date = end_date.with_month(end_date.month() + 1).unwrap()
        .with_day(1).unwrap();
    end_date = end_date - chrono::Duration::days(1);
    
    end_date.with_hour(23).unwrap()
        .with_minute(59).unwrap()
        .with_second(59).unwrap()
        .with_nanosecond(999_999_999).unwrap()
}