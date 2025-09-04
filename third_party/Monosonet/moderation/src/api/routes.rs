use axum::{
    routing::{get, post, put, delete},
    extract::{State, Path, Query},
    Json, Router,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::main::AppState;
use crate::core::classifier::{ClassificationRequest, ContentType, Priority};
use crate::core::reports::{CreateReportRequest, ReportType};
use tokio_stream::wrappers::BroadcastStream;
use axum::response::{Sse, sse::Event};
use futures_util::StreamExt;

// Response types
#[derive(Serialize)]
struct Health { 
    status: &'static str,
    timestamp: String,
    version: &'static str,
}

#[derive(Serialize)]
struct Readiness { 
    ready: bool,
    components: std::collections::HashMap<String, ComponentStatus>,
}

#[derive(Serialize)]
struct ComponentStatus {
    status: String,
    message: Option<String>,
    last_check: String,
}

#[derive(Serialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
    timestamp: String,
}

#[derive(Deserialize)]
struct ClassifyQuery {
    content_id: String,
    user_id: String,
    text: String,
    content_type: Option<String>,
    language_hint: Option<String>,
    priority: Option<String>,
}

#[derive(Serialize)]
struct ClassificationResponse {
    content_id: String,
    label: String,
    confidence: f32,
    language: String,
    processing_time_ms: u64,
    model_version: String,
    signals: Vec<String>,
}

#[derive(Deserialize)]
struct CreateReportQuery {
    reporter_id: String,
    target_id: String,
    content_id: Option<String>,
    report_type: String,
    reason: String,
    description: Option<String>,
    evidence: Vec<EvidenceQuery>,
}

#[derive(Deserialize)]
struct EvidenceQuery {
    evidence_type: String,
    content: String,
    url: Option<String>,
    screenshot: Option<Vec<u8>>,
}

#[derive(Serialize)]
struct ReportResponse {
    id: String,
    status: String,
    priority: String,
    created_at: String,
}

#[derive(Deserialize)]
struct ListReportsQuery {
    status: Option<String>,
    priority: Option<String>,
    report_type: Option<String>,
    assignee: Option<String>,
    since: Option<String>,
    until: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Deserialize)]
struct InvestigationQuery {
    report_id: String,
    investigator_id: String,
}

#[derive(Deserialize)]
struct InvestigationNoteQuery {
    investigation_id: String,
    author_id: String,
    content: String,
    is_internal: Option<bool>,
}

pub fn create_router() -> Router {
    Router::new()
        // Health and status endpoints
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/status", get(status))
        
        // API v1 endpoints
        .route("/api/v1/classify", post(classify_content))
        .route("/api/v1/classify/batch", post(classify_batch))
        .route("/api/v1/classify/stream", post(classify_stream))
        
        // Reports endpoints
        .route("/api/v1/reports", post(create_report))
        .route("/api/v1/reports", get(list_reports))
        .route("/api/v1/reports/:id", get(get_report))
        .route("/api/v1/reports/user/:user_id", get(get_user_reports))
        .route("/api/v1/reports/:id/status", put(update_report_status))
        
        // Investigation endpoints
        .route("/api/v1/investigations", post(start_investigation))
        .route("/api/v1/investigations/:id/notes", post(add_investigation_note))
        .route("/api/v1/investigations/:id/complete", put(complete_investigation))
        
        // Metrics and analytics
        .route("/api/v1/metrics", get(get_metrics))
        .route("/api/v1/metrics/reports", get(get_report_metrics))
        .route("/api/v1/metrics/classifications", get(get_classification_metrics))
        // Streaming endpoints
        .route("/api/v1/stream/reports", get(stream_reports))
        .route("/api/v1/stream/signals", get(stream_signals))
        // Audit endpoints
        .route("/api/v1/audit", get(list_audit_events))
        
        // Admin endpoints
        .route("/api/v1/admin/signals", get(get_signals))
        .route("/api/v1/admin/signals/:id", get(get_signal))
        .route("/api/v1/admin/pipelines", get(get_pipelines))
        .route("/api/v1/admin/models", get(get_ml_models))
}

// Health check endpoint
async fn health() -> Json<Health> {
    Json(Health { 
        status: "healthy",
        timestamp: chrono::Utc::now().to_rfc3339(),
        version: env!("CARGO_PKG_VERSION"),
    })
}

// Readiness probe endpoint
async fn ready(State(state): State<Arc<AppState>>) -> Json<Readiness> {
    let mut components = std::collections::HashMap::new();
    
    // Check ML models
    match state.ml_models.get_model_info().await {
        Ok(_) => {
            components.insert("ml_models".to_string(), ComponentStatus {
                status: "healthy".to_string(),
                message: None,
                last_check: chrono::Utc::now().to_rfc3339(),
            });
        }
        Err(e) => {
            components.insert("ml_models".to_string(), ComponentStatus {
                status: "unhealthy".to_string(),
                message: Some(e.to_string()),
                last_check: chrono::Utc::now().to_rfc3339(),
            });
        }
    }
    
    // Check database
    match state.datastores.health_check().await {
        Ok(_) => {
            components.insert("database".to_string(), ComponentStatus {
                status: "healthy".to_string(),
                message: None,
                last_check: chrono::Utc::now().to_rfc3339(),
            });
        }
        Err(e) => {
            components.insert("database".to_string(), ComponentStatus {
                status: "unhealthy".to_string(),
                message: Some(e.to_string()),
                last_check: chrono::Utc::now().to_rfc3339(),
            });
        }
    }
    
    // Check Redis
    match state.datastores.redis_health_check().await {
        Ok(_) => {
            components.insert("redis".to_string(), ComponentStatus {
                status: "healthy".to_string(),
                message: None,
                last_check: chrono::Utc::now().to_rfc3339(),
            });
        }
        Err(e) => {
            components.insert("redis".to_string(), ComponentStatus {
                status: "unhealthy".to_string(),
                message: Some(e.to_string()),
                last_check: chrono::Utc::now().to_rfc3339(),
            });
        }
    }
    
    let ready = components.values().all(|c| c.status == "healthy");
    
    Json(Readiness { ready, components })
}

// Status endpoint with detailed component information
async fn status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    state.health_check().await
}

// Content classification endpoint
async fn classify_content(
    State(state): State<Arc<AppState>>,
    Json(query): Json<ClassifyQuery>,
) -> Result<Json<ApiResponse<ClassificationResponse>>, StatusCode> {
    let start_time = std::time::Instant::now();
    
    // Convert query to internal request
    let content_type = query.content_type
        .as_deref()
        .and_then(|ct| match ct {
            "post" => Some(ContentType::Post),
            "comment" => Some(ContentType::Comment),
            "message" => Some(ContentType::Message),
            "profile" => Some(ContentType::Profile),
            "media" => Some(ContentType::Media),
            "link" => Some(ContentType::Link),
            _ => None,
        })
        .unwrap_or(ContentType::Post);
    
    let priority = query.priority
        .as_deref()
        .and_then(|p| match p {
            "low" => Some(Priority::Low),
            "normal" => Some(Priority::Normal),
            "high" => Some(Priority::High),
            "critical" => Some(Priority::Critical),
            _ => None,
        })
        .unwrap_or(Priority::Normal);
    
    let classification_request = ClassificationRequest {
        content_id: query.content_id,
        user_id: query.user_id,
        text: query.text,
        content_type,
        language_hint: query.language_hint,
        context: std::collections::HashMap::new(),
        priority,
    };
    
    // Perform classification
    match state.classifier.classify(classification_request).await {
        Ok(result) => {
            let processing_time = start_time.elapsed();
            
            // Record metrics
            state.metrics.record_request(processing_time, true);
            
            let response = ClassificationResponse {
                content_id: result.content_id,
                label: format!("{:?}", result.label),
                confidence: result.confidence,
                language: result.language,
                processing_time_ms: result.processing_time_ms,
                model_version: result.model_version,
                signals: result.signals,
            };
            
            Ok(Json(ApiResponse {
                success: true,
                data: Some(response),
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        Err(e) => {
            let processing_time = start_time.elapsed();
            state.metrics.record_request(processing_time, false);
            
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Batch classification endpoint
async fn classify_batch(
    State(state): State<Arc<AppState>>,
    Json(queries): Json<Vec<ClassifyQuery>>,
) -> Result<Json<ApiResponse<Vec<ClassificationResponse>>>, StatusCode> {
    let start_time = std::time::Instant::now();
    
    // Convert queries to internal requests
    let mut requests = Vec::new();
    for query in queries {
        let content_type = query.content_type
            .as_deref()
            .and_then(|ct| match ct {
                "post" => Some(ContentType::Post),
                "comment" => Some(ContentType::Comment),
                "message" => Some(ContentType::Message),
                "profile" => Some(ContentType::Profile),
                "media" => Some(ContentType::Media),
                "link" => Some(ContentType::Link),
                _ => None,
            })
            .unwrap_or(ContentType::Post);
        
        let priority = query.priority
            .as_deref()
            .and_then(|p| match p {
                "low" => Some(Priority::Low),
                "normal" => Some(Priority::Normal),
                "high" => Some(Priority::High),
                "critical" => Some(Priority::Critical),
                _ => None,
            })
            .unwrap_or(Priority::Normal);
        
        requests.push(ClassificationRequest {
            content_id: query.content_id,
            user_id: query.user_id,
            text: query.text,
            content_type,
            language_hint: query.language_hint,
            context: std::collections::HashMap::new(),
            priority,
        });
    }
    
    // Perform batch classification
    match state.classifier.classify_batch(requests).await {
        Ok(results) => {
            let processing_time = start_time.elapsed();
            state.metrics.record_request(processing_time, true);
            
            let responses: Vec<ClassificationResponse> = results.into_iter()
                .map(|result| ClassificationResponse {
                    content_id: result.content_id,
                    label: format!("{:?}", result.label),
                    confidence: result.confidence,
                    language: result.language,
                    processing_time_ms: result.processing_time_ms,
                    model_version: result.model_version,
                    signals: result.signals,
                })
                .collect();
            
            Ok(Json(ApiResponse {
                success: true,
                data: Some(responses),
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        Err(e) => {
            let processing_time = start_time.elapsed();
            state.metrics.record_request(processing_time, false);
            
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Stream classification endpoint (placeholder for WebSocket implementation)
async fn classify_stream() -> StatusCode {
    // This would implement WebSocket streaming for real-time classification
    StatusCode::NOT_IMPLEMENTED
}

// Create report endpoint
async fn create_report(
    State(state): State<Arc<AppState>>,
    Json(query): Json<CreateReportQuery>,
) -> Result<Json<ApiResponse<ReportResponse>>, StatusCode> {
    // Convert query to internal request
    let report_type = match query.report_type.as_str() {
        "hate_speech" => ReportType::HateSpeech,
        "harassment" => ReportType::Harassment,
        "violence" => ReportType::Violence,
        "spam" => ReportType::Spam,
        "misinformation" => ReportType::Misinformation,
        "copyright" => ReportType::Copyright,
        "nsfw" => ReportType::Nsfw,
        "child_safety" => ReportType::ChildSafety,
        "impersonation" => ReportType::Impersonation,
        "bot" => ReportType::Bot,
        "troll" => ReportType::Troll,
        "threats" => ReportType::Threats,
        "report_abuse" => ReportType::ReportAbuse,
        "spam_reports" => ReportType::SpamReports,
        "false_reporting" => ReportType::FalseReporting,
        _ => ReportType::Custom(query.report_type.clone()),
    };
    
    let evidence: Vec<crate::core::reports::Evidence> = query.evidence.into_iter()
        .map(|e| {
            let evidence_type = match e.evidence_type.as_str() {
                "text" => crate::core::reports::EvidenceType::Text,
                "image" => crate::core::reports::EvidenceType::Image,
                "link" => crate::core::reports::EvidenceType::Link,
                "video" => crate::core::reports::EvidenceType::Video,
                "audio" => crate::core::reports::EvidenceType::Audio,
                "screenshot" => crate::core::reports::EvidenceType::Screenshot,
                "log" => crate::core::reports::EvidenceType::Log,
                _ => crate::core::reports::EvidenceType::Other,
            };
            
            crate::core::reports::Evidence {
                id: uuid::Uuid::new_v4(),
                evidence_type,
                content: e.content,
                url: e.url,
                screenshot: e.screenshot,
                metadata: std::collections::HashMap::new(),
                created_at: chrono::Utc::now(),
            }
        })
        .collect();
    
    let create_request = CreateReportRequest {
        reporter_id: Uuid::parse_str(&query.reporter_id)
            .map_err(|_| StatusCode::BAD_REQUEST)?,
        target_id: Uuid::parse_str(&query.target_id)
            .map_err(|_| StatusCode::BAD_REQUEST)?,
        content_id: query.content_id,
        report_type,
        reason: query.reason,
        description: query.description,
        evidence,
        metadata: std::collections::HashMap::new(),
    };
    
    // Create report
    match state.report_manager.create_report(create_request).await {
        Ok(report) => {
            let response = ReportResponse {
                id: report.id.to_string(),
                status: format!("{:?}", report.status),
                priority: format!("{:?}", report.priority),
                created_at: report.created_at.to_rfc3339(),
            };
            
            Ok(Json(ApiResponse {
                success: true,
                data: Some(response),
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        Err(e) => {
            tracing::error!("Failed to create report: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Get report endpoint
async fn get_report(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    let report_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    match state.report_manager.get_report(report_id).await {
        Some(report) => {
            let report_data = serde_json::json!({
                "id": report.id.to_string(),
                "reporter_id": report.reporter_id.to_string(),
                "target_id": report.target_id.to_string(),
                "content_id": report.content_id,
                "report_type": format!("{:?}", report.report_type),
                "reason": report.reason,
                "description": report.description,
                "status": format!("{:?}", report.status),
                "priority": format!("{:?}", report.priority),
                "assigned_specialist": report.assigned_specialist.map(|id| id.to_string()),
                "created_at": report.created_at.to_rfc3339(),
                "updated_at": report.updated_at.to_rfc3339(),
                "resolved_at": report.resolved_at.map(|dt| dt.to_rfc3339()),
            });
            
            Ok(Json(ApiResponse {
                success: true,
                data: Some(report_data),
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

// Get user reports endpoint
async fn get_user_reports(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Result<Json<ApiResponse<Vec<serde_json::Value>>>, StatusCode> {
    let user_id = Uuid::parse_str(&user_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    let reports = state.report_manager.get_reports_by_user(user_id).await;
    
    let reports_data: Vec<serde_json::Value> = reports.into_iter()
        .map(|report| serde_json::json!({
            "id": report.id.to_string(),
            "reporter_id": report.reporter_id.to_string(),
            "target_id": report.target_id.to_string(),
            "content_id": report.content_id,
            "report_type": format!("{:?}", report.report_type),
            "reason": report.reason,
            "description": report.description,
            "status": format!("{:?}", report.status),
            "priority": format!("{:?}", report.priority),
            "assigned_specialist": report.assigned_specialist.map(|id| id.to_string()),
            "created_at": report.created_at.to_rfc3339(),
            "updated_at": report.updated_at.to_rfc3339(),
            "resolved_at": report.resolved_at.map(|dt| dt.to_rfc3339()),
        }))
        .collect();
    
    Ok(Json(ApiResponse {
        success: true,
        data: Some(reports_data),
        error: None,
        timestamp: chrono::Utc::now().to_rfc3339(),
    }))
}

// List reports endpoint for queues
async fn list_reports(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListReportsQuery>,
) -> Result<Json<ApiResponse<Vec<serde_json::Value>>>, StatusCode> {
    let status = params.status.as_deref().map(|s| match s {
        "pending" => Some(crate::core::reports::ReportStatus::Pending),
        "under_investigation" => Some(crate::core::reports::ReportStatus::UnderInvestigation),
        "escalated" => Some(crate::core::reports::ReportStatus::Escalated),
        "resolved" => Some(crate::core::reports::ReportStatus::Resolved),
        "dismissed" => Some(crate::core::reports::ReportStatus::Dismissed),
        "requires_more_info" => Some(crate::core::reports::ReportStatus::RequiresMoreInfo),
        _ => None,
    }).flatten();
    let priority = params.priority.as_deref().map(|p| match p {
        "low" => Some(crate::core::reports::ReportPriority::Low),
        "normal" => Some(crate::core::reports::ReportPriority::Normal),
        "high" => Some(crate::core::reports::ReportPriority::High),
        "critical" => Some(crate::core::reports::ReportPriority::Critical),
        "urgent" => Some(crate::core::reports::ReportPriority::Urgent),
        _ => None,
    }).flatten();

    // Fetch
    // Prefer DB-backed listing for production scale
    let offset = params.offset.unwrap_or(0) as i64;
    let limit = params.limit.unwrap_or(50) as i64;
    let status_key = status.as_ref().map(|s| format!("{:?}", s));
    let priority_key = priority.as_ref().map(|p| format!("{:?}", p));
    // TODO: extend datastore query to support more filters (type, assignee, date ranges)
    let rows = state.datastores.list_user_reports(status_key, priority_key, limit, offset, None).await.unwrap_or_default();
    let data: Vec<serde_json::Value> = rows.into_iter().map(|report| serde_json::json!({
        "id": report.id.to_string(),
        "reporter_id": report.reporter_id.to_string(),
        "target_id": report.target_id.to_string(),
        "content_id": report.content_id,
        "report_type": format!("{:?}", report.report_type),
        "reason": report.reason,
        "status": format!("{:?}", report.status),
        "priority": format!("{:?}", report.priority),
        "created_at": report.created_at.to_rfc3339(),
    })).collect();

    Ok(Json(ApiResponse { success: true, data: Some(data), error: None, timestamp: chrono::Utc::now().to_rfc3339() }))
}

// Update report status endpoint
async fn update_report_status(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(status_update): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    let report_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    let new_status = status_update["status"].as_str()
        .ok_or(StatusCode::BAD_REQUEST)?;
    
    let status = match new_status {
        "pending" => crate::core::reports::ReportStatus::Pending,
        "under_investigation" => crate::core::reports::ReportStatus::UnderInvestigation,
        "escalated" => crate::core::reports::ReportStatus::Escalated,
        "resolved" => crate::core::reports::ReportStatus::Resolved,
        "dismissed" => crate::core::reports::ReportStatus::Dismissed,
        "requires_more_info" => crate::core::reports::ReportStatus::RequiresMoreInfo,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    
    match state.report_manager.update_report_status(report_id, status).await {
        Ok(_) => {
            Ok(Json(ApiResponse {
                success: true,
                data: None,
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        Err(e) => {
            tracing::error!("Failed to update report status: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Start investigation endpoint
async fn start_investigation(
    State(state): State<Arc<AppState>>,
    Json(query): Json<InvestigationQuery>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    let report_id = Uuid::parse_str(&query.report_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let investigator_id = Uuid::parse_str(&query.investigator_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    match state.report_manager.start_investigation(report_id, investigator_id).await {
        Ok(investigation) => {
            let investigation_data = serde_json::json!({
                "id": investigation.id.to_string(),
                "report_id": investigation.report_id.to_string(),
                "investigator_id": investigation.investigator_id.to_string(),
                "status": format!("{:?}", investigation.status),
                "started_at": investigation.started_at.to_rfc3339(),
            });
            
            Ok(Json(ApiResponse {
                success: true,
                data: Some(investigation_data),
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        Err(e) => {
            tracing::error!("Failed to start investigation: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Add investigation note endpoint
async fn add_investigation_note(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(query): Json<InvestigationNoteQuery>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    let investigation_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let author_id = Uuid::parse_str(&query.author_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    let note = crate::core::reports::InvestigationNote {
        id: uuid::Uuid::new_v4(),
        author_id,
        content: query.content,
        is_internal: query.is_internal.unwrap_or(false),
        created_at: chrono::Utc::now(),
    };
    
    match state.report_manager.add_investigation_note(investigation_id, note).await {
        Ok(_) => {
            let note_data = serde_json::json!({
                "id": note.id.to_string(),
                "author_id": note.author_id.to_string(),
                "content": note.content,
                "is_internal": note.is_internal,
                "created_at": note.created_at.to_rfc3339(),
            });
            
            Ok(Json(ApiResponse {
                success: true,
                data: Some(note_data),
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        Err(e) => {
            tracing::error!("Failed to add investigation note: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Complete investigation endpoint
async fn complete_investigation(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(completion): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    let investigation_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    let final_status = completion["final_status"].as_str()
        .ok_or(StatusCode::BAD_REQUEST)?;
    
    let status = match final_status {
        "not_started" => crate::core::reports::InvestigationStatus::NotStarted,
        "in_progress" => crate::core::reports::InvestigationStatus::InProgress,
        "pending_review" => crate::core::reports::InvestigationStatus::PendingReview,
        "completed" => crate::core::reports::InvestigationStatus::Completed,
        "escalated" => crate::core::reports::InvestigationStatus::Escalated,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    
    match state.report_manager.complete_investigation(investigation_id, status).await {
        Ok(_) => {
            Ok(Json(ApiResponse {
                success: true,
                data: None,
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        Err(e) => {
            tracing::error!("Failed to complete investigation: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Get metrics endpoint
async fn get_metrics(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<ApiResponse<serde_json::Value>> {
    let time_range = params.get("time_range").cloned().unwrap_or_else(|| "24h".to_string());
    
    let metrics = state.metrics.get_metrics().await;
    let alerts = state.metrics.get_alerts().await;
    
    let metrics_data = serde_json::json!({
        "time_range": time_range,
        "metrics": metrics,
        "alerts": alerts,
        "generated_at": chrono::Utc::now().to_rfc3339(),
    });
    
    Json(ApiResponse {
        success: true,
        data: Some(metrics_data),
        error: None,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

// Get report metrics endpoint
async fn get_report_metrics(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<ApiResponse<serde_json::Value>> {
    let time_range = params.get("time_range").cloned().unwrap_or_else(|| "24h".to_string());
    
    // Parse time range
    let (start, end) = match time_range.as_str() {
        "1h" => {
            let end = chrono::Utc::now();
            let start = end - chrono::Duration::hours(1);
            (start, end)
        }
        "24h" => {
            let end = chrono::Utc::now();
            let start = end - chrono::Duration::hours(24);
            (start, end)
        }
        "7d" => {
            let end = chrono::Utc::now();
            let start = end - chrono::Duration::days(7);
            (start, end)
        }
        "30d" => {
            let end = chrono::Utc::now();
            let start = end - chrono::Duration::days(30);
            (start, end)
        }
        _ => {
            let end = chrono::Utc::now();
            let start = end - chrono::Duration::hours(24);
            (start, end)
        }
    };
    
    let metrics = state.report_manager.get_report_metrics(Some((start, end))).await;
    
    let metrics_data = serde_json::json!({
        "time_range": time_range,
        "total_reports": metrics.total_reports,
        "reports_by_type": metrics.reports_by_type,
        "reports_by_status": metrics.reports_by_status,
        "reports_by_priority": metrics.reports_by_priority,
        "average_resolution_time_minutes": metrics.average_resolution_time_minutes,
        "false_report_rate": metrics.false_report_rate,
        "generated_at": chrono::Utc::now().to_rfc3339(),
    });
    
    Json(ApiResponse {
        success: true,
        data: Some(metrics_data),
        error: None,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

// Get classification metrics endpoint
async fn get_classification_metrics(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<serde_json::Value>> {
    let metrics_data = serde_json::json!({
        "requests_total": state.metrics.requests_total.get(),
        "requests_errors": state.metrics.requests_errors.get(),
        "classification_total": state.metrics.classification_total.get(),
        "ml_inference_total": state.metrics.ml_inference_total.get(),
        "cache_hits": state.metrics.cache_hits.get(),
        "cache_misses": state.metrics.cache_misses.get(),
        "generated_at": chrono::Utc::now().to_rfc3339(),
    });
    
    Json(ApiResponse {
        success: true,
        data: Some(metrics_data),
        error: None,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

// Admin endpoints
async fn get_signals(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<serde_json::Value>>> {
    let signals = state.signal_processor.get_signals(None).await;
    
    let signals_data: Vec<serde_json::Value> = signals.into_iter()
        .map(|signal| serde_json::json!({
            "id": signal.id.to_string(),
            "signal_type": format!("{:?}", signal.signal_type),
            "source": signal.source,
            "content_id": signal.content_id,
            "user_id": signal.user_id,
            "severity": format!("{:?}", signal.severity),
            "confidence": signal.confidence,
            "timestamp": signal.timestamp.to_rfc3339(),
        }))
        .collect();
    
    Json(ApiResponse {
        success: true,
        data: Some(signals_data),
        error: None,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

// Streaming: reports SSE
async fn stream_reports(State(state): State<Arc<AppState>>) -> Sse<impl futures_core::Stream<Item = Result<Event, std::convert::Infallible>>> {
    // Create a channel per-request
    let (tx, rx) = tokio::sync::broadcast::channel::<serde_json::Value>(100);
    // Attach to report manager via a simple forwarder from the global channel if available
    // For simplicity, re-use metrics alerts as heartbeat
    let stream = BroadcastStream::new(rx).filter_map(|msg| async move {
        match msg {
            Ok(value) => Some(Ok(Event::default().json_data(value).unwrap_or_else(|_| Event::default().data("{}")) )),
            Err(_) => None,
        }
    });
    Sse::new(stream)
}

// Streaming: signals SSE (placeholder)
async fn stream_signals(State(state): State<Arc<AppState>>) -> Sse<impl futures_core::Stream<Item = Result<Event, std::convert::Infallible>>> {
    // Bridge pipeline results into SSE
    let rx = state.signal_processor.subscribe_pipeline_results();
    let stream = BroadcastStream::new(rx).filter_map(|msg| async move {
        match msg {
            Ok(result) => {
                let value = serde_json::json!({
                    "type": "pipeline.result",
                    "pipeline_id": result.pipeline_id,
                    "signal_id": result.signal_id,
                    "processing_time_ms": result.processing_time_ms,
                    "timestamp": result.timestamp,
                });
                Some(Ok(Event::default().json_data(value).unwrap_or_else(|_| Event::default().data("{}"))))
            },
            Err(_) => None,
        }
    });
    Sse::new(stream)
}

#[derive(Deserialize)]
struct ListAuditQuery {
    action: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

async fn list_audit_events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListAuditQuery>,
) -> Json<ApiResponse<Vec<serde_json::Value>>> {
    let limit = params.limit.unwrap_or(100) as i64;
    let offset = params.offset.unwrap_or(0) as i64;
    let action = params.action.as_deref();
    let rows = state.datastores.list_audit_events(action, limit, offset).await.unwrap_or_default();
    Json(ApiResponse { success: true, data: Some(rows), error: None, timestamp: chrono::Utc::now().to_rfc3339() })
}

async fn get_signal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    let signal_id = Uuid::parse_str(&id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    match state.signal_processor.get_signal(signal_id).await {
        Some(signal) => {
            let signal_data = serde_json::json!({
                "id": signal.id.to_string(),
                "signal_type": format!("{:?}", signal.signal_type),
                "source": signal.source,
                "content_id": signal.content_id,
                "user_id": signal.user_id,
                "severity": format!("{:?}", signal.severity),
                "confidence": signal.confidence,
                "timestamp": signal.timestamp.to_rfc3339(),
            });
            
            Ok(Json(ApiResponse {
                success: true,
                data: Some(signal_data),
                error: None,
                timestamp: chrono::Utc::now().to_rfc3339(),
            }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn get_pipelines(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<serde_json::Value>>> {
    // This would return pipeline information
    // For now, return empty array
    Json(ApiResponse {
        success: true,
        data: Some(vec![]),
        error: None,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

async fn get_ml_models(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<serde_json::Value>>> {
    let models = state.ml_models.get_available_models().await;
    
    let models_data: Vec<serde_json::Value> = models.into_iter()
        .map(|model| serde_json::json!({
            "id": model.id,
            "name": model.name,
            "version": model.version,
            "description": model.description,
            "supported_languages": model.supported_languages,
            "model_type": format!("{:?}", model.model_type),
            "created_at": model.created_at.to_rfc3339(),
            "updated_at": model.updated_at.to_rfc3339(),
        }))
        .collect();
    
    Json(ApiResponse {
        success: true,
        data: Some(models_data),
        error: None,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}