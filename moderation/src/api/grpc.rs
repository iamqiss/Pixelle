use tonic::{Request, Response, Status};
use async_stream::try_stream;
use futures_core::Stream;
use std::pin::Pin;
use std::sync::Arc;

use crate::core::classifier::{ContentClassifier, ClassificationRequest, ClassificationResult, ContentType, Priority};
use crate::core::reports::{ReportManager, CreateReportRequest, UserReport};
use crate::main::AppState;

pub mod moderation_v1 {
    tonic::include_proto!("moderation.v1");
}

use moderation_v1::{
    moderation_service_server::{ModerationService, ModerationServiceServer}, 
    ClassifyRequest, 
    ClassifyResponse, 
    Classification,
    CreateReportRequest as GrpcCreateReportRequest,
    CreateReportResponse,
    GetReportRequest,
    GetReportResponse,
    Report,
    ReportType,
    ReportStatus,
    ReportPriority,
    Evidence,
    EvidenceType,
};

#[derive(Clone)]
pub struct ModerationGrpcService {
    app_state: Arc<AppState>,
}

#[tonic::async_trait]
impl ModerationService for ModerationGrpcService {
    async fn classify(&self, request: Request<ClassifyRequest>) -> Result<Response<ClassifyResponse>, Status> {
        let req = request.into_inner();
        
        // Convert gRPC request to internal format
        let classification_request = ClassificationRequest {
            content_id: req.content_id,
            user_id: req.user_id,
            text: req.text,
            content_type: ContentType::Post, // Default, could be enhanced
            language_hint: None,
            context: std::collections::HashMap::new(),
            priority: Priority::Normal,
        };

        // Use the production classifier
        let result = self.app_state.classifier.classify(classification_request).await
            .map_err(|e| Status::internal(format!("Classification failed: {}", e)))?;

        // Convert result to gRPC response
        let resp = ClassifyResponse { 
            content_id: req.content_id, 
            result: Some(Classification { 
                label: format!("{:?}", result.label), 
                confidence: result.confidence,
                language: result.language,
                processing_time_ms: result.processing_time_ms,
                model_version: result.model_version,
                signals: result.signals,
            }) 
        };
        
        Ok(Response::new(resp))
    }

    type ClassifyStreamStream = Pin<Box<dyn Stream<Item = Result<ClassifyResponse, Status>> + Send + 'static>>;

    async fn classify_stream(&self, request: Request<tonic::Streaming<ClassifyRequest>>) -> Result<Response<Self::ClassifyStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let app_state = self.app_state.clone();
        
        let output = try_stream! {
            while let Some(req) = inbound.message().await? {
                let classification_request = ClassificationRequest {
                    content_id: req.content_id,
                    user_id: req.user_id,
                    text: req.text,
                    content_type: ContentType::Post,
                    language_hint: None,
                    context: std::collections::HashMap::new(),
                    priority: Priority::Normal,
                };

                match app_state.classifier.classify(classification_request).await {
                    Ok(result) => {
                        yield ClassifyResponse { 
                            content_id: req.content_id, 
                            result: Some(Classification { 
                                label: format!("{:?}", result.label), 
                                confidence: result.confidence,
                                language: result.language,
                                processing_time_ms: result.processing_time_ms,
                                model_version: result.model_version,
                                signals: result.signals,
                            }) 
                        };
                    }
                    Err(e) => {
                        tracing::error!("Stream classification failed: {}", e);
                        // Continue processing other requests
                    }
                }
            }
        };
        
        Ok(Response::new(Box::pin(output) as Self::ClassifyStreamStream))
    }

    async fn create_report(&self, request: Request<GrpcCreateReportRequest>) -> Result<Response<CreateReportResponse>, Status> {
        let req = request.into_inner();
        
        // Convert gRPC request to internal format
        let create_request = CreateReportRequest {
            reporter_id: uuid::Uuid::parse_str(&req.reporter_id)
                .map_err(|_| Status::invalid_argument("Invalid reporter_id"))?,
            target_id: uuid::Uuid::parse_str(&req.target_id)
                .map_err(|_| Status::invalid_argument("Invalid target_id"))?,
            content_id: req.content_id,
            report_type: convert_report_type(&req.report_type)?,
            reason: req.reason,
            description: req.description,
            evidence: req.evidence.into_iter()
                .map(|e| convert_evidence(e))
                .collect(),
            metadata: std::collections::HashMap::new(),
        };

        // Create report using the report manager
        let report = self.app_state.report_manager.create_report(create_request).await
            .map_err(|e| Status::internal(format!("Failed to create report: {}", e)))?;

        // Convert to gRPC response
        let resp = CreateReportResponse {
            report_id: report.id.to_string(),
            status: format!("{:?}", report.status),
            created_at: report.created_at.to_rfc3339(),
        };

        Ok(Response::new(resp))
    }

    async fn get_report(&self, request: Request<GetReportRequest>) -> Result<Response<GetReportResponse>, Status> {
        let req = request.into_inner();
        
        let report_id = uuid::Uuid::parse_str(&req.report_id)
            .map_err(|_| Status::invalid_argument("Invalid report_id"))?;

        // Get report from the report manager
        let report = self.app_state.report_manager.get_report(report_id).await
            .ok_or_else(|| Status::not_found("Report not found"))?;

        // Convert to gRPC response
        let resp = GetReportResponse {
            report: Some(convert_report_to_grpc(report)),
        };

        Ok(Response::new(resp))
    }

    async fn get_reports_by_user(&self, request: Request<GetReportsByUserRequest>) -> Result<Response<GetReportsByUserResponse>, Status> {
        let req = request.into_inner();
        
        let user_id = uuid::Uuid::parse_str(&req.user_id)
            .map_err(|_| Status::invalid_argument("Invalid user_id"))?;

        // Get reports from the report manager
        let reports = self.app_state.report_manager.get_reports_by_user(user_id).await;

        // Convert to gRPC response
        let resp = GetReportsByUserResponse {
            reports: reports.into_iter()
                .map(convert_report_to_grpc)
                .collect(),
        };

        Ok(Response::new(resp))
    }
}

impl ModerationGrpcService {
    pub fn new(app_state: Arc<AppState>) -> Self {
        Self { app_state }
    }

    pub fn into_server(self) -> ModerationServiceServer<Self> {
        ModerationServiceServer::new(self)
    }
}

// Helper functions for converting between internal and gRPC types
fn convert_report_type(grpc_type: &str) -> Result<crate::core::reports::ReportType, Status> {
    match grpc_type {
        "hate_speech" => Ok(crate::core::reports::ReportType::HateSpeech),
        "harassment" => Ok(crate::core::reports::ReportType::Harassment),
        "violence" => Ok(crate::core::reports::ReportType::Violence),
        "spam" => Ok(crate::core::reports::ReportType::Spam),
        "misinformation" => Ok(crate::core::reports::ReportType::Misinformation),
        "copyright" => Ok(crate::core::reports::ReportType::Copyright),
        "nsfw" => Ok(crate::core::reports::ReportType::Nsfw),
        "child_safety" => Ok(crate::core::reports::ReportType::ChildSafety),
        "impersonation" => Ok(crate::core::reports::ReportType::Impersonation),
        "bot" => Ok(crate::core::reports::ReportType::Bot),
        "troll" => Ok(crate::core::reports::ReportType::Troll),
        "threats" => Ok(crate::core::reports::ReportType::Threats),
        "report_abuse" => Ok(crate::core::reports::ReportType::ReportAbuse),
        "spam_reports" => Ok(crate::core::reports::ReportType::SpamReports),
        "false_reporting" => Ok(crate::core::reports::ReportType::FalseReporting),
        _ => Ok(crate::core::reports::ReportType::Custom(grpc_type.to_string())),
    }
}

fn convert_evidence(grpc_evidence: Evidence) -> crate::core::reports::Evidence {
    crate::core::reports::Evidence {
        id: uuid::Uuid::new_v4(),
        evidence_type: match grpc_evidence.evidence_type.as_str() {
            "text" => crate::core::reports::EvidenceType::Text,
            "image" => crate::core::reports::EvidenceType::Image,
            "link" => crate::core::reports::EvidenceType::Link,
            "video" => crate::core::reports::EvidenceType::Video,
            "audio" => crate::core::reports::EvidenceType::Audio,
            "screenshot" => crate::core::reports::EvidenceType::Screenshot,
            "log" => crate::core::reports::EvidenceType::Log,
            _ => crate::core::reports::EvidenceType::Other,
        },
        content: grpc_evidence.content,
        url: grpc_evidence.url,
        screenshot: grpc_evidence.screenshot,
        metadata: std::collections::HashMap::new(),
        created_at: chrono::Utc::now(),
    }
}

fn convert_report_to_grpc(report: crate::core::reports::UserReport) -> Report {
    Report {
        id: report.id.to_string(),
        reporter_id: report.reporter_id.to_string(),
        target_id: report.target_id.to_string(),
        content_id: report.content_id.unwrap_or_default(),
        report_type: format!("{:?}", report.report_type),
        reason: report.reason,
        description: report.description.unwrap_or_default(),
        status: format!("{:?}", report.status),
        priority: format!("{:?}", report.priority),
        assigned_specialist: report.assigned_specialist.map(|id| id.to_string()),
        created_at: report.created_at.to_rfc3339(),
        updated_at: report.updated_at.to_rfc3339(),
        resolved_at: report.resolved_at.map(|dt| dt.to_rfc3339()),
    }
}

// Add missing gRPC message types
#[derive(Clone)]
pub struct GetReportsByUserRequest {
    pub user_id: String,
}

#[derive(Clone)]
pub struct GetReportsByUserResponse {
    pub reports: Vec<Report>,
}