// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Custom binary protocols for high-performance operations

use std::collections::HashMap;
use std::io::{Read, Write, Cursor};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::errors::{NimbuxError, Result};

/// Binary protocol version
pub const PROTOCOL_VERSION: u8 = 1;

/// Operation codes for the binary protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum OpCode {
    // Connection management
    Handshake = 0x01,
    Ping = 0x02,
    Pong = 0x03,
    Disconnect = 0x04,
    
    // Authentication
    Auth = 0x10,
    AuthResponse = 0x11,
    RefreshToken = 0x12,
    Logout = 0x13,
    
    // Bucket operations
    CreateBucket = 0x20,
    DeleteBucket = 0x21,
    ListBuckets = 0x22,
    GetBucketInfo = 0x23,
    UpdateBucket = 0x24,
    
    // Object operations
    PutObject = 0x30,
    GetObject = 0x31,
    DeleteObject = 0x32,
    ListObjects = 0x33,
    HeadObject = 0x34,
    CopyObject = 0x35,
    MoveObject = 0x36,
    
    // Batch operations
    BatchStart = 0x40,
    BatchOperation = 0x41,
    BatchCommit = 0x42,
    BatchAbort = 0x43,
    
    // Search operations
    SearchObjects = 0x50,
    SearchResponse = 0x51,
    GetSuggestions = 0x52,
    
    // Analytics operations
    GetMetrics = 0x60,
    GetAnalytics = 0x61,
    GetDashboard = 0x62,
    
    // Compression operations
    CompressData = 0x70,
    DecompressData = 0x71,
    AnalyzeCompression = 0x72,
    
    // Replication operations
    ReplicateObject = 0x80,
    GetReplicationStatus = 0x81,
    
    // Error responses
    Error = 0xFF,
}

/// Binary protocol header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryHeader {
    pub version: u8,
    pub op_code: OpCode,
    pub request_id: u64,
    pub payload_length: u32,
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    pub priority: Priority,
    pub flags: u16,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionType {
    None = 0x00,
    Gzip = 0x01,
    Zstd = 0x02,
    Lz4 = 0x03,
    Brotli = 0x04,
    Auto = 0xFF,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EncryptionType {
    None = 0x00,
    AES256 = 0x01,
    ChaCha20 = 0x02,
    Custom = 0xFF,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Priority {
    Low = 0x01,
    Normal = 0x02,
    High = 0x03,
    Critical = 0x04,
}

/// Binary protocol message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryMessage {
    pub header: BinaryHeader,
    pub payload: Vec<u8>,
}

/// Request/Response types for different operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinaryRequest {
    Handshake(HandshakeRequest),
    Ping(PingRequest),
    Auth(AuthRequest),
    CreateBucket(CreateBucketRequest),
    DeleteBucket(DeleteBucketRequest),
    ListBuckets(ListBucketsRequest),
    GetBucketInfo(GetBucketInfoRequest),
    UpdateBucket(UpdateBucketRequest),
    PutObject(PutObjectRequest),
    GetObject(GetObjectRequest),
    DeleteObject(DeleteObjectRequest),
    ListObjects(ListObjectsRequest),
    HeadObject(HeadObjectRequest),
    CopyObject(CopyObjectRequest),
    MoveObject(MoveObjectRequest),
    BatchStart(BatchStartRequest),
    BatchOperation(BatchOperationRequest),
    BatchCommit(BatchCommitRequest),
    BatchAbort(BatchAbortRequest),
    SearchObjects(SearchObjectsRequest),
    GetSuggestions(GetSuggestionsRequest),
    GetMetrics(GetMetricsRequest),
    GetAnalytics(GetAnalyticsRequest),
    GetDashboard(GetDashboardRequest),
    CompressData(CompressDataRequest),
    DecompressData(DecompressDataRequest),
    AnalyzeCompression(AnalyzeCompressionRequest),
    ReplicateObject(ReplicateObjectRequest),
    GetReplicationStatus(GetReplicationStatusRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinaryResponse {
    Handshake(HandshakeResponse),
    Pong(PongResponse),
    Auth(AuthResponse),
    CreateBucket(CreateBucketResponse),
    DeleteBucket(DeleteBucketResponse),
    ListBuckets(ListBucketsResponse),
    GetBucketInfo(GetBucketInfoResponse),
    UpdateBucket(UpdateBucketResponse),
    PutObject(PutObjectResponse),
    GetObject(GetObjectResponse),
    DeleteObject(DeleteObjectResponse),
    ListObjects(ListObjectsResponse),
    HeadObject(HeadObjectResponse),
    CopyObject(CopyObjectResponse),
    MoveObject(MoveObjectResponse),
    BatchStart(BatchStartResponse),
    BatchOperation(BatchOperationResponse),
    BatchCommit(BatchCommitResponse),
    BatchAbort(BatchAbortResponse),
    SearchObjects(SearchObjectsResponse),
    GetSuggestions(GetSuggestionsResponse),
    GetMetrics(GetMetricsResponse),
    GetAnalytics(GetAnalyticsResponse),
    GetDashboard(GetDashboardResponse),
    CompressData(CompressDataResponse),
    DecompressData(DecompressDataResponse),
    AnalyzeCompression(AnalyzeCompressionResponse),
    ReplicateObject(ReplicateObjectResponse),
    GetReplicationStatus(GetReplicationStatusResponse),
    Error(ErrorResponse),
}

// Request/Response structures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    pub client_version: String,
    pub client_id: String,
    pub capabilities: Vec<String>,
    pub compression_support: Vec<CompressionType>,
    pub encryption_support: Vec<EncryptionType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    pub server_version: String,
    pub server_id: String,
    pub supported_capabilities: Vec<String>,
    pub selected_compression: CompressionType,
    pub selected_encryption: EncryptionType,
    pub session_id: String,
    pub max_payload_size: u32,
    pub heartbeat_interval: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingRequest {
    pub timestamp: u64,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongResponse {
    pub timestamp: u64,
    pub server_timestamp: u64,
    pub latency_ms: u32,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthRequest {
    pub token: String,
    pub session_id: Option<String>,
    pub device_id: Option<String>,
    pub client_info: Option<ClientInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub platform: String,
    pub version: String,
    pub user_agent: String,
    pub ip_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResponse {
    pub success: bool,
    pub user_id: Option<String>,
    pub permissions: Vec<String>,
    pub expires_at: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateBucketRequest {
    pub name: String,
    pub region: Option<String>,
    pub storage_class: Option<String>,
    pub encryption: Option<EncryptionConfig>,
    pub versioning: Option<bool>,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub algorithm: String,
    pub key_id: String,
    pub key_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateBucketResponse {
    pub success: bool,
    pub bucket_id: Option<String>,
    pub created_at: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteBucketRequest {
    pub name: String,
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteBucketResponse {
    pub success: bool,
    pub deleted_at: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListBucketsRequest {
    pub prefix: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListBucketsResponse {
    pub buckets: Vec<BucketInfo>,
    pub total: u32,
    pub has_more: bool,
    pub next_offset: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub id: String,
    pub name: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub object_count: u64,
    pub total_size: u64,
    pub compressed_size: u64,
    pub compression_ratio: f64,
    pub region: String,
    pub storage_class: String,
    pub versioning_enabled: bool,
    pub encryption_enabled: bool,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBucketInfoRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBucketInfoResponse {
    pub success: bool,
    pub bucket: Option<BucketInfo>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateBucketRequest {
    pub name: String,
    pub updates: BucketUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketUpdate {
    pub storage_class: Option<String>,
    pub encryption: Option<EncryptionConfig>,
    pub versioning: Option<bool>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateBucketResponse {
    pub success: bool,
    pub updated_at: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutObjectRequest {
    pub bucket: String,
    pub key: String,
    pub data: Vec<u8>,
    pub content_type: Option<String>,
    pub metadata: HashMap<String, String>,
    pub tags: HashMap<String, String>,
    pub compression: Option<CompressionConfig>,
    pub encryption: Option<EncryptionConfig>,
    pub versioning: Option<bool>,
    pub integrity_check: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub algorithm: Option<String>,
    pub level: Option<u32>,
    pub threshold: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutObjectResponse {
    pub success: bool,
    pub object_id: Option<String>,
    pub version_id: Option<String>,
    pub etag: Option<String>,
    pub size: Option<u64>,
    pub compressed_size: Option<u64>,
    pub compression_ratio: Option<f64>,
    pub created_at: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetObjectRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub range: Option<RangeSpec>,
    pub decompress: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeSpec {
    pub start: u64,
    pub end: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetObjectResponse {
    pub success: bool,
    pub data: Option<Vec<u8>>,
    pub metadata: Option<ObjectMetadata>,
    pub content_type: Option<String>,
    pub size: Option<u64>,
    pub compressed: Option<bool>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub id: String,
    pub name: String,
    pub bucket: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub content_hash: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub version: u64,
    pub version_id: Option<String>,
    pub storage_class: String,
    pub compression: Option<CompressionInfo>,
    pub encryption: Option<EncryptionInfo>,
    pub tags: HashMap<String, String>,
    pub custom_metadata: HashMap<String, String>,
    pub access_count: u64,
    pub last_accessed: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionInfo {
    pub algorithm: String,
    pub ratio: f64,
    pub original_size: u64,
    pub compressed_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionInfo {
    pub algorithm: String,
    pub key_id: String,
    pub encrypted_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteObjectRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteObjectResponse {
    pub success: bool,
    pub deleted_at: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsRequest {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
    pub include_metadata: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsResponse {
    pub objects: Vec<ObjectInfo>,
    pub total: u32,
    pub has_more: bool,
    pub next_offset: Option<u32>,
    pub common_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub version: u64,
    pub storage_class: String,
    pub compression_ratio: Option<f64>,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadObjectRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadObjectResponse {
    pub success: bool,
    pub metadata: Option<ObjectMetadata>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyObjectRequest {
    pub source_bucket: String,
    pub source_key: String,
    pub dest_bucket: String,
    pub dest_key: String,
    pub metadata_directive: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    pub tags_directive: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyObjectResponse {
    pub success: bool,
    pub object_id: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveObjectRequest {
    pub source_bucket: String,
    pub source_key: String,
    pub dest_bucket: String,
    pub dest_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveObjectResponse {
    pub success: bool,
    pub object_id: Option<String>,
    pub moved_at: Option<u64>,
    pub error: Option<String>,
}

// Batch operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStartRequest {
    pub batch_id: String,
    pub operations: Vec<BatchOperationSpec>,
    pub fail_fast: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOperationSpec {
    pub id: String,
    pub operation: String,
    pub parameters: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStartResponse {
    pub success: bool,
    pub batch_id: Option<String>,
    pub estimated_duration: Option<u32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOperationRequest {
    pub batch_id: String,
    pub operation_id: String,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOperationResponse {
    pub success: bool,
    pub operation_id: Option<String>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommitRequest {
    pub batch_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommitResponse {
    pub success: bool,
    pub completed_operations: u32,
    pub failed_operations: u32,
    pub total_duration: u32,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchAbortRequest {
    pub batch_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchAbortResponse {
    pub success: bool,
    pub aborted_operations: u32,
    pub error: Option<String>,
}

// Search operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchObjectsRequest {
    pub query: String,
    pub filters: Vec<SearchFilter>,
    pub sort: Option<SortSpec>,
    pub pagination: Option<PaginationSpec>,
    pub facets: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchFilter {
    pub field: String,
    pub operator: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    pub field: String,
    pub order: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationSpec {
    pub page: u32,
    pub per_page: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchObjectsResponse {
    pub success: bool,
    pub results: Vec<SearchResult>,
    pub total: u32,
    pub facets: Option<HashMap<String, Vec<FacetValue>>>,
    pub suggestions: Option<Vec<String>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub object_id: String,
    pub score: f64,
    pub highlights: HashMap<String, Vec<String>>,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FacetValue {
    pub value: String,
    pub count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSuggestionsRequest {
    pub query: String,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSuggestionsResponse {
    pub success: bool,
    pub suggestions: Vec<String>,
    pub error: Option<String>,
}

// Analytics operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetricsRequest {
    pub metric_names: Vec<String>,
    pub time_range: TimeRange,
    pub aggregation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetricsResponse {
    pub success: bool,
    pub metrics: HashMap<String, MetricData>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricData {
    pub name: String,
    pub points: Vec<DataPoint>,
    pub aggregation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    pub timestamp: u64,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAnalyticsRequest {
    pub dashboard_id: Option<String>,
    pub time_range: TimeRange,
    pub filters: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAnalyticsResponse {
    pub success: bool,
    pub analytics: Option<AnalyticsData>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsData {
    pub dashboard_id: String,
    pub widgets: HashMap<String, WidgetData>,
    pub generated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetData {
    pub widget_id: String,
    pub data: serde_json::Value,
    pub generated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetDashboardRequest {
    pub dashboard_id: String,
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetDashboardResponse {
    pub success: bool,
    pub dashboard: Option<DashboardData>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardData {
    pub id: String,
    pub name: String,
    pub widgets: Vec<WidgetInfo>,
    pub layout: DashboardLayout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetInfo {
    pub id: String,
    pub name: String,
    pub widget_type: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardLayout {
    pub rows: Vec<LayoutRow>,
    pub columns: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutRow {
    pub height: u32,
    pub widgets: Vec<WidgetPosition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetPosition {
    pub widget_id: String,
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

// Compression operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressDataRequest {
    pub data: Vec<u8>,
    pub algorithm: Option<String>,
    pub level: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressDataResponse {
    pub success: bool,
    pub compressed_data: Option<Vec<u8>>,
    pub original_size: Option<u64>,
    pub compressed_size: Option<u64>,
    pub compression_ratio: Option<f64>,
    pub algorithm: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecompressDataRequest {
    pub compressed_data: Vec<u8>,
    pub algorithm: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecompressDataResponse {
    pub success: bool,
    pub data: Option<Vec<u8>>,
    pub original_size: Option<u64>,
    pub decompressed_size: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeCompressionRequest {
    pub data: Vec<u8>,
    pub algorithms: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeCompressionResponse {
    pub success: bool,
    pub analysis: Option<CompressionAnalysis>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionAnalysis {
    pub original_size: u64,
    pub recommended_algorithm: String,
    pub algorithm_results: Vec<AlgorithmResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmResult {
    pub algorithm: String,
    pub compressed_size: u64,
    pub compression_ratio: f64,
    pub compression_time_ms: u64,
    pub decompression_time_ms: u64,
}

// Replication operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateObjectRequest {
    pub object_id: String,
    pub source_region: String,
    pub dest_region: String,
    pub priority: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateObjectResponse {
    pub success: bool,
    pub replication_id: Option<String>,
    pub estimated_completion: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetReplicationStatusRequest {
    pub replication_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetReplicationStatusResponse {
    pub success: bool,
    pub status: Option<ReplicationStatus>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub id: String,
    pub status: String,
    pub progress: f64,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    pub error: Option<String>,
}

// Error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub details: Option<HashMap<String, serde_json::Value>>,
    pub request_id: Option<u64>,
}

/// Binary protocol codec for encoding/decoding messages
pub struct BinaryCodec;

impl BinaryCodec {
    /// Encode a message to binary format
    pub fn encode(message: &BinaryMessage) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        
        // Encode header
        Self::encode_header(&mut buffer, &message.header)?;
        
        // Encode payload
        buffer.extend_from_slice(&message.payload);
        
        Ok(buffer)
    }
    
    /// Decode a message from binary format
    pub fn decode(data: &[u8]) -> Result<BinaryMessage> {
        let mut cursor = Cursor::new(data);
        
        // Decode header
        let header = Self::decode_header(&mut cursor)?;
        
        // Decode payload
        let payload_length = header.payload_length as usize;
        let mut payload = vec![0u8; payload_length];
        cursor.read_exact(&mut payload)?;
        
        Ok(BinaryMessage { header, payload })
    }
    
    /// Encode a request to binary format
    pub fn encode_request(request: &BinaryRequest, request_id: u64) -> Result<Vec<u8>> {
        let payload = bincode::serialize(request)
            .map_err(|e| NimbuxError::Protocol(format!("Failed to serialize request: {}", e)))?;
        
        let header = BinaryHeader {
            version: PROTOCOL_VERSION,
            op_code: Self::request_to_op_code(request),
            request_id,
            payload_length: payload.len() as u32,
            compression: CompressionType::None,
            encryption: EncryptionType::None,
            priority: Priority::Normal,
            flags: 0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        let message = BinaryMessage { header, payload };
        Self::encode(&message)
    }
    
    /// Decode a response from binary format
    pub fn decode_response(data: &[u8]) -> Result<BinaryResponse> {
        let message = Self::decode(data)?;
        
        let response: BinaryResponse = bincode::deserialize(&message.payload)
            .map_err(|e| NimbuxError::Protocol(format!("Failed to deserialize response: {}", e)))?;
        
        Ok(response)
    }
    
    // Private helper methods
    
    fn encode_header(buffer: &mut Vec<u8>, header: &BinaryHeader) -> Result<()> {
        buffer.write_all(&[header.version])?;
        buffer.write_all(&[header.op_code as u8])?;
        buffer.write_all(&header.request_id.to_le_bytes())?;
        buffer.write_all(&header.payload_length.to_le_bytes())?;
        buffer.write_all(&[header.compression as u8])?;
        buffer.write_all(&[header.encryption as u8])?;
        buffer.write_all(&[header.priority as u8])?;
        buffer.write_all(&header.flags.to_le_bytes())?;
        buffer.write_all(&header.timestamp.to_le_bytes())?;
        Ok(())
    }
    
    fn decode_header(cursor: &mut Cursor<&[u8]>) -> Result<BinaryHeader> {
        let mut version = [0u8; 1];
        cursor.read_exact(&mut version)?;
        
        let mut op_code = [0u8; 1];
        cursor.read_exact(&mut op_code)?;
        
        let mut request_id = [0u8; 8];
        cursor.read_exact(&mut request_id)?;
        
        let mut payload_length = [0u8; 4];
        cursor.read_exact(&mut payload_length)?;
        
        let mut compression = [0u8; 1];
        cursor.read_exact(&mut compression)?;
        
        let mut encryption = [0u8; 1];
        cursor.read_exact(&mut encryption)?;
        
        let mut priority = [0u8; 1];
        cursor.read_exact(&mut priority)?;
        
        let mut flags = [0u8; 2];
        cursor.read_exact(&mut flags)?;
        
        let mut timestamp = [0u8; 8];
        cursor.read_exact(&mut timestamp)?;
        
        Ok(BinaryHeader {
            version: version[0],
            op_code: unsafe { std::mem::transmute(op_code[0]) },
            request_id: u64::from_le_bytes(request_id),
            payload_length: u32::from_le_bytes(payload_length),
            compression: unsafe { std::mem::transmute(compression[0]) },
            encryption: unsafe { std::mem::transmute(encryption[0]) },
            priority: unsafe { std::mem::transmute(priority[0]) },
            flags: u16::from_le_bytes(flags),
            timestamp: u64::from_le_bytes(timestamp),
        })
    }
    
    fn request_to_op_code(request: &BinaryRequest) -> OpCode {
        match request {
            BinaryRequest::Handshake(_) => OpCode::Handshake,
            BinaryRequest::Ping(_) => OpCode::Ping,
            BinaryRequest::Auth(_) => OpCode::Auth,
            BinaryRequest::CreateBucket(_) => OpCode::CreateBucket,
            BinaryRequest::DeleteBucket(_) => OpCode::DeleteBucket,
            BinaryRequest::ListBuckets(_) => OpCode::ListBuckets,
            BinaryRequest::GetBucketInfo(_) => OpCode::GetBucketInfo,
            BinaryRequest::UpdateBucket(_) => OpCode::UpdateBucket,
            BinaryRequest::PutObject(_) => OpCode::PutObject,
            BinaryRequest::GetObject(_) => OpCode::GetObject,
            BinaryRequest::DeleteObject(_) => OpCode::DeleteObject,
            BinaryRequest::ListObjects(_) => OpCode::ListObjects,
            BinaryRequest::HeadObject(_) => OpCode::HeadObject,
            BinaryRequest::CopyObject(_) => OpCode::CopyObject,
            BinaryRequest::MoveObject(_) => OpCode::MoveObject,
            BinaryRequest::BatchStart(_) => OpCode::BatchStart,
            BinaryRequest::BatchOperation(_) => OpCode::BatchOperation,
            BinaryRequest::BatchCommit(_) => OpCode::BatchCommit,
            BinaryRequest::BatchAbort(_) => OpCode::BatchAbort,
            BinaryRequest::SearchObjects(_) => OpCode::SearchObjects,
            BinaryRequest::GetSuggestions(_) => OpCode::GetSuggestions,
            BinaryRequest::GetMetrics(_) => OpCode::GetMetrics,
            BinaryRequest::GetAnalytics(_) => OpCode::GetAnalytics,
            BinaryRequest::GetDashboard(_) => OpCode::GetDashboard,
            BinaryRequest::CompressData(_) => OpCode::CompressData,
            BinaryRequest::DecompressData(_) => OpCode::DecompressData,
            BinaryRequest::AnalyzeCompression(_) => OpCode::AnalyzeCompression,
            BinaryRequest::ReplicateObject(_) => OpCode::ReplicateObject,
            BinaryRequest::GetReplicationStatus(_) => OpCode::GetReplicationStatus,
        }
    }
}

use std::time::{SystemTime, UNIX_EPOCH};