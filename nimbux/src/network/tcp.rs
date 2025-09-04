// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Custom TCP protocol module

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use serde::{Deserialize, Serialize};
use tracing::{info, error, debug, instrument};
use uuid::Uuid;

use crate::errors::{NimbuxError, Result};
use crate::storage::{StorageBackend, Object, ObjectMetadata};

/// Custom binary protocol for Nimbux TCP communication
/// 
/// Protocol Format:
/// [4 bytes: Magic] [4 bytes: Version] [4 bytes: OpCode] [8 bytes: RequestID] 
/// [4 bytes: PayloadLength] [Payload] [4 bytes: Checksum]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProtocolHeader {
    pub magic: u32,        // 0x4E494D42 ("NIMB")
    pub version: u32,      // Protocol version
    pub op_code: u32,      // Operation code
    pub request_id: u64,   // Unique request identifier
    pub payload_length: u32, // Length of payload in bytes
    pub checksum: u32,     // CRC32 checksum of payload
}

/// Operation codes for the TCP protocol
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpCode {
    PutObject = 0x01,
    GetObject = 0x02,
    DeleteObject = 0x03,
    ListObjects = 0x04,
    GetMetadata = 0x05,
    HealthCheck = 0x06,
    Stats = 0x07,
    Auth = 0x08,
    Error = 0xFF,
}

impl From<u32> for OpCode {
    fn from(value: u32) -> Self {
        match value {
            0x01 => OpCode::PutObject,
            0x02 => OpCode::GetObject,
            0x03 => OpCode::DeleteObject,
            0x04 => OpCode::ListObjects,
            0x05 => OpCode::GetMetadata,
            0x06 => OpCode::HealthCheck,
            0x07 => OpCode::Stats,
            0x08 => OpCode::Auth,
            _ => OpCode::Error,
        }
    }
}

/// TCP protocol request/response structures
#[derive(Debug, Serialize, Deserialize)]
pub struct TcpRequest {
    pub bucket: String,
    pub key: String,
    pub data: Option<Vec<u8>>,
    pub metadata: Option<ObjectMetadata>,
    pub auth_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TcpResponse {
    pub success: bool,
    pub data: Option<Vec<u8>>,
    pub metadata: Option<ObjectMetadata>,
    pub error: Option<String>,
    pub objects: Option<Vec<Object>>,
}

/// High-performance TCP server for Nimbux
pub struct TcpServer {
    storage: Arc<dyn StorageBackend>,
    port: u16,
    max_connections: usize,
}

impl TcpServer {
    /// Create a new TCP server
    pub fn new(storage: Arc<dyn StorageBackend>, port: u16) -> Self {
        Self {
            storage,
            port,
            max_connections: 1000,
        }
    }

    /// Set maximum concurrent connections
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Start the TCP server
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .map_err(|e| NimbuxError::Network(format!("Failed to bind TCP port {}: {}", self.port, e)))?;

        info!("TCP server listening on port {}", self.port);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_connections));

        loop {
            let (stream, addr) = listener.accept().await
                .map_err(|e| NimbuxError::Network(format!("Failed to accept connection: {}", e)))?;

            debug!("New TCP connection from {}", addr);

            let storage = Arc::clone(&self.storage);
            let permit = semaphore.clone().acquire_owned().await
                .map_err(|e| NimbuxError::Network(format!("Failed to acquire semaphore: {}", e)))?;

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(stream, storage).await {
                    error!("Error handling TCP connection from {}: {}", addr, e);
                }
                drop(permit);
            });
        }
    }

    /// Handle individual TCP connection
    #[instrument(skip(stream, storage))]
    async fn handle_connection(
        mut stream: TcpStream,
        storage: Arc<dyn StorageBackend>,
    ) -> Result<()> {
        loop {
            // Read protocol header
            let header = Self::read_header(&mut stream).await?;
            debug!("Received TCP request: {:?}", header);

            // Read payload
            let mut payload = vec![0u8; header.payload_length as usize];
            stream.read_exact(&mut payload).await
                .map_err(|e| NimbuxError::Network(format!("Failed to read payload: {}", e)))?;

            // Verify checksum
            let calculated_checksum = crc32fast::Hasher::new()
                .update(&payload)
                .finalize();
            if calculated_checksum != header.checksum {
                return Err(NimbuxError::Network("Checksum mismatch".to_string()));
            }

            // Process request
            let response = Self::process_request(header.op_code, &payload, &storage).await?;

            // Send response
            Self::send_response(&mut stream, &response).await?;
        }
    }

    /// Read protocol header from stream
    async fn read_header(stream: &mut TcpStream) -> Result<ProtocolHeader> {
        let mut header_bytes = [0u8; 28]; // Total header size
        stream.read_exact(&mut header_bytes).await
            .map_err(|e| NimbuxError::Network(format!("Failed to read header: {}", e)))?;

        let magic = u32::from_be_bytes([header_bytes[0], header_bytes[1], header_bytes[2], header_bytes[3]]);
        if magic != 0x4E494D42 { // "NIMB"
            return Err(NimbuxError::Network("Invalid magic number".to_string()));
        }

        let version = u32::from_be_bytes([header_bytes[4], header_bytes[5], header_bytes[6], header_bytes[7]]);
        let op_code = u32::from_be_bytes([header_bytes[8], header_bytes[9], header_bytes[10], header_bytes[11]]);
        let request_id = u64::from_be_bytes([
            header_bytes[12], header_bytes[13], header_bytes[14], header_bytes[15],
            header_bytes[16], header_bytes[17], header_bytes[18], header_bytes[19],
        ]);
        let payload_length = u32::from_be_bytes([header_bytes[20], header_bytes[21], header_bytes[22], header_bytes[23]]);
        let checksum = u32::from_be_bytes([header_bytes[24], header_bytes[25], header_bytes[26], header_bytes[27]]);

        Ok(ProtocolHeader {
            magic,
            version,
            op_code,
            request_id,
            payload_length,
            checksum,
        })
    }

    /// Process incoming request based on operation code
    async fn process_request(
        op_code: u32,
        payload: &[u8],
        storage: &Arc<dyn StorageBackend>,
    ) -> Result<TcpResponse> {
        let op = OpCode::from(op_code);
        
        match op {
            OpCode::PutObject => {
                let request: TcpRequest = serde_json::from_slice(payload)
                    .map_err(|e| NimbuxError::Serialization(format!("Failed to deserialize request: {}", e)))?;
                
                if let Some(data) = request.data {
                    let object = Object {
                        bucket: request.bucket.clone(),
                        key: request.key.clone(),
                        data,
                        metadata: request.metadata.unwrap_or_default(),
                        created_at: chrono::Utc::now(),
                    };
                    
                    storage.put_object(&object).await?;
                    Ok(TcpResponse {
                        success: true,
                        data: None,
                        metadata: None,
                        error: None,
                        objects: None,
                    })
                } else {
                    Ok(TcpResponse {
                        success: false,
                        data: None,
                        metadata: None,
                        error: Some("No data provided".to_string()),
                        objects: None,
                    })
                }
            }
            
            OpCode::GetObject => {
                let request: TcpRequest = serde_json::from_slice(payload)
                    .map_err(|e| NimbuxError::Serialization(format!("Failed to deserialize request: {}", e)))?;
                
                match storage.get_object(&request.bucket, &request.key).await? {
                    Some(object) => Ok(TcpResponse {
                        success: true,
                        data: Some(object.data),
                        metadata: Some(object.metadata),
                        error: None,
                        objects: None,
                    }),
                    None => Ok(TcpResponse {
                        success: false,
                        data: None,
                        metadata: None,
                        error: Some("Object not found".to_string()),
                        objects: None,
                    }),
                }
            }
            
            OpCode::DeleteObject => {
                let request: TcpRequest = serde_json::from_slice(payload)
                    .map_err(|e| NimbuxError::Serialization(format!("Failed to deserialize request: {}", e)))?;
                
                storage.delete_object(&request.bucket, &request.key).await?;
                Ok(TcpResponse {
                    success: true,
                    data: None,
                    metadata: None,
                    error: None,
                    objects: None,
                })
            }
            
            OpCode::ListObjects => {
                let request: TcpRequest = serde_json::from_slice(payload)
                    .map_err(|e| NimbuxError::Serialization(format!("Failed to deserialize request: {}", e)))?;
                
                let objects = storage.list_objects(&request.bucket, Some(&request.key)).await?;
                Ok(TcpResponse {
                    success: true,
                    data: None,
                    metadata: None,
                    error: None,
                    objects: Some(objects),
                })
            }
            
            OpCode::HealthCheck => {
                Ok(TcpResponse {
                    success: true,
                    data: Some(b"OK".to_vec()),
                    metadata: None,
                    error: None,
                    objects: None,
                })
            }
            
            OpCode::Stats => {
                let stats = storage.get_stats().await?;
                let stats_json = serde_json::to_vec(&stats)
                    .map_err(|e| NimbuxError::Serialization(format!("Failed to serialize stats: {}", e)))?;
                
                Ok(TcpResponse {
                    success: true,
                    data: Some(stats_json),
                    metadata: None,
                    error: None,
                    objects: None,
                })
            }
            
            _ => Ok(TcpResponse {
                success: false,
                data: None,
                metadata: None,
                error: Some("Unsupported operation".to_string()),
                objects: None,
            }),
        }
    }

    /// Send response back to client
    async fn send_response(stream: &mut TcpStream, response: &TcpResponse) -> Result<()> {
        let response_data = serde_json::to_vec(response)
            .map_err(|e| NimbuxError::Serialization(format!("Failed to serialize response: {}", e)))?;
        
        let checksum = crc32fast::Hasher::new()
            .update(&response_data)
            .finalize();
        
        // Create response header
        let header = ProtocolHeader {
            magic: 0x4E494D42, // "NIMB"
            version: 1,
            op_code: OpCode::Error as u32, // Will be overridden based on success
            request_id: Uuid::new_v4().as_u128() as u64,
            payload_length: response_data.len() as u32,
            checksum,
        };
        
        // Write header
        let mut header_bytes = Vec::new();
        header_bytes.extend_from_slice(&header.magic.to_be_bytes());
        header_bytes.extend_from_slice(&header.version.to_be_bytes());
        header_bytes.extend_from_slice(&header.op_code.to_be_bytes());
        header_bytes.extend_from_slice(&header.request_id.to_be_bytes());
        header_bytes.extend_from_slice(&header.payload_length.to_be_bytes());
        header_bytes.extend_from_slice(&header.checksum.to_be_bytes());
        
        stream.write_all(&header_bytes).await
            .map_err(|e| NimbuxError::Network(format!("Failed to write header: {}", e)))?;
        
        // Write payload
        stream.write_all(&response_data).await
            .map_err(|e| NimbuxError::Network(format!("Failed to write payload: {}", e)))?;
        
        stream.flush().await
            .map_err(|e| NimbuxError::Network(format!("Failed to flush stream: {}", e)))?;
        
        Ok(())
    }
}