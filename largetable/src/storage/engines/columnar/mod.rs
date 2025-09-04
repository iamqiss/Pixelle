// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Columnar storage engine - analytics-optimized

use crate::storage::StorageEngine;
use crate::{Result, DocumentId, Document, LargetableError};
use async_trait::async_trait;
use arrow::array::{Array, StringArray, Int64Array, Float64Array, BooleanArray};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rkyv::{to_bytes, from_bytes};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};
use std::collections::HashMap;

/// Columnar storage engine using Apache Arrow/Parquet
pub struct ColumnarEngine {
    data_path: String,
    schema: Arc<Schema>,
    writer_props: WriterProperties,
}

impl ColumnarEngine {
    /// Create a new Columnar engine with Arrow/Parquet backend
    pub fn new() -> Result<Self> {
        Self::with_path("largetable_columnar")
    }

    /// Create Columnar engine with custom data path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data_path = path.as_ref().to_string_lossy().to_string();
        
        // Define schema for documents
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("created_at", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
            Field::new("data", DataType::Utf8, true), // JSON serialized document data
        ]));
        
        let writer_props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .set_write_batch_size(1000)
            .build();
        
        info!("Columnar Engine initialized with Arrow/Parquet backend");
        
        Ok(Self {
            data_path,
            schema,
            writer_props,
        })
    }

    /// Serialize document to JSON for columnar storage
    fn serialize_document(&self, doc: &Document) -> Result<String> {
        serde_json::to_string(doc)
            .map_err(|e| LargetableError::Serialization(format!("Failed to serialize document: {}", e)))
    }

    /// Deserialize JSON to document
    fn deserialize_document(&self, data: &str) -> Result<Document> {
        serde_json::from_str(data)
            .map_err(|e| LargetableError::Serialization(format!("Failed to deserialize document: {}", e)))
    }

    /// Get the parquet file path for a collection
    fn get_file_path(&self) -> String {
        format!("{}.parquet", self.data_path)
    }
}

#[async_trait]
impl StorageEngine for ColumnarEngine {
    async fn get(&self, id: &DocumentId) -> Result<Option<Document>> {
        let file_path = self.get_file_path();
        
        if !std::path::Path::new(&file_path).exists() {
            return Ok(None);
        }
        
        let file = std::fs::File::open(&file_path)
            .map_err(|e| LargetableError::Storage(format!("Failed to open parquet file: {}", e)))?;
        
        let builder = ParquetRecordBatchReaderBuilder::new(file)
            .map_err(|e| LargetableError::Storage(format!("Failed to create parquet reader: {}", e)))?;
        
        let reader = builder.build()
            .map_err(|e| LargetableError::Storage(format!("Failed to build parquet reader: {}", e)))?;
        
        let id_str = id.to_string();
        
        for batch in reader {
            let batch = batch.map_err(|e| LargetableError::Storage(format!("Failed to read batch: {}", e)))?;
            
            if let Some(id_array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                for i in 0..batch.num_rows() {
                    if id_array.value(i) == id_str {
                        if let Some(data_array) = batch.column(4).as_any().downcast_ref::<StringArray>() {
                            let data = data_array.value(i);
                            debug!("Retrieved document with ID: {}", id);
                            return self.deserialize_document(data).map(Some);
                        }
                    }
                }
            }
        }
        
        debug!("Document not found with ID: {}", id);
        Ok(None)
    }
    
    async fn put(&self, id: DocumentId, doc: Document) -> Result<()> {
        // For columnar storage, we need to read all existing data, add the new document,
        // and write back. This is not optimal for single document writes.
        // In a real implementation, you'd use a more sophisticated approach like Delta Lake.
        
        let file_path = self.get_file_path();
        let mut existing_docs = Vec::new();
        
        // Read existing documents
        if std::path::Path::new(&file_path).exists() {
            let file = std::fs::File::open(&file_path)
                .map_err(|e| LargetableError::Storage(format!("Failed to open parquet file: {}", e)))?;
            
            let builder = ParquetRecordBatchReaderBuilder::new(file)
                .map_err(|e| LargetableError::Storage(format!("Failed to create parquet reader: {}", e)))?;
            
            let reader = builder.build()
                .map_err(|e| LargetableError::Storage(format!("Failed to build parquet reader: {}", e)))?;
            
            for batch in reader {
                let batch = batch.map_err(|e| LargetableError::Storage(format!("Failed to read batch: {}", e)))?;
                
                if let (Some(id_array), Some(version_array), Some(created_array), Some(updated_array), Some(data_array)) = (
                    batch.column(0).as_any().downcast_ref::<StringArray>(),
                    batch.column(1).as_any().downcast_ref::<Int64Array>(),
                    batch.column(2).as_any().downcast_ref::<Int64Array>(),
                    batch.column(3).as_any().downcast_ref::<Int64Array>(),
                    batch.column(4).as_any().downcast_ref::<StringArray>(),
                ) {
                    for i in 0..batch.num_rows() {
                        let doc_id = DocumentId::parse_str(id_array.value(i))
                            .map_err(|e| LargetableError::Serialization(format!("Invalid document ID: {}", e)))?;
                        
                        let mut doc = self.deserialize_document(data_array.value(i))?;
                        doc.version = version_array.value(i) as u64;
                        doc.created_at = created_array.value(i);
                        doc.updated_at = updated_array.value(i);
                        
                        existing_docs.push((doc_id, doc));
                    }
                }
            }
        }
        
        // Add or update the document
        let doc_id_str = id.to_string();
        let mut found = false;
        for (existing_id, existing_doc) in existing_docs.iter_mut() {
            if existing_id == &id {
                *existing_doc = doc;
                found = true;
                break;
            }
        }
        
        if !found {
            existing_docs.push((id, doc));
        }
        
        // Write all documents back to parquet
        let file = std::fs::File::create(&file_path)
            .map_err(|e| LargetableError::Storage(format!("Failed to create parquet file: {}", e)))?;
        
        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(self.writer_props.clone()))
            .map_err(|e| LargetableError::Storage(format!("Failed to create parquet writer: {}", e)))?;
        
        // Convert documents to arrow arrays
        let mut ids = Vec::new();
        let mut versions = Vec::new();
        let mut created_ats = Vec::new();
        let mut updated_ats = Vec::new();
        let mut datas = Vec::new();
        
        for (doc_id, doc) in existing_docs {
            ids.push(doc_id.to_string());
            versions.push(doc.version as i64);
            created_ats.push(doc.created_at);
            updated_ats.push(doc.updated_at);
            datas.push(self.serialize_document(&doc)?);
        }
        
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(versions)),
                Arc::new(Int64Array::from(created_ats)),
                Arc::new(Int64Array::from(updated_ats)),
                Arc::new(StringArray::from(datas)),
            ],
        ).map_err(|e| LargetableError::Storage(format!("Failed to create record batch: {}", e)))?;
        
        writer.write(&batch)
            .map_err(|e| LargetableError::Storage(format!("Failed to write batch: {}", e)))?;
        
        writer.close()
            .map_err(|e| LargetableError::Storage(format!("Failed to close writer: {}", e)))?;
        
        debug!("Stored document with ID: {}", id);
        Ok(())
    }
    
    async fn delete(&self, id: &DocumentId) -> Result<bool> {
        // Similar to put, we need to read all data, remove the document, and write back
        let file_path = self.get_file_path();
        
        if !std::path::Path::new(&file_path).exists() {
            return Ok(false);
        }
        
        let mut existing_docs = Vec::new();
        let mut found = false;
        
        // Read existing documents
        let file = std::fs::File::open(&file_path)
            .map_err(|e| LargetableError::Storage(format!("Failed to open parquet file: {}", e)))?;
        
        let builder = ParquetRecordBatchReaderBuilder::new(file)
            .map_err(|e| LargetableError::Storage(format!("Failed to create parquet reader: {}", e)))?;
        
        let reader = builder.build()
            .map_err(|e| LargetableError::Storage(format!("Failed to build parquet reader: {}", e)))?;
        
        for batch in reader {
            let batch = batch.map_err(|e| LargetableError::Storage(format!("Failed to read batch: {}", e)))?;
            
            if let (Some(id_array), Some(version_array), Some(created_array), Some(updated_array), Some(data_array)) = (
                batch.column(0).as_any().downcast_ref::<StringArray>(),
                batch.column(1).as_any().downcast_ref::<Int64Array>(),
                batch.column(2).as_any().downcast_ref::<Int64Array>(),
                batch.column(3).as_any().downcast_ref::<Int64Array>(),
                batch.column(4).as_any().downcast_ref::<StringArray>(),
            ) {
                for i in 0..batch.num_rows() {
                    let doc_id = DocumentId::parse_str(id_array.value(i))
                        .map_err(|e| LargetableError::Serialization(format!("Invalid document ID: {}", e)))?;
                    
                    if doc_id == *id {
                        found = true;
                        continue; // Skip this document (delete it)
                    }
                    
                    let mut doc = self.deserialize_document(data_array.value(i))?;
                    doc.version = version_array.value(i) as u64;
                    doc.created_at = created_array.value(i);
                    doc.updated_at = updated_array.value(i);
                    
                    existing_docs.push((doc_id, doc));
                }
            }
        }
        
        if !found {
            return Ok(false);
        }
        
        // Write remaining documents back to parquet
        let file = std::fs::File::create(&file_path)
            .map_err(|e| LargetableError::Storage(format!("Failed to create parquet file: {}", e)))?;
        
        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(self.writer_props.clone()))
            .map_err(|e| LargetableError::Storage(format!("Failed to create parquet writer: {}", e)))?;
        
        // Convert documents to arrow arrays
        let mut ids = Vec::new();
        let mut versions = Vec::new();
        let mut created_ats = Vec::new();
        let mut updated_ats = Vec::new();
        let mut datas = Vec::new();
        
        for (doc_id, doc) in existing_docs {
            ids.push(doc_id.to_string());
            versions.push(doc.version as i64);
            created_ats.push(doc.created_at);
            updated_ats.push(doc.updated_at);
            datas.push(self.serialize_document(&doc)?);
        }
        
        if !ids.is_empty() {
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(Int64Array::from(versions)),
                    Arc::new(Int64Array::from(created_ats)),
                    Arc::new(Int64Array::from(updated_ats)),
                    Arc::new(StringArray::from(datas)),
                ],
            ).map_err(|e| LargetableError::Storage(format!("Failed to create record batch: {}", e)))?;
            
            writer.write(&batch)
                .map_err(|e| LargetableError::Storage(format!("Failed to write batch: {}", e)))?;
        }
        
        writer.close()
            .map_err(|e| LargetableError::Storage(format!("Failed to close writer: {}", e)))?;
        
        debug!("Deleted document with ID: {}", id);
        Ok(true)
    }
    
    async fn scan(&self, start: Option<DocumentId>, limit: usize) -> Result<Vec<(DocumentId, Document)>> {
        let file_path = self.get_file_path();
        
        if !std::path::Path::new(&file_path).exists() {
            return Ok(Vec::new());
        }
        
        let file = std::fs::File::open(&file_path)
            .map_err(|e| LargetableError::Storage(format!("Failed to open parquet file: {}", e)))?;
        
        let builder = ParquetRecordBatchReaderBuilder::new(file)
            .map_err(|e| LargetableError::Storage(format!("Failed to create parquet reader: {}", e)))?;
        
        let reader = builder.build()
            .map_err(|e| LargetableError::Storage(format!("Failed to build parquet reader: {}", e)))?;
        
        let mut results = Vec::new();
        let mut count = 0;
        let mut started = start.is_none();
        
        for batch in reader {
            let batch = batch.map_err(|e| LargetableError::Storage(format!("Failed to read batch: {}", e)))?;
            
            if let (Some(id_array), Some(version_array), Some(created_array), Some(updated_array), Some(data_array)) = (
                batch.column(0).as_any().downcast_ref::<StringArray>(),
                batch.column(1).as_any().downcast_ref::<Int64Array>(),
                batch.column(2).as_any().downcast_ref::<Int64Array>(),
                batch.column(3).as_any().downcast_ref::<Int64Array>(),
                batch.column(4).as_any().downcast_ref::<StringArray>(),
            ) {
                for i in 0..batch.num_rows() {
                    if count >= limit {
                        break;
                    }
                    
                    let doc_id = DocumentId::parse_str(id_array.value(i))
                        .map_err(|e| LargetableError::Serialization(format!("Invalid document ID: {}", e)))?;
                    
                    if !started {
                        if let Some(start_id) = start {
                            if doc_id == start_id {
                                started = true;
                            } else {
                                continue;
                            }
                        }
                    }
                    
                    let mut doc = self.deserialize_document(data_array.value(i))?;
                    doc.version = version_array.value(i) as u64;
                    doc.created_at = created_array.value(i);
                    doc.updated_at = updated_array.value(i);
                    
                    results.push((doc_id, doc));
                    count += 1;
                }
            }
        }
        
        debug!("Scanned {} documents", results.len());
        Ok(results)
    }
}