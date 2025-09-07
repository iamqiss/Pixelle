// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Zero-copy serialization using rkyv

use crate::{Result, Document, Value, DocumentId};
use rkyv::{Archive, Serialize, Deserialize, ser::serializers::AllocSerializer};
use std::collections::HashMap;

/// Zero-copy serialization for documents
pub struct ZeroCopySerializer {
    buffer: Vec<u8>,
    position: usize,
}

impl ZeroCopySerializer {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
            position: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            position: 0,
        }
    }

    /// Serialize a document to bytes using zero-copy serialization
    pub fn serialize_document(&mut self, document: &Document) -> Result<&[u8]> {
        self.buffer.clear();
        self.position = 0;

        // Use rkyv for zero-copy serialization
        let serializer = AllocSerializer::<256>::default();
        let serialized = document.serialize(serializer)?;
        
        self.buffer.extend_from_slice(&serialized);
        Ok(&self.buffer)
    }

    /// Serialize a value to bytes
    pub fn serialize_value(&mut self, value: &Value) -> Result<&[u8]> {
        self.buffer.clear();
        self.position = 0;

        let serializer = AllocSerializer::<256>::default();
        let serialized = value.serialize(serializer)?;
        
        self.buffer.extend_from_slice(&serialized);
        Ok(&self.buffer)
    }

    /// Serialize multiple documents in batch
    pub fn serialize_documents(&mut self, documents: &[Document]) -> Result<&[u8]> {
        self.buffer.clear();
        self.position = 0;

        // Write number of documents
        self.write_usize(documents.len())?;

        // Serialize each document
        for document in documents {
            let doc_bytes = self.serialize_document(document)?;
            self.write_usize(doc_bytes.len())?;
            self.buffer.extend_from_slice(doc_bytes);
        }

        Ok(&self.buffer)
    }

    fn write_usize(&mut self, value: usize) -> Result<()> {
        let bytes = value.to_le_bytes();
        self.buffer.extend_from_slice(&bytes);
        Ok(())
    }
}

/// Zero-copy deserialization for documents
pub struct ZeroCopyDeserializer<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> ZeroCopyDeserializer<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    /// Deserialize a document from bytes
    pub fn deserialize_document(&mut self) -> Result<Document> {
        let archived = rkyv::from_bytes::<Document>(self.buffer)?;
        Ok(archived.deserialize(&mut rkyv::de::deserializers::AllocDeserializer::default())?)
    }

    /// Deserialize a value from bytes
    pub fn deserialize_value(&mut self) -> Result<Value> {
        let archived = rkyv::from_bytes::<Value>(self.buffer)?;
        Ok(archived.deserialize(&mut rkyv::de::deserializers::AllocDeserializer::default())?)
    }

    /// Deserialize multiple documents from batch
    pub fn deserialize_documents(&mut self) -> Result<Vec<Document>> {
        let count = self.read_usize()?;
        let mut documents = Vec::with_capacity(count);

        for _ in 0..count {
            let doc_len = self.read_usize()?;
            let doc_bytes = &self.buffer[self.position..self.position + doc_len];
            self.position += doc_len;

            let archived = rkyv::from_bytes::<Document>(doc_bytes)?;
            let document = archived.deserialize(&mut rkyv::de::deserializers::AllocDeserializer::default())?;
            documents.push(document);
        }

        Ok(documents)
    }

    fn read_usize(&mut self) -> Result<usize> {
        if self.position + 8 > self.buffer.len() {
            return Err(crate::LargetableError::InvalidInput("Insufficient data".to_string()));
        }

        let bytes = &self.buffer[self.position..self.position + 8];
        self.position += 8;
        Ok(usize::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

/// High-performance document compression
pub struct DocumentCompressor {
    compressor: lz4_flex::frame::FrameEncoder<std::io::Cursor<Vec<u8>>>,
}

impl DocumentCompressor {
    pub fn new() -> Result<Self> {
        Ok(Self {
            compressor: lz4_flex::frame::FrameEncoder::new(std::io::Cursor::new(Vec::new())),
        })
    }

    /// Compress a document
    pub fn compress_document(&mut self, document: &Document) -> Result<Vec<u8>> {
        let mut serializer = ZeroCopySerializer::new();
        let serialized = serializer.serialize_document(document)?;
        
        self.compressor.write_all(serialized)?;
        let compressed = self.compressor.get_ref().get_ref().clone();
        self.compressor = lz4_flex::frame::FrameEncoder::new(std::io::Cursor::new(Vec::new()));
        
        Ok(compressed)
    }

    /// Decompress a document
    pub fn decompress_document(compressed: &[u8]) -> Result<Document> {
        let mut decompressor = lz4_flex::frame::FrameDecoder::new(compressed);
        let mut decompressed = Vec::new();
        decompressor.read_to_end(&mut decompressed)?;
        
        let mut deserializer = ZeroCopyDeserializer::new(&decompressed);
        deserializer.deserialize_document()
    }
}

/// Memory-mapped file operations for zero-copy I/O
pub struct MemoryMappedFile {
    data: memmap2::Mmap,
}

impl MemoryMappedFile {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let data = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self { data })
    }

    pub fn create(path: &std::path::Path, size: usize) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(size as u64)?;
        
        let data = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self { data })
    }

    /// Read a document from memory-mapped file
    pub fn read_document(&self, offset: usize, length: usize) -> Result<Document> {
        if offset + length > self.data.len() {
            return Err(crate::LargetableError::InvalidInput("Read beyond file bounds".to_string()));
        }

        let slice = &self.data[offset..offset + length];
        let mut deserializer = ZeroCopyDeserializer::new(slice);
        deserializer.deserialize_document()
    }

    /// Write a document to memory-mapped file
    pub fn write_document(&mut self, offset: usize, document: &Document) -> Result<usize> {
        let mut serializer = ZeroCopySerializer::new();
        let serialized = serializer.serialize_document(document)?;
        
        if offset + serialized.len() > self.data.len() {
            return Err(crate::LargetableError::InvalidInput("Write beyond file bounds".to_string()));
        }

        self.data[offset..offset + serialized.len()].copy_from_slice(serialized);
        Ok(serialized.len())
    }
}

/// Batch operations for high-throughput serialization
pub struct BatchSerializer {
    serializer: ZeroCopySerializer,
    batch_size: usize,
    current_batch: Vec<Document>,
}

impl BatchSerializer {
    pub fn new(batch_size: usize) -> Self {
        Self {
            serializer: ZeroCopySerializer::with_capacity(batch_size * 1024),
            batch_size,
            current_batch: Vec::with_capacity(batch_size),
        }
    }

    /// Add a document to the current batch
    pub fn add_document(&mut self, document: Document) -> Result<Option<&[u8]>> {
        self.current_batch.push(document);
        
        if self.current_batch.len() >= self.batch_size {
            Ok(Some(self.flush_batch()?))
        } else {
            Ok(None)
        }
    }

    /// Flush the current batch and return serialized data
    pub fn flush_batch(&mut self) -> Result<&[u8]> {
        if self.current_batch.is_empty() {
            return Ok(&[]);
        }

        let result = self.serializer.serialize_documents(&self.current_batch)?;
        self.current_batch.clear();
        Ok(result)
    }

    /// Get the number of documents in the current batch
    pub fn batch_size(&self) -> usize {
        self.current_batch.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_document_serialization() {
        let mut doc = Document::new();
        doc.fields.insert("name".to_string(), Value::String("John Doe".to_string()));
        doc.fields.insert("age".to_string(), Value::Int32(30));

        let mut serializer = ZeroCopySerializer::new();
        let serialized = serializer.serialize_document(&doc).unwrap();

        let mut deserializer = ZeroCopyDeserializer::new(serialized);
        let deserialized = deserializer.deserialize_document().unwrap();

        assert_eq!(doc.id, deserialized.id);
        assert_eq!(doc.fields, deserialized.fields);
    }

    #[test]
    fn test_batch_serialization() {
        let mut batch = BatchSerializer::new(2);
        
        let mut doc1 = Document::new();
        doc1.fields.insert("name".to_string(), Value::String("Alice".to_string()));
        
        let mut doc2 = Document::new();
        doc2.fields.insert("name".to_string(), Value::String("Bob".to_string()));

        batch.add_document(doc1).unwrap();
        let result = batch.add_document(doc2).unwrap();
        assert!(result.is_some());
    }
}