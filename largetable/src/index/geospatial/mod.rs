// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Geospatial index implementation

use crate::{Result, DocumentId, Document, LargetableError, IndexType, IndexQuery, IndexStats};
use crate::index::Index;
use crate::document::DocumentUtils;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// Geospatial index for location-based queries
pub struct GeospatialIndex {
    field: String,
    coordinate_system: String,
    points: Arc<RwLock<HashMap<DocumentId, (f64, f64)>>>,
}

impl GeospatialIndex {
    /// Create a new geospatial index
    pub fn new(field: String, coordinate_system: String) -> Self {
        Self {
            field,
            coordinate_system,
            points: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Extract coordinates from a document field
    fn extract_coordinates(&self, doc: &Document) -> Option<(f64, f64)> {
        DocumentUtils::get_field(doc, &self.field).and_then(|value| match value {
            crate::Value::Document(geo_doc) => {
                let lon = geo_doc.fields.get("longitude")
                    .and_then(|v| match v {
                        crate::Value::Float64(f) => Some(*f),
                        crate::Value::Int64(i) => Some(*i as f64),
                        _ => None,
                    })?;
                let lat = geo_doc.fields.get("latitude")
                    .and_then(|v| match v {
                        crate::Value::Float64(f) => Some(*f),
                        crate::Value::Int64(i) => Some(*i as f64),
                        _ => None,
                    })?;
                Some((lon, lat))
            }
            crate::Value::Array(coords) if coords.len() == 2 => {
                let lon = match &coords[0] {
                    crate::Value::Float64(f) => *f,
                    crate::Value::Int64(i) => *i as f64,
                    _ => return None,
                };
                let lat = match &coords[1] {
                    crate::Value::Float64(f) => *f,
                    crate::Value::Int64(i) => *i as f64,
                    _ => return None,
                };
                Some((lon, lat))
            }
            _ => None,
        })
    }

    /// Calculate distance between two points using Haversine formula
    fn haversine_distance(&self, (lon1, lat1): (f64, f64), (lon2, lat2): (f64, f64)) -> f64 {
        const EARTH_RADIUS_KM: f64 = 6371.0;
        
        let lat1_rad = lat1.to_radians();
        let lat2_rad = lat2.to_radians();
        let delta_lat = (lat2 - lat1).to_radians();
        let delta_lon = (lon2 - lon1).to_radians();
        
        let a = (delta_lat / 2.0).sin().powi(2) +
            lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().asin();
        
        EARTH_RADIUS_KM * c
    }

    /// Search for points within a radius
    async fn search_within_radius(&self, center: (f64, f64), radius: f64) -> Result<Vec<DocumentId>> {
        let points = self.points.read().await;
        let mut results = Vec::new();
        
        for (doc_id, point) in points.iter() {
            let distance = self.haversine_distance(center, *point);
            if distance <= radius {
                results.push(*doc_id);
            }
        }
        
        Ok(results)
    }
}

#[async_trait::async_trait]
impl Index for GeospatialIndex {
    async fn insert(&self, id: DocumentId, doc: &Document) -> Result<()> {
        if let Some(coordinates) = self.extract_coordinates(doc) {
            let mut points = self.points.write().await;
            points.insert(id, coordinates);
            debug!("Inserted document {} into geospatial index on field '{}'", id, self.field);
        }
        Ok(())
    }

    async fn remove(&self, id: &DocumentId) -> Result<()> {
        let mut points = self.points.write().await;
        points.remove(id);
        debug!("Removed document {} from geospatial index on field '{}'", id, self.field);
        Ok(())
    }

    async fn update(&self, id: DocumentId, old_doc: &Document, new_doc: &Document) -> Result<()> {
        // Remove old coordinates
        self.remove(&id).await?;
        
        // Insert new coordinates
        self.insert(id, new_doc).await?;
        
        debug!("Updated document {} in geospatial index on field '{}'", id, self.field);
        Ok(())
    }

    async fn search(&self, query: &IndexQuery) -> Result<Vec<DocumentId>> {
        match query {
            IndexQuery::Geospatial { field, center, radius } if field == &self.field => {
                self.search_within_radius(*center, *radius).await
            }
            _ => {
                Err(LargetableError::Index(format!(
                    "Geospatial index on field '{}' only supports geospatial queries, got: {:?}",
                    self.field, query
                )))
            }
        }
    }

    async fn stats(&self) -> Result<IndexStats> {
        let points = self.points.read().await;
        let total_entries = points.len();
        let memory_usage = std::mem::size_of_val(&*points) + 
            points.iter().map(|(k, v)| std::mem::size_of_val(k) + std::mem::size_of_val(v)).sum::<usize>();
        
        Ok(IndexStats {
            total_entries,
            memory_usage,
            index_type: IndexType::Geospatial {
                coordinate_system: self.coordinate_system.clone(),
            },
        })
    }

    fn index_type(&self) -> IndexType {
        IndexType::Geospatial {
            coordinate_system: self.coordinate_system.clone(),
        }
    }
}