// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Vector similarity algorithms with SIMD optimizations

use crate::{Result, VectorMetric};
use crate::engine::zero_copy::simd::SimdOps;

/// Vector similarity calculator with multiple algorithms
pub struct VectorSimilarity {
    metric: VectorMetric,
}

impl VectorSimilarity {
    pub fn new(metric: VectorMetric) -> Self {
        Self { metric }
    }

    /// Calculate similarity between two vectors
    pub fn calculate_similarity(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        if a.len() != b.len() {
            return Err(crate::LargetableError::InvalidInput(
                "Vector dimensions must match".to_string()
            ));
        }

        match self.metric {
            VectorMetric::Cosine => self.cosine_similarity(a, b),
            VectorMetric::Euclidean => self.euclidean_similarity(a, b),
            VectorMetric::Dot => self.dot_similarity(a, b),
            VectorMetric::Manhattan => self.manhattan_similarity(a, b),
        }
    }

    /// Calculate distance between two vectors
    pub fn calculate_distance(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        if a.len() != b.len() {
            return Err(crate::LargetableError::InvalidInput(
                "Vector dimensions must match".to_string()
            ));
        }

        match self.metric {
            VectorMetric::Cosine => self.cosine_distance(a, b),
            VectorMetric::Euclidean => self.euclidean_distance(a, b),
            VectorMetric::Dot => self.dot_distance(a, b),
            VectorMetric::Manhattan => self.manhattan_distance(a, b),
        }
    }

    fn cosine_similarity(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        SimdOps::cosine_similarity(a, b)
    }

    fn cosine_distance(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        let similarity = self.cosine_similarity(a, b)?;
        Ok(1.0 - similarity)
    }

    fn euclidean_similarity(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        let distance = self.euclidean_distance(a, b)?;
        Ok(1.0 / (1.0 + distance))
    }

    fn euclidean_distance(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        SimdOps::euclidean_distance(a, b)
    }

    fn dot_similarity(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        let dot_product = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>();
        Ok(dot_product / (a.len() as f32))
    }

    fn dot_distance(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        let similarity = self.dot_similarity(a, b)?;
        Ok(1.0 - similarity)
    }

    fn manhattan_similarity(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        let distance = self.manhattan_distance(a, b)?;
        Ok(1.0 / (1.0 + distance))
    }

    fn manhattan_distance(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        Ok(a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum())
    }
}

/// Batch similarity calculator for multiple vectors
pub struct BatchSimilarity {
    similarity: VectorSimilarity,
}

impl BatchSimilarity {
    pub fn new(metric: VectorMetric) -> Self {
        Self {
            similarity: VectorSimilarity::new(metric),
        }
    }

    /// Calculate similarities between a query vector and multiple vectors
    pub fn calculate_similarities(&self, query: &[f32], vectors: &[Vec<f32>]) -> Result<Vec<f32>> {
        let mut similarities = Vec::with_capacity(vectors.len());
        
        for vector in vectors {
            let similarity = self.similarity.calculate_similarity(query, vector)?;
            similarities.push(similarity);
        }

        Ok(similarities)
    }

    /// Calculate distances between a query vector and multiple vectors
    pub fn calculate_distances(&self, query: &[f32], vectors: &[Vec<f32>]) -> Result<Vec<f32>> {
        let mut distances = Vec::with_capacity(vectors.len());
        
        for vector in vectors {
            let distance = self.similarity.calculate_distance(query, vector)?;
            distances.push(distance);
        }

        Ok(distances)
    }

    /// Find the most similar vector
    pub fn find_most_similar(&self, query: &[f32], vectors: &[Vec<f32>]) -> Result<Option<(usize, f32)>> {
        let similarities = self.calculate_similarities(query, vectors)?;
        
        let mut best_index = None;
        let mut best_similarity = f32::NEG_INFINITY;

        for (i, similarity) in similarities.into_iter().enumerate() {
            if similarity > best_similarity {
                best_similarity = similarity;
                best_index = Some(i);
            }
        }

        Ok(best_index.map(|i| (i, best_similarity)))
    }

    /// Find all vectors above a similarity threshold
    pub fn find_above_threshold(&self, query: &[f32], vectors: &[Vec<f32>], threshold: f32) -> Result<Vec<(usize, f32)>> {
        let similarities = self.calculate_similarities(query, vectors)?;
        
        let mut results = Vec::new();
        for (i, similarity) in similarities.into_iter().enumerate() {
            if similarity >= threshold {
                results.push((i, similarity));
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let similarity = VectorSimilarity::new(VectorMetric::Cosine);
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        
        let sim = similarity.calculate_similarity(&a, &b).unwrap();
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_distance() {
        let similarity = VectorSimilarity::new(VectorMetric::Euclidean);
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        
        let dist = similarity.calculate_distance(&a, &b).unwrap();
        assert!((dist - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_batch_similarity() {
        let batch = BatchSimilarity::new(VectorMetric::Cosine);
        let query = vec![1.0, 0.0, 0.0];
        let vectors = vec![
            vec![1.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0],
            vec![0.0, 0.0, 1.0],
        ];
        
        let similarities = batch.calculate_similarities(&query, &vectors).unwrap();
        assert_eq!(similarities.len(), 3);
        assert!((similarities[0] - 1.0).abs() < 1e-6);
    }
}