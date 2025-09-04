// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! SIMD-optimized operations for maximum performance

use crate::{Result, Value};
use std::arch::x86_64::*;
use std::mem;

/// SIMD-optimized vector operations
pub struct SimdOps;

impl SimdOps {
    /// Compute cosine similarity between two vectors using SIMD
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32> {
        if a.len() != b.len() {
            return Err(crate::LargetableError::InvalidInput("Vector dimensions must match".to_string()));
        }

        let len = a.len();
        let mut dot_product = 0.0;
        let mut norm_a = 0.0;
        let mut norm_b = 0.0;

        // Process 8 elements at a time using AVX2
        let chunks = len / 8;
        let remainder = len % 8;

        unsafe {
            for i in 0..chunks {
                let offset = i * 8;
                let a_vec = _mm256_loadu_ps(&a[offset]);
                let b_vec = _mm256_loadu_ps(&b[offset]);

                // Compute dot product
                let dot_vec = _mm256_mul_ps(a_vec, b_vec);
                let dot_sum = _mm256_hadd_ps(dot_vec, dot_vec);
                let dot_sum = _mm256_hadd_ps(dot_sum, dot_sum);
                let dot_sum = _mm256_hadd_ps(dot_sum, dot_sum);
                dot_product += mem::transmute::<__m128, f32>(_mm256_extractf128_ps(dot_sum, 0));

                // Compute norms
                let a_squared = _mm256_mul_ps(a_vec, a_vec);
                let b_squared = _mm256_mul_ps(b_vec, b_vec);
                
                let a_sum = _mm256_hadd_ps(a_squared, a_squared);
                let a_sum = _mm256_hadd_ps(a_sum, a_sum);
                let a_sum = _mm256_hadd_ps(a_sum, a_sum);
                norm_a += mem::transmute::<__m128, f32>(_mm256_extractf128_ps(a_sum, 0));

                let b_sum = _mm256_hadd_ps(b_squared, b_squared);
                let b_sum = _mm256_hadd_ps(b_sum, b_sum);
                let b_sum = _mm256_hadd_ps(b_sum, b_sum);
                norm_b += mem::transmute::<__m128, f32>(_mm256_extractf128_ps(b_sum, 0));
            }
        }

        // Process remainder
        for i in (chunks * 8)..len {
            dot_product += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }

        if norm_a == 0.0 || norm_b == 0.0 {
            return Ok(0.0);
        }

        Ok(dot_product / (norm_a.sqrt() * norm_b.sqrt()))
    }

    /// Compute Euclidean distance between two vectors using SIMD
    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> Result<f32> {
        if a.len() != b.len() {
            return Err(crate::LargetableError::InvalidInput("Vector dimensions must match".to_string()));
        }

        let len = a.len();
        let mut sum_squared_diff = 0.0;

        // Process 8 elements at a time using AVX2
        let chunks = len / 8;
        let remainder = len % 8;

        unsafe {
            for i in 0..chunks {
                let offset = i * 8;
                let a_vec = _mm256_loadu_ps(&a[offset]);
                let b_vec = _mm256_loadu_ps(&b[offset]);

                // Compute squared difference
                let diff = _mm256_sub_ps(a_vec, b_vec);
                let squared_diff = _mm256_mul_ps(diff, diff);
                
                let sum_vec = _mm256_hadd_ps(squared_diff, squared_diff);
                let sum_vec = _mm256_hadd_ps(sum_vec, sum_vec);
                let sum_vec = _mm256_hadd_ps(sum_vec, sum_vec);
                sum_squared_diff += mem::transmute::<__m128, f32>(_mm256_extractf128_ps(sum_vec, 0));
            }
        }

        // Process remainder
        for i in (chunks * 8)..len {
            let diff = a[i] - b[i];
            sum_squared_diff += diff * diff;
        }

        Ok(sum_squared_diff.sqrt())
    }

    /// Find the maximum value in a vector using SIMD
    pub fn max_value(values: &[f32]) -> f32 {
        if values.is_empty() {
            return f32::NEG_INFINITY;
        }

        let len = values.len();
        let mut max_val = f32::NEG_INFINITY;

        // Process 8 elements at a time using AVX2
        let chunks = len / 8;
        let remainder = len % 8;

        unsafe {
            for i in 0..chunks {
                let offset = i * 8;
                let vec = _mm256_loadu_ps(&values[offset]);
                
                // Find maximum in the vector
                let max_vec = _mm256_max_ps(vec, _mm256_permute2f128_ps(vec, vec, 1));
                let max_vec = _mm256_max_ps(max_vec, _mm256_shuffle_ps(max_vec, max_vec, 0b1110));
                let max_vec = _mm256_max_ps(max_vec, _mm256_shuffle_ps(max_vec, max_vec, 0b0001));
                let max_val_vec = _mm256_max_ps(max_vec, _mm256_movehl_ps(max_vec, max_vec));
                
                let current_max = mem::transmute::<__m128, f32>(_mm256_extractf128_ps(max_val_vec, 0));
                max_val = max_val.max(current_max);
            }
        }

        // Process remainder
        for i in (chunks * 8)..len {
            max_val = max_val.max(values[i]);
        }

        max_val
    }

    /// Find the minimum value in a vector using SIMD
    pub fn min_value(values: &[f32]) -> f32 {
        if values.is_empty() {
            return f32::INFINITY;
        }

        let len = values.len();
        let mut min_val = f32::INFINITY;

        // Process 8 elements at a time using AVX2
        let chunks = len / 8;
        let remainder = len % 8;

        unsafe {
            for i in 0..chunks {
                let offset = i * 8;
                let vec = _mm256_loadu_ps(&values[offset]);
                
                // Find minimum in the vector
                let min_vec = _mm256_min_ps(vec, _mm256_permute2f128_ps(vec, vec, 1));
                let min_vec = _mm256_min_ps(min_vec, _mm256_shuffle_ps(min_vec, min_vec, 0b1110));
                let min_vec = _mm256_min_ps(min_vec, _mm256_shuffle_ps(min_vec, min_vec, 0b0001));
                let min_val_vec = _mm256_min_ps(min_vec, _mm256_movehl_ps(min_vec, min_vec));
                
                let current_min = mem::transmute::<__m128, f32>(_mm256_extractf128_ps(min_val_vec, 0));
                min_val = min_val.min(current_min);
            }
        }

        // Process remainder
        for i in (chunks * 8)..len {
            min_val = min_val.min(values[i]);
        }

        min_val
    }

    /// Compute sum of vector elements using SIMD
    pub fn sum(values: &[f32]) -> f32 {
        if values.is_empty() {
            return 0.0;
        }

        let len = values.len();
        let mut sum = 0.0;

        // Process 8 elements at a time using AVX2
        let chunks = len / 8;
        let remainder = len % 8;

        unsafe {
            for i in 0..chunks {
                let offset = i * 8;
                let vec = _mm256_loadu_ps(&values[offset]);
                
                // Sum the vector
                let sum_vec = _mm256_hadd_ps(vec, vec);
                let sum_vec = _mm256_hadd_ps(sum_vec, sum_vec);
                let sum_vec = _mm256_hadd_ps(sum_vec, sum_vec);
                sum += mem::transmute::<__m128, f32>(_mm256_extractf128_ps(sum_vec, 0));
            }
        }

        // Process remainder
        for i in (chunks * 8)..len {
            sum += values[i];
        }

        sum
    }

    /// Compute average of vector elements using SIMD
    pub fn average(values: &[f32]) -> f32 {
        if values.is_empty() {
            return 0.0;
        }
        Self::sum(values) / values.len() as f32
    }
}

/// SIMD-optimized string operations
pub struct SimdStringOps;

impl SimdStringOps {
    /// Fast string comparison using SIMD
    pub fn fast_compare(a: &str, b: &str) -> bool {
        if a.len() != b.len() {
            return false;
        }

        let a_bytes = a.as_bytes();
        let b_bytes = b.as_bytes();
        let len = a_bytes.len();

        // Process 32 bytes at a time using AVX2
        let chunks = len / 32;
        let remainder = len % 32;

        unsafe {
            for i in 0..chunks {
                let offset = i * 32;
                let a_vec = _mm256_loadu_si256(a_bytes.as_ptr().add(offset) as *const __m256i);
                let b_vec = _mm256_loadu_si256(b_bytes.as_ptr().add(offset) as *const __m256i);
                
                let cmp = _mm256_cmpeq_epi8(a_vec, b_vec);
                let mask = _mm256_movemask_epi8(cmp);
                
                if mask != 0xFFFFFFFF {
                    return false;
                }
            }
        }

        // Process remainder
        for i in (chunks * 32)..len {
            if a_bytes[i] != b_bytes[i] {
                return false;
            }
        }

        true
    }

    /// Fast string search using SIMD (Boyer-Moore-Horspool with SIMD)
    pub fn fast_search(haystack: &str, needle: &str) -> Option<usize> {
        if needle.is_empty() {
            return Some(0);
        }
        if needle.len() > haystack.len() {
            return None;
        }

        let haystack_bytes = haystack.as_bytes();
        let needle_bytes = needle.as_bytes();
        let needle_len = needle_bytes.len();
        let haystack_len = haystack_bytes.len();

        // Simple implementation for now - can be optimized further
        for i in 0..=(haystack_len - needle_len) {
            if Self::fast_compare(
                std::str::from_utf8(&haystack_bytes[i..i + needle_len]).unwrap(),
                needle
            ) {
                return Some(i);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![2.0, 4.0, 6.0, 8.0];
        let similarity = SimdOps::cosine_similarity(&a, &b).unwrap();
        assert!((similarity - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        let distance = SimdOps::euclidean_distance(&a, &b).unwrap();
        assert!((distance - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_max_min_values() {
        let values = vec![1.0, 5.0, 3.0, 9.0, 2.0];
        assert_eq!(SimdOps::max_value(&values), 9.0);
        assert_eq!(SimdOps::min_value(&values), 1.0);
    }

    #[test]
    fn test_string_operations() {
        assert!(SimdStringOps::fast_compare("hello", "hello"));
        assert!(!SimdStringOps::fast_compare("hello", "world"));
        
        assert_eq!(SimdStringOps::fast_search("hello world", "world"), Some(6));
        assert_eq!(SimdStringOps::fast_search("hello world", "xyz"), None);
    }
}