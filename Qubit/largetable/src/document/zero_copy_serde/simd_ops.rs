// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! AVX2/AVX512 + SIMD + multi-threaded operations
//! for high-performance vector embeddings and large arrays

use wide::f32x8;
use rayon::prelude::*;

/// Dot product of two vectors using SIMD and multi-threading
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vector lengths must match");

    if a.is_empty() {
        return 0.0;
    }

    // Use parallel computation for large vectors
    if a.len() > 1024 {
        return a.par_chunks(1024)
            .zip(b.par_chunks(1024))
            .map(|(chunk_a, chunk_b)| dot_product_simd(chunk_a, chunk_b))
            .sum();
    }

    dot_product_simd(a, b)
}

/// Magnitude (L2 norm) using SIMD and multi-threading
pub fn magnitude(vec: &[f32]) -> f32 {
    if vec.is_empty() {
        return 0.0;
    }

    // Use parallel computation for large vectors
    if vec.len() > 1024 {
        let sum_squares: f32 = vec.par_chunks(1024)
            .map(|chunk| magnitude_simd_squared(chunk))
            .sum();
        return sum_squares.sqrt();
    }

    magnitude_simd_squared(vec).sqrt()
}

// ===================== SIMD squared magnitude =====================
#[inline]
fn magnitude_simd_squared(vec: &[f32]) -> f32 {
    #[cfg(any(target_feature = "avx512f"))]
    {
        use std::arch::x86_64::*;
        unsafe { magnitude_avx512(vec) }
    }
    #[cfg(all(not(target_feature = "avx512f"), target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        unsafe { magnitude_avx2(vec) }
    }
    #[cfg(not(any(target_feature = "avx2", target_feature = "avx512f")))]
    {
        magnitude_fallback(vec)
    }
}

// ===================== Fallback SIMD with f32x8 =====================
fn magnitude_fallback(vec: &[f32]) -> f32 {
    let mut sum = f32x8::ZERO;
    let chunks = vec.len() / 8;

    for i in 0..chunks {
        let start = i * 8;
        let v = f32x8::from_array([
            vec[start], vec[start + 1], vec[start + 2], vec[start + 3],
            vec[start + 4], vec[start + 5], vec[start + 6], vec[start + 7],
        ]);
        sum += v * v;
    }

    let mut result = sum.reduce_add();
    for i in (chunks * 8)..vec.len() {
        result += vec[i] * vec[i];
    }
    result
}

// ===================== AVX2 =====================
#[cfg(target_feature = "avx2")]
#[target_feature(enable = "avx2")]
unsafe fn magnitude_avx2(vec: &[f32]) -> f32 {
    let mut sum = _mm256_setzero_ps();
    let chunks = vec.len() / 8;

    for i in 0..chunks {
        let start = i * 8;
        let v = _mm256_loadu_ps(vec[start..].as_ptr());
        sum = _mm256_add_ps(sum, _mm256_mul_ps(v, v));
    }

    let mut result_arr = [0.0f32; 8];
    _mm256_storeu_ps(result_arr.as_mut_ptr(), sum);
    let mut total: f32 = result_arr.iter().sum();
    for i in (chunks * 8)..vec.len() {
        total += vec[i] * vec[i];
    }
    total
}

// ===================== AVX512 =====================
#[cfg(target_feature = "avx512f")]
#[target_feature(enable = "avx512f")]
unsafe fn magnitude_avx512(vec: &[f32]) -> f32 {
    let mut sum = _mm512_setzero_ps();
    let chunks = vec.len() / 16;

    for i in 0..chunks {
        let start = i * 16;
        let v = _mm512_loadu_ps(vec[start..].as_ptr());
        sum = _mm512_add_ps(sum, _mm512_mul_ps(v, v));
    }

    let mut total = _mm512_reduce_add_ps(sum);
    for i in (chunks * 16)..vec.len() {
        total += vec[i] * vec[i];
    }
    total
}

/// Cosine similarity using SIMD + multi-threading
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let norm_a = magnitude(a);
    let norm_b = magnitude(b);
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

// ===================== SIMD Implementation =====================
#[inline]
fn dot_product_simd(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(any(target_feature = "avx512f"))]
    {
        use std::arch::x86_64::*;
        unsafe { dot_product_avx512(a, b) }
    }
    #[cfg(all(not(target_feature = "avx512f"), target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        unsafe { dot_product_avx2(a, b) }
    }
    #[cfg(not(any(target_feature = "avx2", target_feature = "avx512f")))]
    {
        dot_product_fallback(a, b)
    }
}

// ===================== Fallback SIMD with f32x8 =====================
fn dot_product_fallback(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = f32x8::ZERO;
    let chunks = a.len() / 8;

    for i in 0..chunks {
        let start = i * 8;
        let va = f32x8::from_array([
            a[start], a[start + 1], a[start + 2], a[start + 3],
            a[start + 4], a[start + 5], a[start + 6], a[start + 7],
        ]);
        let vb = f32x8::from_array([
            b[start], b[start + 1], b[start + 2], b[start + 3],
            b[start + 4], b[start + 5], b[start + 6], b[start + 7],
        ]);
        sum += va * vb;
    }

    let mut result = sum.reduce_add();
    for i in (chunks * 8)..a.len() {
        result += a[i] * b[i];
    }
    result
}

// ===================== AVX2 =====================
#[cfg(target_feature = "avx2")]
#[target_feature(enable = "avx2")]
unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = _mm256_setzero_ps();
    let chunks = a.len() / 8;

    for i in 0..chunks {
        let start = i * 8;
        let va = _mm256_loadu_ps(a[start..].as_ptr());
        let vb = _mm256_loadu_ps(b[start..].as_ptr());
        sum = _mm256_add_ps(sum, _mm256_mul_ps(va, vb));
    }

    let mut result = [0.0f32; 8];
    _mm256_storeu_ps(result.as_mut_ptr(), sum);
    let mut total: f32 = result.iter().sum();
    for i in (chunks * 8)..a.len() {
        total += a[i] * b[i];
    }
    total
}

// ===================== AVX512 =====================
#[cfg(target_feature = "avx512f")]
#[target_feature(enable = "avx512f")]
unsafe fn dot_product_avx512(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = _mm512_setzero_ps();
    let chunks = a.len() / 16;

    for i in 0..chunks {
        let start = i * 16;
        let va = _mm512_loadu_ps(a[start..].as_ptr());
        let vb = _mm512_loadu_ps(b[start..].as_ptr());
        sum = _mm512_add_ps(sum, _mm512_mul_ps(va, vb));
    }

    let mut total: f32 = _mm512_reduce_add_ps(sum);
    for i in (chunks * 16)..a.len() {
        total += a[i] * b[i];
    }
    total
}

// ===================== Unit Tests =====================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_small() {
        let a = vec![1.0f32; 16];
        let b = vec![2.0f32; 16];
        let res = dot_product(&a, &b);
        assert_eq!(res, 32.0);
    }

    #[test]
    fn test_magnitude() {
        let v = vec![3.0, 4.0];
        let res = magnitude(&v);
        assert_eq!(res, 5.0);
    }

    #[test]
    fn test_cosine() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let res = cosine_similarity(&a, &b);
        assert_eq!(res, 0.0);
    }

    #[test]
    fn test_cosine_parallel_vectors() {
        let a = vec![1.0, 1.0];
        let b = vec![1.0, 1.0];
        let res = cosine_similarity(&a, &b);
        assert!((res - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_large_parallel_dot() {
        let a: Vec<f32> = vec![1.0; 10_000];
        let b: Vec<f32> = vec![2.0; 10_000];
        let res = dot_product(&a, &b);
        assert_eq!(res, 20_000.0);
    }
          }
