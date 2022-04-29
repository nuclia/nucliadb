#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crate::distance::simple_sse::*;

#[cfg(target_arch = "x86_64")]
use crate::distance::simple_avx::*;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::distance::simple_neon::*;

#[cfg(target_arch = "x86_64")]
const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))]
const MIN_DIM_SIZE_SIMD: usize = 16;

pub fn cosine_distance(a: &Vec<f32>, b: &Vec<f32>) -> f32 {
    let a = cosine_preprocess(a).unwrap();
    let b = cosine_preprocess(b).unwrap();
    cosine_similarity(&a, &b)
}

pub fn cosine_preprocess(vector: &Vec<f32>) -> Option<Vec<f32>> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && vector.len() >= MIN_DIM_SIZE_AVX
        {
            return Some(unsafe { cosine_preprocess_avx(vector) });
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && vector.len() >= MIN_DIM_SIZE_SIMD {
            return Some(unsafe { cosine_preprocess_sse(vector) });
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && vector.len() >= MIN_DIM_SIZE_SIMD {
            return Some(unsafe { cosine_preprocess_neon(vector) });
        }
    }

    Some(default_cosine_preprocess(vector))
}

pub fn default_cosine_preprocess(vector: &Vec<f32>) -> Vec<f32> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

pub fn cosine_similarity(v1: &Vec<f32>, v2: &Vec<f32>) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && v1.len() >= MIN_DIM_SIZE_AVX
        {
            return unsafe { dot_similarity_avx(v1, v2) };
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
            return unsafe { dot_similarity_sse(v1, v2) };
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
            return unsafe { dot_similarity_neon(v1, v2) };
        }
    }

    default_dot_similarity(v1, v2)
}

pub fn default_dot_similarity(v1: &Vec<f32>, v2: &Vec<f32>) -> f32 {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}
