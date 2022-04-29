// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

#[cfg(target_arch = "x86_64")]
use crate::distance::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use crate::distance::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use crate::distance::simple_sse::*;

#[cfg(target_arch = "x86_64")]
const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))]
const MIN_DIM_SIZE_SIMD: usize = 16;

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let a = cosine_preprocess(a).unwrap();
    let b = cosine_preprocess(b).unwrap();
    cosine_similarity(&a, &b)
}

pub fn cosine_preprocess(vector: &[f32]) -> Option<Vec<f32>> {
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

pub fn default_cosine_preprocess(vector: &[f32]) -> Vec<f32> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

pub fn cosine_similarity(v1: &[f32], v2: &[f32]) -> f32 {
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

pub fn default_dot_similarity(v1: &[f32], v2: &[f32]) -> f32 {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}
