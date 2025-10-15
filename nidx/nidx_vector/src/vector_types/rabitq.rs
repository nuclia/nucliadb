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

use std::{cmp::Reverse, collections::BinaryHeap};

use simsimd::SpatialSimilarity;

use crate::{
    VectorAddr,
    config::VectorType,
    hnsw::{Cnx, DataRetriever, EstimatedScore, SearchVector},
};

/// Constant to adjust the error bound of the estimated similarity.
// The paper recommends 1.9, reasonable values are from 0.0 to 4.0
// Higher numbers give more error bound so we need to load more raw
// vectors but provide slightly better recall.
const EPSILON: f32 = 1.9;

/// How many vectors to evaluate per each expected result.
/// This is multiplied by top_k to get the number of vectors to search for.
pub const RERANKING_FACTOR: usize = 100;
/// The maximum number of vectors to evaluate during reranking.
pub const RERANKING_LIMIT: usize = 2000;

pub struct EncodedVector<'a> {
    data: &'a [u8],
}

impl<'a> EncodedVector<'a> {
    pub fn from_bytes(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn bytes(&self) -> &[u8] {
        self.data
    }

    /// Binary quantized vector
    fn quantized(&self) -> &[u64] {
        let (p, data, s) = unsafe { self.data[8..].align_to() };
        debug_assert!(p.is_empty());
        debug_assert!(s.is_empty());

        data
    }

    /// Dot product of the original vector and the quantized version
    fn dot_quant_original(&self) -> f32 {
        f32::from_le_bytes(self.data[..4].try_into().unwrap())
    }

    /// Number of '1' bits in the quantized vector
    fn sum_bits(&self) -> u32 {
        u32::from_le_bytes(self.data[4..8].try_into().unwrap())
    }

    pub fn encoded_len(dimension: usize) -> usize {
        // 4 bytes (f32, dot_quant_original) + 4 bytes (u32, sum_bit) + one byte per 8 dimensions (binary vector)
        dimension / 8 + 8
    }

    pub fn encode(v: &[f32]) -> Vec<u8> {
        let root_dim = (v.len() as f32).sqrt();
        assert!(v.len().is_multiple_of(64));

        // Binary vector
        let mut quantized = vec![0u64; v.len() / 64];
        // Sum of '1' bits in the vector
        let mut sum_bits: u32 = 0;
        // Float representation of the binary vector
        let mut v_repr = Vec::with_capacity(v.len());

        for i in 0..v.len() {
            if v[i] > 0.0 {
                quantized[i / 64] += 1 << (i % 64);
                sum_bits += 1;
                v_repr.push(1.0 / root_dim);
            } else {
                v_repr.push(-1.0 / root_dim);
            }
        }
        // Dot distance from the original vector to what the binary vector represents
        let dot_quant_original = f32::dot(v, &v_repr).unwrap() as f32;

        let mut serialized = Vec::with_capacity(Self::encoded_len(v.len()));
        serialized.extend_from_slice(&dot_quant_original.to_le_bytes());
        serialized.extend_from_slice(&sum_bits.to_le_bytes());
        for x in quantized {
            serialized.extend(x.to_le_bytes());
        }

        serialized
    }
}

pub struct QueryVector {
    /// Original vector, encoded for similarity search
    original: Vec<u8>,
    /// Vector quantized to u4. Each embedding occupies one bit in each element of the array
    quantized: [Vec<u64>; 4],
    /// Lowest value in original vector. quantized value 0 maps to this
    low: f32,
    /// Delta in quantization. Each quantized unit maps to this differences in the original vector
    delta: f32,
    /// Sum of all quantized values
    sum_quantized: u32,
    /// sqrt of dimension
    root_dim: f32,
}

impl QueryVector {
    pub fn from_vector(q: &[f32], vector_type: &VectorType) -> Self {
        let (low, mut hi) = q.iter().fold((q[0], q[0]), |(min, max), it| {
            (if *it < min { *it } else { min }, if *it > max { *it } else { max })
        });
        hi += 0.00001; // Add a small epsilon to hi, so the highest value in the vector maps to 15, not 16
        let delta = (hi - low) / 16.0;

        assert!(vector_type.dimension().is_multiple_of(64));
        let u64_len = vector_type.dimension() / 64;
        let mut quantized = [vec![0; u64_len], vec![0; u64_len], vec![0; u64_len], vec![0; u64_len]];
        let mut sum_quantized = 0;
        for (i, q_i) in q.iter().enumerate() {
            // Quantize our vector to a 4-bit vector.
            let wq = ((q_i - low) / delta) as u64;
            sum_quantized += wq;
            // Stores it into 4 vectors (one bit in each vector)
            // This makes it easier to optimize `fn dot()`
            quantized[0][i / 64] += (wq % 2) << (i % 64);
            quantized[1][i / 64] += ((wq / 2) % 2) << (i % 64);
            quantized[2][i / 64] += ((wq / 4) % 2) << (i % 64);
            quantized[3][i / 64] += ((wq / 8) % 2) << (i % 64);
        }
        let root_dim = (q.len() as f32).sqrt();

        QueryVector {
            original: vector_type.encode(q),
            quantized,
            sum_quantized: sum_quantized as u32,
            root_dim,
            low,
            delta,
        }
    }

    pub fn original(&self) -> &[u8] {
        &self.original
    }

    /// Dot product of a binary vector (other) and a 4-bit vector (self)
    /// Our vector is stored as 4 1-bit vector (one per bit of our quantization)
    /// in order to make the operations below easier to run by the CPU.
    fn dot(&self, other: &EncodedVector) -> u32 {
        // The code below is auto-vectorized by the compiler. It's within
        // 20% performance of a hand-written implementation, mainly because
        // it doesn't assume the number of u64 elements in the vector is a
        // multiple of the extended registry size.
        let stored = other.quantized();

        // We need to do q * s. We have q split into one vector per bit position,
        // so we can calculate the product of each bit with the stored vector and
        // multiply it by the bit value (1, 2, 4, 8)
        // Multiplication of binary vectors is just AND, and summing each bit can be
        // done efficienctly with the count_ones / popcnt operation.
        let d0 = self.quantized[0]
            .iter()
            .zip(stored.iter())
            .map(|(a, b)| (a & b).count_ones())
            .sum::<u32>();
        let d1 = self.quantized[1]
            .iter()
            .zip(stored.iter())
            .map(|(a, b)| (a & b).count_ones())
            .sum::<u32>();
        let d2 = self.quantized[2]
            .iter()
            .zip(stored.iter())
            .map(|(a, b)| (a & b).count_ones())
            .sum::<u32>();
        let d3 = self.quantized[3]
            .iter()
            .zip(stored.iter())
            .map(|(a, b)| (a & b).count_ones())
            .sum::<u32>();

        d0 + d1 * 2 + d2 * 4 + d3 * 8
    }

    pub fn similarity(&self, other: EncodedVector) -> (f32, f32) {
        let dot = self.dot(&other) as f32;

        let dot_quant_query_vector = 2.0 * self.delta / self.root_dim * dot
            + 2.0 * self.low * other.sum_bits() as f32 / self.root_dim
            - self.delta * self.sum_quantized as f32 / self.root_dim
            - self.low * self.root_dim;

        let estimate = dot_quant_query_vector / other.dot_quant_original();

        let d2 = other.dot_quant_original() * other.dot_quant_original();

        // Should divide by sqrt(DIM - 1) but sqrt(DIM) is already computed and close enough for usual values of DIM
        let error = ((1.0 - d2) / d2).sqrt() * EPSILON / self.root_dim;

        (estimate, error)
    }
}

/// Rerank results from RabitQ search using the raw vectors. Uses the error bound to know when to stop reranking
pub fn rerank_top(
    candidates: Vec<(VectorAddr, EstimatedScore)>,
    top_k: usize,
    retriever: &impl DataRetriever,
    query: &SearchVector,
) -> Vec<Reverse<Cnx>> {
    let mut best = BinaryHeap::new();
    let mut best_k = 0.0;
    for (addr, EstimatedScore { upper_bound, .. }) in candidates {
        if best.len() < top_k || best_k < upper_bound {
            // If the candidate score could be better than what we have so far, calculate the accurate similarity
            let real_score = retriever.similarity(addr, query);
            if best.len() < top_k || best_k < real_score {
                best.push(Reverse(Cnx(addr, real_score)));
                if best.len() > top_k {
                    best.pop();
                }
                best_k = best.peek().unwrap().0.1;
            }
        }
    }
    best.into_sorted_vec()
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng, rngs::SmallRng};
    use simsimd::SpatialSimilarity;

    use crate::{
        config::VectorType,
        vector_types::rabitq::{EncodedVector, QueryVector},
    };

    const DIMENSION: usize = 2048;

    fn random_vector(rng: &mut impl Rng) -> Vec<f32> {
        let v: Vec<f32> = (0..DIMENSION).map(|_| rng.gen_range(-1.0..1.0)).collect();
        normalize(v)
    }

    fn normalize(v: Vec<f32>) -> Vec<f32> {
        let mut modulus = 0.0;
        for w in &v {
            modulus += w * w;
        }
        modulus = modulus.powf(0.5);

        v.into_iter().map(|w| w / modulus).collect()
    }

    fn random_nearby_vector(rng: &mut impl Rng, close_to: &[f32], distance: f32) -> Vec<f32> {
        // Create a random vector of low modulus
        let fuzz = random_vector(rng);
        let v = close_to
            .iter()
            .zip(fuzz.iter())
            .map(|(v, fuzz)| v + fuzz * distance)
            .collect();
        normalize(v)
    }

    #[test]
    fn test_rabitq_estimate() {
        let mut rng = SmallRng::seed_from_u64(123);

        let v1 = random_vector(&mut rng);
        let v2 = random_nearby_vector(&mut rng, &v1, 0.1);
        let v3 = random_vector(&mut rng);

        // v1-v2 (high similarity)
        let actual = f32::dot(&v1, &v2).unwrap() as f32;
        let v1_encoded = EncodedVector::encode(&v1);
        let v2_query = QueryVector::from_vector(&v2, &VectorType::DenseF32 { dimension: DIMENSION });
        let (estimate, err) = v2_query.similarity(EncodedVector { data: &v1_encoded });
        assert!((actual - estimate).abs() < err);
        assert!(err < 0.05);

        // v1-v3 (low similarity)
        let actual = f32::dot(&v1, &v3).unwrap() as f32;
        let v3_query = QueryVector::from_vector(&v3, &VectorType::DenseF32 { dimension: DIMENSION });
        let (estimate, err) = v3_query.similarity(EncodedVector { data: &v1_encoded });
        assert!((actual - estimate).abs() < err);
        assert!(err < 0.05);
    }
}
