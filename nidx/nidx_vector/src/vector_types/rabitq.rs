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

use bit_vec::BitVec;
use simsimd::SpatialSimilarity;

const EPSILON: f32 = 1.9;

struct EncodedVector<'a> {
    data: &'a [u8],
}

impl<'a> EncodedVector<'a> {
    /// Binary quantized vector
    fn quantized(&self) -> BitVec {
        BitVec::from_bytes(&self.data[8..])
    }

    /// Dot product of the original vector and the quantized version
    fn dot_quant_original(&self) -> f32 {
        f32::from_le_bytes(self.data[..4].try_into().unwrap())
    }

    /// Number of '1' bits in the quantized vector
    fn sum_bits(&self) -> u32 {
        u32::from_le_bytes(self.data[4..8].try_into().unwrap())
    }

    fn encode(v: &[f32]) -> Vec<u8> {
        let root_dim = (v.len() as f32).sqrt();
        let quantized: BitVec<u32> = BitVec::from_iter(v.iter().map(|v| *v > 0.0));
        let sum_bits = quantized.count_ones() as u32;
        let qv: Vec<f32> = quantized
            .iter()
            .map(|w| if w { 1.0 / root_dim } else { -1.0 / root_dim })
            .collect();
        let dot_quant_original = f32::dot(v, &qv).unwrap() as f32;

        let mut serialized = Vec::with_capacity(v.len() / 8 + 8);
        serialized.extend_from_slice(&dot_quant_original.to_le_bytes());
        serialized.extend_from_slice(&sum_bits.to_le_bytes());
        serialized.append(&mut quantized.to_bytes());

        serialized
    }
}

struct QueryVector {
    /// Vector quantized to u8
    quantized: Vec<u8>,
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
    fn from_vector(q: &[f32]) -> Self {
        let (low, hi) = q.iter().fold((q[0], q[0]), |(min, max), it| {
            (if *it < min { *it } else { min }, if *it > max { *it } else { max })
        });
        let delta = (hi - low) / 256.0;

        let quantized: Vec<u8> = q.iter().map(|w| ((w - low) / delta) as u8).collect();
        let sum_quantized = quantized.iter().map(|x| *x as u32).sum();
        let root_dim = (q.len() as f32).sqrt();

        QueryVector {
            quantized,
            sum_quantized,
            root_dim,
            low,
            delta,
        }
    }

    fn similarity(&self, other: &EncodedVector) -> (f32, f32) {
        let dot = self
            .quantized
            .iter()
            .zip(other.quantized().iter())
            .filter(|(_, b)| *b)
            .map(|(i, _)| *i as u32)
            .sum::<u32>() as f32;

        let dot_quant_query_vector = 2.0 * self.delta / self.root_dim * dot as f32
            + 2.0 * self.low * other.sum_bits() as f32 / self.root_dim
            - self.delta * self.sum_quantized as f32 / self.root_dim
            - self.low * self.root_dim;

        let estimate = dot_quant_query_vector / other.dot_quant_original();

        let d2 = other.dot_quant_original() * other.dot_quant_original();
        // Should divide by sqrt(DIM - 1) but sqrt(DIM) is already computed and close enough
        let error = ((1.0 - d2) / d2).sqrt() * EPSILON / self.root_dim;

        (estimate, error)
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng, rngs::SmallRng};
    use simsimd::SpatialSimilarity;

    use crate::vector_types::rabitq::{EncodedVector, QueryVector};

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
        let v2_query = QueryVector::from_vector(&v2);
        let (estimate, err) = v2_query.similarity(&EncodedVector { data: &v1_encoded });
        assert!((actual - estimate).abs() < err);
        assert!(err < 0.05);

        // v1-v3 (low similarity)
        let actual = f32::dot(&v1, &v3).unwrap() as f32;
        let v3_query = QueryVector::from_vector(&v3);
        let (estimate, err) = v3_query.similarity(&EncodedVector { data: &v1_encoded });
        assert!((actual - estimate).abs() < err);
        assert!(err < 0.05);
    }
}
