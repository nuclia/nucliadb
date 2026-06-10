// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::io::Write;

use simsimd::SpatialSimilarity;

type Unit = f32;
type Dist = f32;

fn encode_unit(mut buff: Vec<u8>, unit: Unit) -> Vec<u8> {
    buff.write_all(&unit.to_le_bytes()).unwrap();
    buff.flush().unwrap();
    buff
}

pub fn cosine_similarity(x: &[u8], y: &[u8]) -> Dist {
    let x = decode_vector(x);
    let y = decode_vector(y);
    1.0 - f32::cosine(x, y).unwrap() as f32
}

pub fn dot_similarity(x: &[u8], y: &[u8]) -> Dist {
    let x = decode_vector(x);
    let y = decode_vector(y);
    f32::dot(x, y).unwrap() as f32
}

pub fn encode_vector(vec: &[Unit]) -> Vec<u8> {
    vec.iter().cloned().fold(vec![], encode_unit)
}

pub fn decode_vector(x: &[u8]) -> &[f32] {
    let (p, x, s) = unsafe { x.align_to() };
    debug_assert!(p.is_empty());
    debug_assert!(s.is_empty());
    x
}

#[cfg(test)]
mod test {
    use super::*;
    fn naive_cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let ab: f32 = a.iter().cloned().zip(b.iter().cloned()).map(|(a, b)| a * b).sum();
        let aa: f32 = a.iter().cloned().map(|a| a * a).sum();
        let bb: f32 = b.iter().cloned().map(|b| b * b).sum();
        ab / (f32::sqrt(aa) * f32::sqrt(bb))
    }

    fn naive_dot_similarity(a: &[f32], b: &[f32]) -> f32 {
        a.iter().cloned().zip(b.iter().cloned()).map(|(a, b)| a * b).sum()
    }

    #[test]
    fn cosine_test() {
        let v0: Vec<_> = (0..758).map(|i| i as f32 * 2.0).collect();
        let v1: Vec<_> = (0..758).map(|i| (i as f32 * 1.0) + 1.0).collect();
        let v0_r = encode_vector(&v0);
        let v1_r = encode_vector(&v1);
        assert!((naive_cosine_similarity(&v0, &v0) - cosine_similarity(&v0_r, &v0_r)).abs() < 0.01);
        assert!((naive_cosine_similarity(&v0, &v1) - cosine_similarity(&v0_r, &v1_r)).abs() < 0.01);
    }

    #[test]
    fn dot_test() {
        let v0: Vec<_> = (0..758).map(|i| i as f32 * 0.002).collect();
        let v1: Vec<_> = (0..758).map(|i| (i as f32 * 0.002) + 0.05).collect();
        let v0_r = encode_vector(&v0);
        let v1_r = encode_vector(&v1);
        assert!((naive_dot_similarity(&v0, &v0) - dot_similarity(&v0_r, &v0_r)).abs() < 0.01);
        assert!((naive_dot_similarity(&v0, &v1) - dot_similarity(&v0_r, &v1_r)).abs() < 0.01);
    }
}
