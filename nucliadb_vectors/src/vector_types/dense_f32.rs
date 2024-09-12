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

use std::io::Write;

use simsimd::SpatialSimilarity;

/// Fallback implementation of simsimd. This is used because manylinux for ARM64
/// uses GCC 7.5.0 which is not able to compile simsimd (missing arm_sve.h headers)
/// Once manylinux with GCC 8+ is released, we could delete this.
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
mod simsimd {
    type Distance = f64;
    pub trait SpatialSimilarity
    where
        Self: Sized,
    {
        fn cosine(a: &[Self], b: &[Self]) -> Option<Distance>;
        fn dot(a: &[Self], b: &[Self]) -> Option<Distance>;
    }

    impl SpatialSimilarity for f32 {
        fn cosine(a: &[Self], b: &[Self]) -> Option<Distance> {
            let len = a.len();
            let mut sum = 0.0;
            let mut dem_x = 0.0;
            let mut dem_y = 0.0;
            for i in 0..len {
                sum += a[i] * b[i];
                dem_x += a[i] * a[i];
                dem_y += b[i] * b[i];
            }
            Some(1.0 - (sum / (f32::sqrt(dem_x) * f32::sqrt(dem_y))) as f64)
        }

        fn dot(a: &[Self], b: &[Self]) -> Option<Distance> {
            let len = a.len();
            let mut sum = 0.0;
            for i in 0..len {
                sum += a[i] * b[i];
            }
            Some(sum as f64)
        }
    }
}

type Unit = f32;
type Dist = f32;

fn encode_unit(mut buff: Vec<u8>, unit: Unit) -> Vec<u8> {
    buff.write_all(&unit.to_le_bytes()).unwrap();
    buff.flush().unwrap();
    buff
}

pub fn cosine_similarity(x: &[u8], y: &[u8]) -> Dist {
    let (p, x, s) = unsafe { x.align_to() };
    debug_assert!(p.is_empty());
    debug_assert!(s.is_empty());
    let (p, y, s) = unsafe { y.align_to() };
    debug_assert!(p.is_empty());
    debug_assert!(s.is_empty());
    1.0 - f32::cosine(x, y).unwrap() as f32
}

pub fn dot_similarity(x: &[u8], y: &[u8]) -> Dist {
    let (p, x, s) = unsafe { x.align_to() };
    debug_assert!(p.is_empty());
    debug_assert!(s.is_empty());
    let (p, y, s) = unsafe { y.align_to() };
    debug_assert!(p.is_empty());
    debug_assert!(s.is_empty());
    f32::dot(x, y).unwrap() as f32
}

pub fn encode_vector(vec: &[Unit]) -> Vec<u8> {
    vec.iter().cloned().fold(vec![], encode_unit)
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
