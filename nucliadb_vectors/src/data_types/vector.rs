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

use std::io::{Read, Write};

type Len = u64;
type Unit = f32;
type Dist = f32;

fn encode_length(mut buff: Vec<u8>, vec: &[Unit]) -> Vec<u8> {
    let len = vec.len() as Len;
    buff.write_all(&len.to_le_bytes()).unwrap();
    buff.flush().unwrap();
    buff
}
fn encode_unit(mut buff: Vec<u8>, unit: Unit) -> Vec<u8> {
    buff.write_all(&unit.to_le_bytes()).unwrap();
    buff.flush().unwrap();
    buff
}
pub fn encode_vector(vec: &[Unit]) -> Vec<u8> {
    vec.iter()
        .cloned()
        .fold(encode_length(vec![], vec), encode_unit)
}
#[inline]
fn read_len(mut x: &[u8]) -> Len {
    let mut buff_x = [0; 8];
    x.read_exact(&mut buff_x).unwrap();
    Len::from_le_bytes(buff_x)
}

#[inline]
fn remove_len(x: &[u8]) -> &[u8] {
    &x[8..]
}

#[inline]
#[allow(unused)]
pub fn simd_cosine_similarity(left: &[u8], right: &[u8]) -> f32 {
    use wide::f32x8;
    const NUM_LANES: usize = 8;
    let dim = read_len(left) as usize;
    let slots = dim / NUM_LANES;
    let used_bytes = slots * NUM_LANES * 4;
    let left = remove_len(left);
    let right = remove_len(right);
    assert_eq!(left.len(), dim * 4);
    assert_eq!(right.len(), dim * 4);
    let mut left_ptr = left.as_ptr() as *const f32x8;
    let mut right_ptr = right.as_ptr() as *const f32x8;
    let mut acc_left_norm: f32x8 = Default::default();
    let mut acc_right_norm: f32x8 = Default::default();
    let mut acc_dot_product: f32x8 = Default::default();
    unsafe {
        if used_bytes < left.len() {
            let unused = (left.len() - used_bytes) / 4;
            let mut unused_left = left[used_bytes..].as_ptr() as *const f32;
            let mut unused_right = right[used_bytes..].as_ptr() as *const f32;
            let mut aux_buff_left = [0.0; 8];
            let mut aux_buff_right = [0.0; 8];
            for i in 0..unused {
                let left: f32 = std::ptr::read_unaligned(unused_left);
                let right: f32 = std::ptr::read_unaligned(unused_right);
                aux_buff_left[i] = left;
                aux_buff_right[i] = right;
                unused_left = unused_left.offset(1);
                unused_right = unused_right.offset(1);
            }
            let aux_left = f32x8::new(aux_buff_left);
            let aux_right = f32x8::new(aux_buff_right);
            acc_left_norm = aux_left * aux_left;
            acc_right_norm = aux_right * aux_right;
            acc_dot_product = aux_left * aux_right;
        }
        for i in 0..slots {
            let mut left: f32x8 = std::ptr::read_unaligned(left_ptr);
            let right: f32x8 = std::ptr::read_unaligned(right_ptr);
            acc_left_norm += left * left;
            acc_right_norm += right * right;
            acc_dot_product += left * right;
            left_ptr = left_ptr.offset(1);
            right_ptr = right_ptr.offset(1);
        }
    }
    let norm_left = acc_left_norm.reduce_add().sqrt();
    let norm_right = acc_right_norm.reduce_add().sqrt();
    acc_dot_product.reduce_add() / (norm_left * norm_right)
}

#[inline]
#[allow(unused)]
pub fn consine_similarity(mut x: &[u8], mut y: &[u8]) -> Dist {
    let mut buff_x = [0; 8];
    let mut buff_y = [0; 8];
    x.read_exact(&mut buff_x).unwrap();
    y.read_exact(&mut buff_y).unwrap();
    let len_x = Len::from_le_bytes(buff_x);
    let len_y = Len::from_le_bytes(buff_y);
    assert_eq!(len_x, len_y);
    let len = len_x;
    let mut buff_x = [0; 4];
    let mut buff_y = [0; 4];
    let mut sum = 0.0;
    let mut dem_x = 0.0;
    let mut dem_y = 0.0;
    for _ in 0..len {
        x.read_exact(&mut buff_x).unwrap();
        y.read_exact(&mut buff_y).unwrap();
        let x_value = Unit::from_le_bytes(buff_x);
        let y_value = Unit::from_le_bytes(buff_y);
        sum += x_value * y_value;
        dem_x += x_value * x_value;
        dem_y += y_value * y_value;
    }
    sum / (f32::sqrt(dem_x) * f32::sqrt(dem_y))
}

#[cfg(test)]
mod test {
    use super::*;
    fn naive_cosine_similatiry(a: &[f32], b: &[f32]) -> f32 {
        let ab: f32 = a
            .iter()
            .cloned()
            .zip(b.iter().cloned())
            .map(|(a, b)| a * b)
            .sum();
        let aa: f32 = a.iter().cloned().map(|a| a * a).sum();
        let bb: f32 = b.iter().cloned().map(|b| b * b).sum();
        ab / (f32::sqrt(aa) * f32::sqrt(bb))
    }
    #[test]
    fn naive_equivalence() {
        let v0: Vec<_> = (0..758).map(|i| (i * 2) as f32).collect();
        let v1: Vec<_> = (0..758).map(|i| ((i * 2) + 1) as f32).collect();
        let v0_r = encode_vector(&v0);
        let v1_r = encode_vector(&v1);
        assert_eq!(
            naive_cosine_similatiry(&v0, &v1),
            consine_similarity(&v0_r, &v1_r)
        );
        assert_eq!(
            naive_cosine_similatiry(&v0, &v0),
            consine_similarity(&v0_r, &v0_r)
        );
    }
    #[test]
    fn naive_equivalence_simd() {
        let v0: Vec<_> = (0..758).map(|i| (i * 2) as f32).collect();
        let v1: Vec<_> = (0..758).map(|i| ((i * 2) + 1) as f32).collect();
        let v0_r = encode_vector(&v0);
        let v1_r = encode_vector(&v1);
        let naive = naive_cosine_similatiry(&v0, &v1);
        let simd = simd_cosine_similarity(&v0_r, &v1_r);
        let diff = f32::abs(naive - simd);
        assert!(diff <= 0.000001);
        let naive = naive_cosine_similatiry(&v0, &v0);
        let simd = simd_cosine_similarity(&v0_r, &v0_r);
        let diff = f32::abs(naive - simd);
        assert!(diff <= 0.000001);
    }
}
