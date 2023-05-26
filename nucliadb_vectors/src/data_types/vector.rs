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

pub fn vector_len(mut x: &[u8]) -> u64 {
    let mut buff_x = [0; 8];
    x.read_exact(&mut buff_x).unwrap();
    Len::from_le_bytes(buff_x)
}

pub fn cosine_similarity(mut x: &[u8], mut y: &[u8]) -> Dist {
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

pub fn dot_similarity(mut x: &[u8], mut y: &[u8]) -> Dist {
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
    for _ in 0..len {
        x.read_exact(&mut buff_x).unwrap();
        y.read_exact(&mut buff_y).unwrap();
        let x_value = Unit::from_le_bytes(buff_x);
        let y_value = Unit::from_le_bytes(buff_y);
        sum += x_value * y_value;
    }
    sum
}

pub fn encode_vector(vec: &[Unit]) -> Vec<u8> {
    vec.iter()
        .cloned()
        .fold(encode_length(vec![], vec), encode_unit)
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

    fn naive_dot_similatiry(a: &[f32], b: &[f32]) -> f32 {
        a.iter()
            .cloned()
            .zip(b.iter().cloned())
            .map(|(a, b)| a * b)
            .sum()
    }

    #[test]
    fn cosine_test() {
        let v0: Vec<_> = (0..758).map(|i| (i * 2) as f32).collect();
        let v1: Vec<_> = (0..758).map(|i| ((i * 2) + 1) as f32).collect();
        let v0_r = encode_vector(&v0);
        let v1_r = encode_vector(&v1);
        assert_eq!(
            naive_cosine_similatiry(&v0, &v1),
            cosine_similarity(&v0_r, &v1_r)
        );
        assert_eq!(
            naive_cosine_similatiry(&v0, &v0),
            cosine_similarity(&v0_r, &v0_r)
        );
    }

    #[test]
    fn dot_test() {
        let v0: Vec<_> = (0..758).map(|i| (i * 2) as f32).collect();
        let v1: Vec<_> = (0..758).map(|i| ((i * 2) + 1) as f32).collect();
        let v0_r = encode_vector(&v0);
        let v1_r = encode_vector(&v1);
        assert_eq!(naive_dot_similatiry(&v0, &v1), dot_similarity(&v0_r, &v1_r));
        assert_eq!(naive_dot_similatiry(&v0, &v1), dot_similarity(&v0_r, &v1_r));
    }
}
