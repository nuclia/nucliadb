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

pub use std::collections::HashMap;

pub trait ByteRpr {
    fn as_byte_rpr(&self) -> Vec<u8>;
    fn from_byte_rpr(bytes: &[u8]) -> Self;
}

pub trait FixedByteLen: ByteRpr {
    fn segment_len() -> usize;
}

impl<T> ByteRpr for Option<T>
where T: ByteRpr + FixedByteLen
{
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut buff = vec![0];
        match self {
            Some(e) => {
                buff[0] = 1;
                buff.append(&mut e.as_byte_rpr());
            }
            None => {
                buff.append(&mut vec![0; T::segment_len()]);
            }
        }
        buff
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        match bytes[0] {
            1 => Some(T::from_byte_rpr(&bytes[1..])),
            0 => None,
            _ => panic!("Invalid byte pattern"),
        }
    }
}

impl<T> FixedByteLen for Option<T>
where T: ByteRpr + FixedByteLen
{
    fn segment_len() -> usize {
        T::segment_len() + 1
    }
}

impl<T> ByteRpr for Vec<T>
where T: ByteRpr + FixedByteLen
{
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut result = vec![];
        for elem in self {
            result.append(&mut elem.as_byte_rpr());
        }
        result
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let segment_len = T::segment_len();
        let mut deserealized = vec![];
        let mut start = 0;
        let mut end = segment_len;
        while start < bytes.len() {
            deserealized.push(T::from_byte_rpr(&bytes[start..end]));
            start = end;
            end = start + segment_len;
        }
        deserealized
    }
}

impl<K, V> ByteRpr for std::collections::HashMap<K, V>
where
    K: std::hash::Hash + Eq + ByteRpr + FixedByteLen,
    V: ByteRpr + FixedByteLen,
{
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut result = vec![];
        for (k, v) in self {
            result.append(&mut k.as_byte_rpr());
            result.append(&mut v.as_byte_rpr());
        }
        result
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let segment_len = K::segment_len() + V::segment_len();
        let mut deserealized = HashMap::new();
        let mut start = 0;
        let mut end = segment_len;
        while start < bytes.len() {
            let key_start = start;
            let key_end = key_start + K::segment_len();
            let value_start = key_end;
            let value_end = value_start + V::segment_len();
            let key = K::from_byte_rpr(&bytes[key_start..key_end]);
            let value = V::from_byte_rpr(&bytes[value_start..value_end]);
            deserealized.insert(key, value);
            start = end;
            end = start + segment_len;
        }
        deserealized
    }
}

impl ByteRpr for u64 {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let mut buff: [u8; 8] = [0; 8];
        buff.copy_from_slice(bytes);
        u64::from_le_bytes(buff)
    }
}
impl FixedByteLen for u64 {
    fn segment_len() -> usize {
        8
    }
}

impl ByteRpr for u128 {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let mut buff: [u8; 16] = [0; 16];
        buff.copy_from_slice(bytes);
        u128::from_le_bytes(buff)
    }
}
impl FixedByteLen for u128 {
    fn segment_len() -> usize {
        16
    }
}
impl ByteRpr for f32 {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let mut buff: [u8; 4] = [0; 4];
        buff.copy_from_slice(bytes);
        f32::from_le_bytes(buff)
    }
}
impl FixedByteLen for f32 {
    fn segment_len() -> usize {
        4
    }
}

impl ByteRpr for () {
    fn as_byte_rpr(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }
    fn from_byte_rpr(_: &[u8]) -> Self {}
}
impl FixedByteLen for () {
    fn segment_len() -> usize {
        0
    }
}

impl ByteRpr for String {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}

impl ByteRpr for Vec<u8> {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.clone()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}

#[cfg(test)]
mod option_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let elem = Some(0u64);
        let none_elem: Option<u64> = None;
        assert_eq!(Option::from_byte_rpr(&elem.as_byte_rpr()), Some(0u64));
        assert_eq!(Option::from_byte_rpr(&none_elem.as_byte_rpr()), none_elem);
        assert_eq!(elem.as_byte_rpr().len(), Option::<u64>::segment_len());
        assert_eq!(none_elem.as_byte_rpr().len(), Option::<u64>::segment_len());
    }
}
#[cfg(test)]
mod vec_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let vector: Vec<u64> = vec![12; 7];
        let tested: Vec<u64> = Vec::from_byte_rpr(&vector.as_byte_rpr());
        assert_eq!(tested, vector);
    }
}

#[cfg(test)]
mod hashmap_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let map: HashMap<u64, u64> = [(0, 0), (1, 1), (2, 2)].into_iter().collect();
        let tested: HashMap<u64, u64> = HashMap::from_byte_rpr(&map.as_byte_rpr());
        assert_eq!(tested, map);
    }
}
