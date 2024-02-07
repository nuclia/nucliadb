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

use std::io::Read;

// VectorIter expects for the vectors to be encoded in the
// following format:
// - vector dimension: u32 in raw little endian
// - the elements in sequence: f32 in raw little endian.
pub struct FileVectors<Contents> {
    reader: Contents,
}

impl<Contents> FileVectors<Contents>
where
    Contents: Read,
{
    pub fn new(reader: Contents) -> FileVectors<Contents> {
        FileVectors {
            reader,
        }
    }
}

impl<Contents> Iterator for FileVectors<Contents>
where
    Contents: Read,
{
    type Item = Vec<f32>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0; 4];
        match self.reader.read_exact(&mut buf) {
            Err(_) => None,
            Ok(_) => {
                let dim = u32::from_le_bytes(buf) as usize;
                let vector = (0..dim)
                    .map(|_| {
                        self.reader.read_exact(&mut buf).unwrap();
                        f32::from_le_bytes(buf)
                    })
                    .collect::<Vec<_>>();
                Some(vector)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};

    use super::*;

    const DIM: u32 = 5;
    const V1: [f32; DIM as usize] = [0.0, 1.1, 2.2, 3.3, 4.4];
    const V2: [f32; DIM as usize] = [5.5, 6.6, 7.7, 8.8, 9.9];
    const V3: [f32; DIM as usize] = [10.10, 11.11, 22.22, 33.33, 44.44];

    struct VecReader<'a> {
        idx: usize,
        buf: &'a [u8],
    }
    impl<'a> Read for VecReader<'a> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let mut i = 0;
            while i < buf.len() && self.idx < self.buf.len() {
                buf[i] = self.buf[self.idx];
                self.idx += 1;
                i += 1;
            }
            Ok(i)
        }
    }

    #[test]
    fn test() {
        let mut buf = vec![];
        buf.write_all(&DIM.to_le_bytes()).unwrap();
        for i in V1 {
            buf.write_all(&i.to_le_bytes()).unwrap();
        }
        buf.write_all(&DIM.to_le_bytes()).unwrap();
        for i in V2 {
            buf.write_all(&i.to_le_bytes()).unwrap();
        }
        buf.write_all(&DIM.to_le_bytes()).unwrap();
        for i in V3 {
            buf.write_all(&i.to_le_bytes()).unwrap();
        }
        let vec_reader = VecReader {
            idx: 0,
            buf: &buf,
        };
        let mut iter = FileVectors::new(vec_reader);
        let v1 = iter.next();
        assert_eq!(v1.as_deref(), Some(V1.as_slice()));
        let v2 = iter.next();
        assert_eq!(v2.as_deref(), Some(V2.as_slice()));
        let v3 = iter.next();
        assert_eq!(v3.as_deref(), Some(V3.as_slice()));
    }
}
