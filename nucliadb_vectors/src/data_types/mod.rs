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

pub mod dtrie_ram;
pub mod key_value;
pub mod trie;
pub mod trie_ram;
pub mod vector;

pub mod usize_utils {
    pub const USIZE_LEN: usize = (usize::BITS / 8) as usize;
    pub fn usize_from_slice_le(v: &[u8]) -> usize {
        let mut buff = [0; USIZE_LEN];
        buff.copy_from_slice(v);
        usize::from_le_bytes(buff)
    }
}

pub trait DeleteLog: std::marker::Sync {
    fn is_deleted(&self, _: &str) -> bool;
}

impl<'a, D: DeleteLog> DeleteLog for &'a D {
    fn is_deleted(&self, x: &str) -> bool {
        D::is_deleted(self, x)
    }
}

impl<Prop: std::marker::Sync> DeleteLog for dtrie_ram::DTrie<Prop> {
    fn is_deleted(&self, key: &str) -> bool {
        self.get(key.as_bytes()).is_some()
    }
}

impl<Dl: DeleteLog, S: key_value::Slot> key_value::Slot for (Dl, S) {
    fn get_key<'a>(&self, x: &'a [u8]) -> &'a [u8] {
        self.1.get_key(x)
    }
    fn cmp_keys(&self, x: &[u8], key: &[u8]) -> std::cmp::Ordering {
        self.1.cmp_keys(x, key)
    }
    fn read_exact<'a>(&self, x: &'a [u8]) -> (/* head */ &'a [u8], /* tail */ &'a [u8]) {
        self.1.read_exact(x)
    }
    fn keep_in_merge(&self, x: &[u8]) -> bool {
        let key = std::str::from_utf8(self.get_key(x)).unwrap();
        !self.0.is_deleted(key)
    }
}
