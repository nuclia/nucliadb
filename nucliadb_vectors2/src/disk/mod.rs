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

pub mod key_value;
pub mod trie;
pub mod vector;

pub mod prelude {
    pub use key_value::Slot;
    pub use {bincode, serde};

    pub use super::{key_value, trie, usize_utils, vector};
}

pub mod usize_utils {
    pub const USIZE_LEN: usize = (usize::BITS / 8) as usize;
    pub fn usize_from_slice_le(v: &[u8]) -> usize {
        let mut buff = [0; USIZE_LEN];
        buff.copy_from_slice(v);
        usize::from_le_bytes(buff)
    }
}
