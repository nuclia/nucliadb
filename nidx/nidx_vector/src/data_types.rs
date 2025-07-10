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

pub mod usize_utils {
    pub const USIZE_LEN: usize = (usize::BITS / 8) as usize;
    pub fn usize_from_slice_le(v: &[u8]) -> usize {
        let mut buff = [0; USIZE_LEN];
        buff.copy_from_slice(v);
        usize::from_le_bytes(buff)
    }
    pub const U32_LEN: usize = (u32::BITS / 8) as usize;
    pub fn u32_from_slice_le(v: &[u8]) -> u32 {
        let mut buff = [0; U32_LEN];
        buff.copy_from_slice(v);
        u32::from_le_bytes(buff)
    }
}
