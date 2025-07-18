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

mod disk_hnsw;
mod ops_hnsw;
mod params;
mod ram_hnsw;

pub use disk_hnsw::DiskHnsw;
pub use ops_hnsw::Cnx;
pub use ops_hnsw::DataRetriever;
pub use ops_hnsw::HnswOps;
pub use ram_hnsw::RAMHnsw;

use crate::VectorAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Address(pub(super) usize);

// VectorAddr and HNSW Address are the same thing with a different data type for serialization purposes
impl From<Address> for VectorAddr {
    fn from(value: Address) -> Self {
        Self(value.0 as u32)
    }
}

impl From<VectorAddr> for Address {
    fn from(value: VectorAddr) -> Self {
        Self(value.0 as usize)
    }
}
