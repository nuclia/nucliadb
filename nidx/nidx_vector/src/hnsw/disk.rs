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

mod v1;
mod v2;

use crate::{VectorR, hnsw::RAMHnsw};
use std::{any::Any, path::Path};

pub use v1::DiskHnswV1;
pub use v2::DiskHnswV2;

pub fn open_disk_hnsw(path: &Path, prewarm: bool) -> VectorR<Box<dyn DiskHnsw>> {
    // let v2 = DiskHnswV2::open(path, prewarm);
    // if let Ok(v2) = v2 {
    //     return Ok(Box::new(v2));
    // };
    let v1 = DiskHnswV1::open(path, prewarm)?;
    Ok(Box::new(v1))
}

pub trait DiskHnsw: Sync + Send {
    fn deserialize(&self) -> VectorR<RAMHnsw>;
    fn size(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
}
