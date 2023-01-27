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

use crate::*;
use nucliadb_vectors::service as vectors;
pub const MAX_VERSION: u32 = 1;

pub type RVectors = Arc<dyn VectorReader>;
pub type WVectors = Arc<RwLock<dyn VectorWriter>>;

pub fn open_reader(config: &VectorConfig, version: u32) -> NodeResult<RVectors> {
    match version {
        1 => vectors::VectorReaderService::open(config).map(|v| Arc::new(v) as RVectors),
        v => Err(node_error!("Invalid vectors  version {v}")),
    }
}

pub fn open_writer(config: &VectorConfig, version: u32) -> NodeResult<WVectors> {
    match version {
        1 => {
            vectors::VectorWriterService::open(config).map(|v| Arc::new(RwLock::new(v)) as WVectors)
        }
        v => Err(node_error!("Invalid vectors  version {v}")),
    }
}
pub fn create_reader(config: &VectorConfig, version: u32) -> NodeResult<RVectors> {
    match version {
        1 => vectors::VectorReaderService::new(config).map(|v| Arc::new(v) as RVectors),
        v => Err(node_error!("Invalid vectors  version {v}")),
    }
}

pub fn create_writer(config: &VectorConfig, version: u32) -> NodeResult<WVectors> {
    match version {
        1 => {
            vectors::VectorWriterService::new(config).map(|v| Arc::new(RwLock::new(v)) as WVectors)
        }
        v => Err(node_error!("Invalid vectors  version {v}")),
    }
}
