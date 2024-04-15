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

use nucliadb_core::vectors::{VectorConfig, VectorWriter};
use nucliadb_core::{node_error, NodeResult};
use nucliadb_vectors::service::VectorWriterService;
use std::sync::RwLock;

pub type VectorWPointer = RwLock<Box<dyn VectorWriter>>;

pub fn new(version: u32, config: &VectorConfig) -> NodeResult<VectorWPointer> {
    match version {
        1 => VectorWriterService::start(config).map(|i| RwLock::new(Box::new(i) as Box<dyn VectorWriter>)),
        2 => VectorWriterService::start(config).map(|i| RwLock::new(Box::new(i) as Box<dyn VectorWriter>)),
        v => Err(node_error!("Invalid vectors version {v}")),
    }
}
