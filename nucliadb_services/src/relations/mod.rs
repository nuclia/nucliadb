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
use nucliadb_service_interface::relations_interface::*;

use crate::*;
pub const MAX_VERSION: u32 = 0;

pub type RRelations = Arc<dyn RelationReader>;
pub type WRelations = Arc<RwLock<dyn RelationWriter>>;

pub fn open_reader(config: &RelationConfig, version: u32) -> InternalResult<RRelations> {
    match version {
        0 => nucliadb_relations::service::RelationsReaderService::start(config)
            .map(|v| Arc::new(v) as RRelations),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}

pub fn open_writer(config: &RelationConfig, version: u32) -> InternalResult<WRelations> {
    match version {
        0 => nucliadb_relations::service::RelationsWriterService::start(config)
            .map(|v| Arc::new(RwLock::new(v)) as WRelations),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}
pub fn create_reader(config: &RelationConfig, version: u32) -> InternalResult<RRelations> {
    match version {
        0 => nucliadb_relations::service::RelationsReaderService::new(config)
            .map(|v| Arc::new(v) as RRelations),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}

pub fn create_writer(config: &RelationConfig, version: u32) -> InternalResult<WRelations> {
    match version {
        0 => nucliadb_relations::service::RelationsWriterService::new(config)
            .map(|v| Arc::new(RwLock::new(v)) as WRelations),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}
