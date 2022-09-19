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

pub const MAX_VERSION: u32 = 0;

pub type RFields = Arc<dyn FieldReader>;
pub type WFields = Arc<RwLock<dyn FieldWriter>>;

pub fn open_reader(config: &FieldConfig, version: u32) -> InternalResult<RFields> {
    match version {
        0 => nucliadb_fields_tantivy::reader::FieldReaderService::open(config)
            .map(|v| Arc::new(v) as RFields),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}

pub fn open_writer(config: &FieldConfig, version: u32) -> InternalResult<WFields> {
    match version {
        0 => nucliadb_fields_tantivy::writer::FieldWriterService::open(config)
            .map(|v| Arc::new(RwLock::new(v)) as WFields),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}
pub fn create_reader(config: &FieldConfig, version: u32) -> InternalResult<RFields> {
    match version {
        0 => nucliadb_fields_tantivy::reader::FieldReaderService::new(config)
            .map(|v| Arc::new(v) as RFields),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}

pub fn create_writer(config: &FieldConfig, version: u32) -> InternalResult<WFields> {
    match version {
        0 => nucliadb_fields_tantivy::writer::FieldWriterService::new(config)
            .map(|v| Arc::new(RwLock::new(v)) as WFields),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}
