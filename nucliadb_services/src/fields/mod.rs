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

type RFieldsT = dyn RService<Request = DocumentSearchRequest, Response = DocumentSearchResponse>;
type WFieldsT = dyn WService;
pub type RFields = Arc<RFieldsT>;
pub type WFields = Arc<RwLock<WFieldsT>>;

pub async fn open_reader(
    config: &FieldServiceConfiguration,
    version: u32,
) -> InternalResult<RFields> {
    match version {
        0 => nucliadb_fields_tantivy::reader::FieldReaderService::open(config)
            .await
            .map(|v| Arc::new(v) as RFields),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}

pub async fn open_writer(
    config: &FieldServiceConfiguration,
    version: u32,
) -> InternalResult<WFields> {
    match version {
        0 => nucliadb_fields_tantivy::writer::FieldWriterService::open(config)
            .await
            .map(|v| Arc::new(RwLock::new(v)) as WFields),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}
pub async fn create_reader(
    config: &FieldServiceConfiguration,
    version: u32,
) -> InternalResult<RFields> {
    match version {
        0 => nucliadb_fields_tantivy::reader::FieldReaderService::new(config)
            .await
            .map(|v| Arc::new(v) as RFields),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}

pub async fn create_writer(
    config: &FieldServiceConfiguration,
    version: u32,
) -> InternalResult<WFields> {
    match version {
        0 => nucliadb_fields_tantivy::writer::FieldWriterService::new(config)
            .await
            .map(|v| Arc::new(RwLock::new(v)) as WFields),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}
