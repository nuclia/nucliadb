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

type RServiceT = dyn RService<Request = VectorSearchRequest, Response = VectorSearchResponse>;
type WServiceT = dyn WService;
pub type RVectors = Arc<RServiceT>;
pub type WVectors = Arc<RwLock<WServiceT>>;

pub async fn open_reader(
    config: &VectorServiceConfiguration,
    version: u32,
) -> InternalResult<RVectors> {
    match version {
        0 => nucliadb_vectors::service::VectorReaderService::open(config)
            .await
            .map(|v| Arc::new(v) as RVectors),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}

pub async fn open_writer(
    config: &VectorServiceConfiguration,
    version: u32,
) -> InternalResult<WVectors> {
    match version {
        0 => nucliadb_vectors::service::VectorWriterService::open(config)
            .await
            .map(|v| Arc::new(RwLock::new(v)) as WVectors),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}
pub async fn create_reader(
    config: &VectorServiceConfiguration,
    version: u32,
) -> InternalResult<RVectors> {
    match version {
        0 => nucliadb_vectors::service::VectorReaderService::new(config)
            .await
            .map(|v| Arc::new(v) as RVectors),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}

pub async fn create_writer(
    config: &VectorServiceConfiguration,
    version: u32,
) -> InternalResult<WVectors> {
    match version {
        0 => nucliadb_vectors::service::VectorWriterService::new(config)
            .await
            .map(|v| Arc::new(RwLock::new(v)) as WVectors),
        v => Err(Box::new(ServiceError::InvalidShardVersion(v).to_string())),
    }
}
