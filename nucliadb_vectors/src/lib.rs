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

pub mod data_point;
pub mod data_point_provider;
mod data_types;
pub mod formula;
pub mod service;
use tempfile::PersistError;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum VectorErr {
    #[error("json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    BincodeError(#[from] bincode::Error),
    #[error("A file update could not be performed: {0}")]
    UpdateError(#[from] PersistError),
    #[error("Work lost due to inconsistent merges: {0}")]
    MergeLostError(data_point::DpId),
    #[error("The operation is blocked by another one")]
    WouldBlockError,
    #[error("Merger is already initialized")]
    MergerExistsError,
    #[error("Can not merge zero datapoints")]
    EmptyMergeError,
    #[error("A writer is already open for this index")]
    WriterExistsError,
}

pub type VectorR<O> = Result<O, VectorErr>;
