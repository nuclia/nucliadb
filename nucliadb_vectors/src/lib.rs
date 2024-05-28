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

pub mod config;
pub mod data_point;
pub mod data_point_provider;
mod data_types;
pub mod formula;
pub mod service;
mod utils;
mod vector_types;

use thiserror::Error;
#[derive(Debug, Error)]
pub enum VectorErr {
    #[error("Error using bincode: {0}")]
    BincodeError(#[from] bincode::Error),
    #[error("Error using fst: {0}")]
    FstError(#[from] fst::Error),
    #[error("json error: {0}")]
    SJ(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    IoErr(#[from] std::io::Error),
    #[error("Error in fs: {0}")]
    FsError(#[from] nucliadb_core::fs_state::FsError),
    #[error("This index does not have an alive writer")]
    NoWriterError,
    #[error("Only one writer can be open at the same time")]
    MultipleWritersError,
    #[error("Writer has uncommitted changes, please commit or abort")]
    UncommittedChangesError,
    #[error("Garbage collection delayed")]
    WorkDelayed,
    #[error("Merger is already initialized")]
    MergerAlreadyInitialized,
    #[error("Can not merge zero datapoints")]
    EmptyMerge,
    #[error("Inconsistent dimensions")]
    InconsistentDimensions,
    #[error("UTF8 decoding error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Some of the merged segments were not found")]
    MissingMergedSegments,
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(&'static str),
}

pub type VectorR<O> = Result<O, VectorErr>;
