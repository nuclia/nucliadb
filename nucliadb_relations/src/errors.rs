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

use nucliadb_core::fs_state::FsError;
use tantivy::TantivyError;

#[derive(Debug, thiserror::Error)]
pub enum RelationsErr {
    #[error("Graph error: {0}")]
    GraphDBError(String),
    #[error("Bincode error: {0}")]
    BincodeError(#[from] bincode::Error),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Disk error: {0}")]
    DiskError(#[from] FsError),
    #[error("Tantivy error: {0}")]
    TantivyError(#[from] TantivyError),
    #[error("Database is full")]
    NeedsResize,
    #[error("UBehaviour")]
    UBehaviour,
}

impl From<heed::Error> for RelationsErr {
    fn from(err: heed::Error) -> Self {
        use heed::{Error, MdbError};
        match err {
            Error::Mdb(MdbError::MapFull) => RelationsErr::NeedsResize,
            err => RelationsErr::GraphDBError(format!("{err:?}")),
        }
    }
}
pub type RResult<O> = Result<O, RelationsErr>;
