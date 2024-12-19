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

use thiserror::Error;

pub type NidxResult<T> = Result<T, NidxError>;

/// Error type for nidx. The idea is not to be exhaustive, but just include
/// enough variants to be able to map application errors to API errors
#[derive(Error, Debug)]
pub enum NidxError {
    #[error("Not found")]
    NotFound,
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Invalid uuid: {0}")]
    InvalidUuid(#[from] uuid::Error),
    #[error(transparent)]
    DatabaseError(sqlx::Error),
    #[error(transparent)]
    TokioTaskError(#[from] tokio::task::JoinError),
    #[error(transparent)]
    GrpcError(#[from] tonic::transport::Error),
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

impl NidxError {
    pub fn invalid(message: &str) -> Self {
        Self::InvalidRequest(message.to_string())
    }
}

impl From<sqlx::Error> for NidxError {
    fn from(value: sqlx::Error) -> Self {
        match value {
            sqlx::Error::RowNotFound => Self::NotFound,
            e => Self::DatabaseError(e),
        }
    }
}

impl From<NidxError> for tonic::Status {
    fn from(value: NidxError) -> Self {
        match value {
            NidxError::NotFound => tonic::Status::not_found("Not found"),
            NidxError::InvalidRequest(_) => tonic::Status::invalid_argument(value.to_string()),
            NidxError::InvalidUuid(_) => tonic::Status::invalid_argument(value.to_string()),
            _ => tonic::Status::internal(value.to_string()),
        }
    }
}
