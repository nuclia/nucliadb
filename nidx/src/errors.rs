// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
    TransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcError(tonic::Status),
    #[error("Query error: {0}")]
    QueryError(String),
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
            NidxError::GrpcError(status) => status,
            _ => tonic::Status::internal(value.to_string()),
        }
    }
}
