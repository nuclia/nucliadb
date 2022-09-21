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
use nucliadb_protos::{Resource, ResourceId};

pub trait InternalError: std::fmt::Display + std::fmt::Debug + Send {}
pub type InternalResult<R> = Result<R, Box<dyn InternalError>>;

pub enum ServiceError {
    GenericErr(Box<dyn std::error::Error>),
    NodeOpErr(Box<dyn InternalError>),
    IOErr(std::io::Error),
    InvalidShardVersion(u32),
}

impl From<Box<dyn std::error::Error>> for ServiceError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        ServiceError::GenericErr(error)
    }
}
impl From<Box<dyn InternalError>> for ServiceError {
    fn from(error: Box<dyn InternalError>) -> Self {
        ServiceError::NodeOpErr(error)
    }
}
impl From<std::io::Error> for ServiceError {
    fn from(error: std::io::Error) -> Self {
        ServiceError::IOErr(error)
    }
}

impl std::error::Error for ServiceError {}
impl std::fmt::Debug for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceError::GenericErr(e) => write!(f, "{:?}", e),
            ServiceError::NodeOpErr(e) => write!(f, "{:?}", e),
            ServiceError::IOErr(e) => write!(f, "{:?}", e),
            ServiceError::InvalidShardVersion(v) => write!(f, "Shard version not found {}", v),
        }
    }
}
impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceError::GenericErr(e) => write!(f, "{}", e),
            ServiceError::NodeOpErr(e) => write!(f, "{}", e),
            ServiceError::IOErr(e) => write!(f, "{}", e),
            ServiceError::InvalidShardVersion(v) => write!(f, "Shard version not found {}", v),
        }
    }
}

impl InternalError for String {}

pub type ServiceResult<V> = Result<V, ServiceError>;

pub trait WriterChild: std::fmt::Debug + Send + Sync {
    fn set_resource(&mut self, resource: &Resource) -> InternalResult<()>;
    fn delete_resource(&mut self, resource_id: &ResourceId) -> InternalResult<()>;
    fn garbage_collection(&mut self);
    fn stop(&mut self) -> InternalResult<()>;
    fn count(&self) -> usize;
}

pub trait ReaderChild: std::fmt::Debug + Send + Sync {
    type Request;
    type Response;
    fn search(&self, request: &Self::Request) -> InternalResult<Self::Response>;
    fn reload(&self);
    fn stored_ids(&self) -> Vec<String>;
    fn stop(&self) -> InternalResult<()>;
    fn count(&self) -> usize;
}
