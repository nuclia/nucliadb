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

use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use nucliadb_core::protos::ShardCleaned;
use nucliadb_core::NodeResult;

use super::{ShardReader, ShardWriter};
use crate::shard_metadata::ShardMetadata;

pub type ShardId = String;

pub trait ReaderShardsProvider: Send + Sync {
    fn load(&self, id: ShardId) -> NodeResult<()>;
    fn load_all(&self) -> NodeResult<()>;

    fn get(&self, id: ShardId) -> Option<Arc<ShardReader>>;
}

#[async_trait]
pub trait AsyncReaderShardsProvider: Send + Sync {
    async fn load(&self, id: ShardId) -> NodeResult<()>;
    async fn load_all(&self) -> NodeResult<()>;

    async fn get(&self, id: ShardId) -> Option<Arc<ShardReader>>;
}

#[async_trait]
pub trait AsyncWriterShardsProvider {
    async fn load(&self, id: ShardId) -> NodeResult<()>;
    async fn load_all(&self) -> NodeResult<()>;

    async fn create(&self, metadata: ShardMetadata) -> NodeResult<ShardWriter>;
    async fn get(&self, id: ShardId) -> Option<Arc<ShardWriter>>;
    async fn delete(&self, id: ShardId) -> NodeResult<()>;

    async fn upgrade(&self, id: ShardId) -> NodeResult<ShardCleaned>;
}

#[derive(Debug)]
pub struct ShardNotFoundError(pub &'static str);

impl Display for ShardNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Shard not found: {}", self.0)
    }
}
