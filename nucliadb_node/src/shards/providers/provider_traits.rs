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

use std::sync::Arc;

use async_trait::async_trait;
use nucliadb_core::protos::ShardCleaned;
use nucliadb_core::NodeResult;

use crate::shards::metadata::ShardMetadata;
use crate::shards::reader::ShardReader;
use crate::shards::writer::ShardWriter;
use crate::shards::ShardId;

pub trait ShardReaderProvider: Send + Sync {
    fn load(&self, id: ShardId) -> NodeResult<Arc<ShardReader>>;
    fn load_all(&self) -> NodeResult<()>;

    fn get(&self, id: ShardId) -> Option<Arc<ShardReader>>;
}

#[async_trait]
pub trait AsyncShardReaderProvider: Send + Sync {
    async fn load(&self, id: ShardId) -> NodeResult<Arc<ShardReader>>;
    async fn load_all(&self) -> NodeResult<()>;

    async fn get(&self, id: ShardId) -> Option<Arc<ShardReader>>;
}

pub trait ShardWriterProvider {
    fn load(&self, id: ShardId) -> NodeResult<Arc<ShardWriter>>;
    fn load_all(&self) -> NodeResult<()>;

    fn create(&self, metadata: ShardMetadata) -> NodeResult<Arc<ShardWriter>>;
    fn get(&self, id: ShardId) -> Option<Arc<ShardWriter>>;
    fn delete(&self, id: ShardId) -> NodeResult<()>;

    fn upgrade(&self, id: ShardId) -> NodeResult<ShardCleaned>;
}

#[async_trait]
pub trait AsyncShardWriterProvider {
    async fn load(&self, id: ShardId) -> NodeResult<Arc<ShardWriter>>;
    async fn load_all(&self) -> NodeResult<()>;

    async fn create(&self, metadata: ShardMetadata) -> NodeResult<Arc<ShardWriter>>;
    async fn get(&self, id: ShardId) -> Option<Arc<ShardWriter>>;
    async fn delete(&self, id: ShardId) -> NodeResult<()>;

    async fn upgrade(&self, id: ShardId) -> NodeResult<ShardCleaned>;
}
