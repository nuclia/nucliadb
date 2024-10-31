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

mod grpc;

use std::collections::HashMap;

use anyhow::anyhow;
use nidx_vector::config::VectorConfig;
use uuid::Uuid;

use crate::metadata::{Index, Segment, Shard};
use crate::NidxMetadata;

pub struct ShardsServer {
    meta: NidxMetadata,
}

impl ShardsServer {
    pub fn new(meta: NidxMetadata) -> Self {
        Self {
            meta,
        }
    }

    pub async fn create_shard(
        &self,
        kbid: Uuid,
        vector_configs: HashMap<String, VectorConfig>,
    ) -> anyhow::Result<Shard> {
        if vector_configs.is_empty() {
            return Err(anyhow!("Can't create shard without a vector index"));
        }

        let mut tx = self.meta.transaction().await?;
        let shard = Shard::create(&mut *tx, kbid).await?;
        // TODO: support other indexes
        // Index::create(&mut *tx, shard.id, "fulltext", IndexConfig::new_fulltext()).await?;
        // Index::create(&mut *tx, shard.id, "keyword", IndexConfig::new_keyword()).await?;
        // Index::create(&mut *tx, shard.id, "relation", IndexConfig::new_relation()).await?;
        for (vectorset_id, config) in vector_configs.into_iter() {
            Index::create(&mut *tx, shard.id, &vectorset_id, config.into()).await?;
        }
        tx.commit().await?;

        Ok(shard)
    }

    /// Mark a shard, its indexes and segments for eventual deletion
    pub async fn delete_shard(&self, shard_id: Uuid) -> anyhow::Result<()> {
        let mut tx = self.meta.transaction().await?;
        let shard = match Shard::get(&mut *tx, shard_id).await {
            Ok(shard) => shard,
            Err(sqlx::error::Error::RowNotFound) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        for index in shard.indexes(&mut *tx).await?.into_iter() {
            Segment::mark_delete_by_index(&mut *tx, index.id).await?;
            index.mark_delete(&mut *tx).await?;
        }

        shard.mark_delete(&mut *tx).await?;
        tx.commit().await?;

        Ok(())
    }
}
