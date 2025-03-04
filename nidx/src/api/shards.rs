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

use std::collections::HashMap;

use nidx_vector::config::VectorConfig;
use uuid::Uuid;

use crate::NidxMetadata;
use crate::errors::{NidxError, NidxResult};
use crate::metadata::{Index, IndexConfig, IndexKind, MergeJob, Segment, Shard};

pub async fn create_shard(
    meta: &NidxMetadata,
    kbid: Uuid,
    vector_configs: HashMap<String, VectorConfig>,
) -> NidxResult<Shard> {
    if vector_configs.is_empty() {
        return Err(NidxError::invalid("Can't create shard without a vector index"));
    }

    let mut tx = meta.transaction().await?;
    let shard = Shard::create(&mut *tx, kbid).await?;
    // TODO: Rename to be closer to API naming? Includes changing DB type, Kind enums, etc.
    Index::create(&mut *tx, shard.id, "text", IndexConfig::new_text()).await?;
    Index::create(&mut *tx, shard.id, "paragraph", IndexConfig::new_paragraph()).await?;
    Index::create(&mut *tx, shard.id, "relation", IndexConfig::new_relation()).await?;
    for (vectorset_id, config) in vector_configs.into_iter() {
        Index::create(&mut *tx, shard.id, &vectorset_id, config.into()).await?;
    }
    tx.commit().await?;

    Ok(shard)
}

/// Mark a shard, its indexes and segments for eventual deletion. Delete merge
/// jobs scheduled for its indexes, as we don't want to keep working on it.
/// Segment deletions will be purged eventually by the worker.
pub async fn delete_shard(meta: &NidxMetadata, shard_id: Uuid) -> NidxResult<()> {
    let mut tx = meta.transaction().await?;
    let shard = match Shard::get(&mut *tx, shard_id).await {
        Ok(shard) => shard,
        Err(sqlx::error::Error::RowNotFound) => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    for index in shard.indexes(&mut *tx).await?.into_iter() {
        MergeJob::delete_many_by_index(&mut *tx, index.id).await?;
        Segment::mark_delete_by_index(&mut *tx, index.id).await?;
        index.mark_delete(&mut *tx).await?;
    }

    shard.mark_delete(&mut *tx).await?;
    tx.commit().await?;

    Ok(())
}

pub async fn delete_vectorset(meta: &NidxMetadata, shard_id: Uuid, vectorset: &str) -> NidxResult<()> {
    let mut tx = meta.transaction().await?;

    let count = Index::for_shard(&mut *tx, shard_id)
        .await?
        .iter()
        .filter(|i| i.kind == IndexKind::Vector)
        .count();
    if count <= 1 {
        return Err(NidxError::InvalidRequest("Can't delete the last vectorset".to_string()));
    }

    let index = Index::find(&mut *tx, shard_id, IndexKind::Vector, vectorset).await?;
    MergeJob::delete_many_by_index(&mut *tx, index.id).await?;
    Segment::mark_delete_by_index(&mut *tx, index.id).await?;
    index.mark_delete(&mut *tx).await?;

    tx.commit().await?;

    Ok(())
}
