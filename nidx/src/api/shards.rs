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

use nidx_vector::config::VectorConfig;
use uuid::Uuid;

use crate::NidxMetadata;
use crate::errors::{NidxError, NidxResult};
use crate::metadata::{Index, IndexConfig, IndexKind, MergeJob, Segment, Shard};

pub async fn create_shard(
    meta: &NidxMetadata,
    kbid: Uuid,
    vector_configs: Vec<(String, VectorConfig)>,
) -> NidxResult<Shard> {
    if vector_configs.is_empty() {
        return Err(NidxError::invalid("Can't create shard without a vector index"));
    }

    let mut tx = meta.transaction().await?;
    let shard = Shard::create(&mut *tx, kbid).await?;
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

/// Configure pre-warm for multiple shards. If at some point we have more things
/// to configure, don't hesitate to replace the bool for a struct and rename
/// this to configure_shards or whatever
pub async fn configure_prewarm(meta: &NidxMetadata, shard_configs: HashMap<Uuid, bool>) -> NidxResult<()> {
    // REVIEW: accessing the JSON value here breaks an abstraction, but
    // using a transaction and one update per index is costly. We prefer the
    // slow query rather than breaking the abstraction although there's
    // probably a better way

    let mut tx = meta.transaction().await.map_err(NidxError::from)?;

    let shard_ids: Vec<Uuid> = shard_configs.keys().cloned().collect();
    let kind = IndexKind::Vector;
    let indexes: Vec<Index> = sqlx::query_as!(
        Index,
        r#"SELECT indexes.id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, indexes.deleted_at
               FROM indexes
               JOIN shards ON indexes.shard_id = shards.id
               WHERE shard_id = ANY($1) AND indexes.kind = $2
               FOR UPDATE
            "#,
        &shard_ids,
        kind as IndexKind,
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(NidxError::from)?;

    for index in indexes {
        let prewarm = *shard_configs.get(&index.shard_id).unwrap();
        let mut config = index.config::<VectorConfig>().unwrap();
        if prewarm != config.prewarm {
            config.prewarm = prewarm;
            Index::update_config(&mut *tx, &index.id, IndexConfig::from(config))
                .await
                .map_err(NidxError::from)?;
        }
    }

    tx.commit().await.map_err(NidxError::from)?;

    Ok(())
}
