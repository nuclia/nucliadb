use std::collections::HashMap;

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
use super::index::*;
use futures::StreamExt;
use sqlx::{Executor, Postgres, types::time::PrimitiveDateTime};
use uuid::Uuid;

pub struct Shard {
    pub id: Uuid,
    pub kbid: Uuid,
    pub deleted_at: Option<PrimitiveDateTime>,
}

pub struct IndexStats {
    pub records: i64,
    pub size_bytes: i64,
}

impl Shard {
    pub async fn create(meta: impl Executor<'_, Database = Postgres>, kbid: Uuid) -> sqlx::Result<Shard> {
        sqlx::query_as!(Shard, "INSERT INTO shards (kbid) VALUES ($1) RETURNING *", kbid)
            .fetch_one(meta)
            .await
    }

    pub async fn get(meta: impl Executor<'_, Database = Postgres>, id: Uuid) -> sqlx::Result<Shard> {
        sqlx::query_as!(Shard, "SELECT * FROM shards WHERE id = $1 AND deleted_at IS NULL", id)
            .fetch_one(meta)
            .await
    }

    pub async fn mark_delete(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("UPDATE shards SET deleted_at = NOW() WHERE id = $1", self.id)
            .execute(meta)
            .await?;
        Ok(())
    }

    pub async fn indexes(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes where shard_id = $1"#,
            self.id
        )
        .fetch_all(meta)
        .await
    }

    pub async fn stats(
        &self,
        meta: impl Executor<'_, Database = Postgres>,
    ) -> sqlx::Result<HashMap<IndexKind, IndexStats>> {
        let mut stats = HashMap::new();
        let mut results = sqlx::query!(
            r#"SELECT
                   kind as "kind: IndexKind",
                   SUM(records)::bigint as "records!",
                   SUM(size_bytes)::bigint as "size_bytes!"
               FROM indexes
               JOIN segments ON index_id = indexes.id
               WHERE shard_id = $1
               AND indexes.deleted_at IS NULL
               AND segments.delete_at IS NULL
               GROUP BY kind"#,
            self.id
        )
        .fetch(meta);
        while let Some(record) = results.next().await {
            let record = record?;
            stats.insert(
                record.kind,
                IndexStats {
                    records: record.records,
                    size_bytes: record.size_bytes,
                },
            );
        }

        Ok(stats)
    }

    pub async fn list_ids(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Uuid>> {
        sqlx::query_scalar("SELECT id FROM shards WHERE deleted_at IS NULL")
            .fetch_all(meta)
            .await
    }
}
