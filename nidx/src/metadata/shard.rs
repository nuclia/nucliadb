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
use super::index::*;
use sqlx::{types::time::PrimitiveDateTime, Executor, Postgres};
use uuid::Uuid;

pub struct Shard {
    pub id: Uuid,
    pub kbid: Uuid,
    pub deleted_at: Option<PrimitiveDateTime>,
}

impl Shard {
    pub async fn create(meta: impl Executor<'_, Database = Postgres>, kbid: Uuid) -> sqlx::Result<Shard> {
        sqlx::query_as!(Shard, "INSERT INTO shards (kbid) VALUES ($1) RETURNING *", kbid).fetch_one(meta).await
    }

    pub async fn get(meta: impl Executor<'_, Database = Postgres>, id: Uuid) -> sqlx::Result<Shard> {
        sqlx::query_as!(Shard, "SELECT * FROM shards WHERE id = $1", id).fetch_one(meta).await
    }

    pub async fn mark_delete(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("UPDATE shards SET deleted_at = NOW() WHERE id = $1", self.id).execute(meta).await?;
        Ok(())
    }

    pub async fn delete(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("DELETE FROM shards WHERE id = $1", self.id).execute(meta).await?;
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
}
