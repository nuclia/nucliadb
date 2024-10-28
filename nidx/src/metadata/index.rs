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
use super::segment::Segment;
use sqlx::{
    self,
    types::{time::PrimitiveDateTime, JsonValue},
    Executor, Postgres,
};
use uuid::Uuid;

#[derive(sqlx::Type, Copy, Clone, PartialEq, Debug)]
#[sqlx(type_name = "index_kind", rename_all = "lowercase")]
pub enum IndexKind {
    Text,
    Paragraph,
    Vector,
    Relation,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct IndexId(pub(super) i64);
impl From<i64> for IndexId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

pub struct Index {
    pub id: IndexId,
    pub shard_id: Uuid,
    pub kind: IndexKind,
    pub name: String,
    pub configuration: JsonValue,
    pub updated_at: PrimitiveDateTime,
}

impl Index {
    pub async fn create(
        meta: impl Executor<'_, Database = Postgres>,
        shard_id: Uuid,
        kind: IndexKind,
        name: &str,
    ) -> Result<Index, sqlx::Error> {
        let inserted = sqlx::query!(
            r#"INSERT INTO indexes (shard_id, kind, name) VALUES ($1, $2, $3) RETURNING id AS "id: IndexId", updated_at"#,
            shard_id,
            kind as IndexKind,
            name
        )
        .fetch_one(meta)
        .await?;
        Ok(Index {
            id: inserted.id,
            shard_id,
            kind,
            name: name.to_owned(),
            configuration: JsonValue::Null,
            updated_at: inserted.updated_at,
        })
    }

    pub async fn find(
        meta: impl Executor<'_, Database = Postgres>,
        shard_id: Uuid,
        kind: IndexKind,
        name: &str,
    ) -> sqlx::Result<Index> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at FROM indexes WHERE shard_id = $1 AND kind = $2 AND name = $3"#,
            shard_id,
            kind as IndexKind,
            name
        )
        .fetch_one(meta)
        .await
    }

    pub async fn for_shard(meta: impl Executor<'_, Database = Postgres>, shard_id: Uuid) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at FROM indexes WHERE shard_id = $1"#,
            shard_id
        )
        .fetch_all(meta)
        .await
    }

    pub async fn get(meta: impl Executor<'_, Database = Postgres>, id: IndexId) -> sqlx::Result<Index> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at FROM indexes WHERE id = $1"#,
            id as IndexId
        )
        .fetch_one(meta)
        .await
    }

    pub async fn updated(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("UPDATE indexes SET updated_at = NOW() WHERE id = $1", self.id as IndexId).execute(meta).await?;
        Ok(())
    }

    pub async fn recently_updated(
        meta: impl Executor<'_, Database = Postgres>,
        newer_than: PrimitiveDateTime,
    ) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at FROM indexes WHERE updated_at > $1"#,
            newer_than
        )
        .fetch_all(meta)
        .await
    }

    pub async fn segments(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(Segment, r#"SELECT * FROM segments WHERE index_id = $1"#, self.id as IndexId)
            .fetch_all(meta)
            .await
    }
}
