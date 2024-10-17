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
use super::{segment::Segment, NidxMetadata};
use sqlx::{self, types::JsonValue};
use uuid::Uuid;

#[derive(sqlx::Type, Copy, Clone, PartialEq, Debug)]
#[sqlx(type_name = "index_kind", rename_all = "lowercase")]
pub enum IndexKind {
    Text,
    Paragraph,
    Vector,
    Relation,
}

#[derive(Copy, Clone, Debug, PartialEq, sqlx::Type)]
pub struct IndexId(i64);
impl From<i64> for IndexId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

pub struct Index {
    pub id: IndexId,
    pub shard_id: Uuid,
    pub kind: IndexKind,
    pub name: Option<String>,
    pub configuration: JsonValue,
}

impl Index {
    pub async fn create(
        meta: &NidxMetadata,
        shard_id: Uuid,
        kind: IndexKind,
        name: Option<&str>,
    ) -> Result<Index, sqlx::Error> {
        let id: IndexId = sqlx::query_scalar!(
            r#"INSERT INTO indexes (shard_id, kind, name) VALUES ($1, $2, $3) RETURNING id"#,
            shard_id,
            kind as IndexKind,
            name
        )
        .fetch_one(&meta.pool)
        .await?
        .into();
        Ok(Index {
            id,
            shard_id,
            kind,
            name: name.map(|s| s.to_owned()),
            configuration: JsonValue::Null,
        })
    }

    pub async fn find(meta: &NidxMetadata, shard_id: Uuid, kind: IndexKind, name: Option<&str>) -> sqlx::Result<Index> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration FROM indexes WHERE shard_id = $1 AND kind = $2 AND name = $3"#,
            shard_id,
            kind as IndexKind,
            name
        )
        .fetch_one(&meta.pool)
        .await
    }

    pub async fn for_shard(meta: &NidxMetadata, shard_id: Uuid) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration FROM indexes WHERE shard_id = $1"#,
            shard_id
        )
        .fetch_all(&meta.pool)
        .await
    }

    pub async fn get(meta: &NidxMetadata, id: IndexId) -> sqlx::Result<Index> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration FROM indexes WHERE id = $1"#,
            id as IndexId
        )
        .fetch_one(&meta.pool)
        .await
    }

    pub async fn segments(&self, meta: &NidxMetadata) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(Segment, r#"SELECT * FROM segments WHERE index_id = $1"#, self.id as IndexId)
            .fetch_all(&meta.pool)
            .await
    }
}
