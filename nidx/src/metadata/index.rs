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

use std::{any::TypeId, path::PathBuf};

use anyhow::anyhow;
use nidx_relation::RelationConfig;
use nidx_text::TextConfig;
use nidx_vector::config::VectorConfig;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use sqlx::{
    self, Executor, Postgres,
    types::{JsonValue, time::PrimitiveDateTime},
};
use uuid::Uuid;

use super::segment::Segment;

#[derive(sqlx::Type, Copy, Clone, PartialEq, Eq, Hash, Debug)]
#[sqlx(type_name = "index_kind", rename_all = "lowercase")]
pub enum IndexKind {
    Text,
    Paragraph,
    Vector,
    Relation,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, sqlx::Type, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct IndexId(pub(super) i64);
impl From<i64> for IndexId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}
impl IndexId {
    pub fn local_path(&self) -> PathBuf {
        PathBuf::from(format!("{}", self.0))
    }

    pub(crate) fn sql(&self) -> i64 {
        self.0
    }
}

#[derive(Clone)]
pub struct Index {
    pub id: IndexId,
    pub shard_id: Uuid,
    pub kind: IndexKind,
    pub name: String,
    pub configuration: JsonValue,
    pub updated_at: PrimitiveDateTime,
    pub deleted_at: Option<PrimitiveDateTime>,
}

pub enum IndexConfig {
    Text(TextConfig),
    Paragraph(()),
    Vector(VectorConfig),
    Relation(RelationConfig),
}

impl Index {
    pub async fn create(
        meta: impl Executor<'_, Database = Postgres>,
        shard_id: Uuid,
        name: &str,
        config: IndexConfig,
    ) -> Result<Index, anyhow::Error> {
        let kind = config.kind();
        let json_config = serde_json::to_value(&config)?;
        let inserted = sqlx::query!(
            r#"INSERT INTO indexes (shard_id, kind, name, configuration)
               VALUES ($1, $2, $3, $4)
               RETURNING id AS "id: IndexId", updated_at"#,
            shard_id,
            kind as IndexKind,
            name,
            json_config,
        )
        .fetch_one(meta)
        .await?;
        Ok(Index {
            id: inserted.id,
            shard_id,
            kind,
            name: name.to_owned(),
            configuration: json_config,
            updated_at: inserted.updated_at,
            deleted_at: None,
        })
    }

    pub async fn get(meta: impl Executor<'_, Database = Postgres>, id: IndexId) -> sqlx::Result<Index> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes
               WHERE id = $1 AND deleted_at IS NULL"#,
            id as IndexId
        )
        .fetch_one(meta)
        .await
    }

    pub async fn get_many(meta: impl Executor<'_, Database = Postgres>, ids: &[&IndexId]) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes
               WHERE id = ANY($1) AND deleted_at IS NULL"#,
            ids as &[&IndexId]
        )
        .fetch_all(meta)
        .await
    }

    pub async fn find(
        meta: impl Executor<'_, Database = Postgres>,
        shard_id: Uuid,
        kind: IndexKind,
        name: &str,
    ) -> sqlx::Result<Index> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes
               WHERE shard_id = $1 AND kind = $2 AND name = $3 AND deleted_at IS NULL"#,
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
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes
               WHERE shard_id = $1 AND deleted_at IS NULL"#,
            shard_id
        )
        .fetch_all(meta)
        .await
    }

    pub async fn for_shards(
        meta: impl Executor<'_, Database = Postgres>,
        shard_ids: &[Uuid],
    ) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes
               WHERE shard_id = ANY($1) AND deleted_at IS NULL"#,
            shard_ids
        )
        .fetch_all(meta)
        .await
    }

    pub async fn updated(meta: impl Executor<'_, Database = Postgres>, index_id: &IndexId) -> sqlx::Result<()> {
        sqlx::query!(
            "UPDATE indexes SET updated_at = NOW() WHERE id = $1",
            index_id as &IndexId
        )
        .execute(meta)
        .await?;
        Ok(())
    }

    pub async fn updated_many(meta: impl Executor<'_, Database = Postgres>, index_id: &[IndexId]) -> sqlx::Result<()> {
        sqlx::query!(
            "UPDATE indexes SET updated_at = NOW() WHERE id = ANY($1)",
            index_id as &[IndexId]
        )
        .execute(meta)
        .await?;
        Ok(())
    }

    pub async fn recently_updated(
        meta: impl Executor<'_, Database = Postgres>,
        shards: &Vec<Uuid>,
        newer_than: PrimitiveDateTime,
    ) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes
               WHERE shard_id = ANY($1)
               AND updated_at > $2
               AND deleted_at IS NULL
               ORDER BY updated_at"#,
            shards,
            newer_than
        )
        .fetch_all(meta)
        .await
    }

    pub async fn mark_delete(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!(
            "UPDATE indexes SET deleted_at = NOW(), name = name || '-deleted-' || gen_random_uuid() WHERE id = $1",
            self.id as IndexId
        )
        .execute(meta)
        .await?;
        Ok(())
    }

    pub async fn delete(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("DELETE FROM indexes WHERE id = $1", self.id as IndexId)
            .execute(meta)
            .await?;
        Ok(())
    }

    pub async fn marked_to_delete(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes
               WHERE deleted_at IS NOT NULL"#,
        )
        .fetch_all(meta)
        .await
    }

    pub async fn segments(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Segment>> {
        Segment::in_index(meta, self.id).await
    }

    pub fn config<T: for<'de> Deserialize<'de> + 'static>(&self) -> anyhow::Result<T> {
        let config_type = match self.kind {
            IndexKind::Vector => TypeId::of::<VectorConfig>(),
            IndexKind::Text => TypeId::of::<TextConfig>(),
            IndexKind::Relation => TypeId::of::<RelationConfig>(),
            _ => TypeId::of::<()>(),
        };

        if TypeId::of::<T>() != config_type {
            return Err(anyhow!("Invalid index type while getting configuration"));
        }
        if self.configuration.is_null() {
            // Deserialize null as if it was an empty dictionary for better backwards compatibility
            // when adding configuration to an index for the first time
            Ok(serde_json::from_value::<T>(JsonValue::Object(Map::new()))?)
        } else {
            Ok(serde_json::from_value::<T>(self.configuration.clone())?)
        }
    }
}

impl IndexConfig {
    pub fn kind(&self) -> IndexKind {
        match self {
            Self::Text(_) => IndexKind::Text,
            Self::Paragraph(_) => IndexKind::Paragraph,
            Self::Vector(_) => IndexKind::Vector,
            Self::Relation(_) => IndexKind::Relation,
        }
    }
}

impl Serialize for IndexConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Text(config) => config.serialize(serializer),
            Self::Paragraph(config) => config.serialize(serializer),
            Self::Vector(config) => config.serialize(serializer),
            Self::Relation(config) => config.serialize(serializer),
        }
    }
}

impl From<VectorConfig> for IndexConfig {
    fn from(value: VectorConfig) -> Self {
        Self::Vector(value)
    }
}

impl TryInto<VectorConfig> for IndexConfig {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<VectorConfig, Self::Error> {
        if let Self::Vector(config) = self {
            Ok(config)
        } else {
            let kind = self.kind();
            Err(anyhow!("Can't convert to vector config from {kind:?}"))
        }
    }
}

impl IndexConfig {
    pub fn new_text() -> Self {
        Self::Text(TextConfig::default())
    }

    pub fn new_paragraph() -> Self {
        Self::Paragraph(())
    }

    pub fn new_relation() -> Self {
        Self::Relation(RelationConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use nidx_text::TextConfig;
    use uuid::Uuid;

    use crate::{
        NidxMetadata,
        metadata::{Index, IndexConfig, Shard},
    };

    #[sqlx::test]
    async fn test_text_config(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await.unwrap();

        // Default version for new indexes is 4
        let shard = Shard::create(&meta.pool, Uuid::new_v4()).await.unwrap();
        let index = Index::create(&meta.pool, shard.id, "multilingual", IndexConfig::new_text())
            .await
            .unwrap();
        assert_eq!(index.config::<TextConfig>()?.version, 4);

        // Default version if DB is empty is 1
        sqlx::query("UPDATE indexes SET configuration = NULL")
            .execute(&meta.pool)
            .await?;
        let index = Index::get(&meta.pool, index.id).await?;
        assert_eq!(index.config::<TextConfig>()?.version, 1);

        Ok(())
    }
}
