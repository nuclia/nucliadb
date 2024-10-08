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
use sqlx;
use uuid::Uuid;

pub struct NidxMetadata {
    pool: sqlx::PgPool,
}

pub struct Shard {
    pub id: Uuid,
    pub kbid: Uuid,
}

#[derive(sqlx::Type, Copy, Clone, PartialEq, Debug)]
#[sqlx(type_name = "index_kind", rename_all = "lowercase")]
pub enum IndexKind {
    Text,
    Paragraph,
    Vector,
    Relation,
}

pub struct Index {
    pub id: i64,
    pub shard_id: Uuid,
    pub kind: IndexKind,
    pub name: Option<String>,
}

pub struct Segment {
    pub id: i64,
    pub index_id: i64,
}

impl NidxMetadata {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new().connect(database_url).await?;

        Self::new_with_pool(pool).await
    }

    pub(crate) async fn new_with_pool(pool: sqlx::PgPool) -> Result<Self, sqlx::Error> {
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(NidxMetadata {
            pool,
        })
    }

    pub async fn create_shard(&self, kbid: Uuid) -> Result<Shard, sqlx::Error> {
        let id =
            sqlx::query!("INSERT INTO shards (kbid) VALUES ($1) RETURNING id", kbid).fetch_one(&self.pool).await?.id;
        Ok(Shard {
            id,
            kbid,
        })
    }

    pub async fn create_index(
        &self,
        shard_id: Uuid,
        kind: IndexKind,
        name: Option<&str>,
    ) -> Result<Index, sqlx::Error> {
        let id = sqlx::query!(
            r#"INSERT INTO indexes (shard_id, kind, name) VALUES ($1, $2, $3) RETURNING id"#,
            shard_id,
            kind as IndexKind,
            name
        )
        .fetch_one(&self.pool)
        .await?
        .id;
        Ok(Index {
            id,
            shard_id,
            kind,
            name: name.map(|s| s.to_owned()),
        })
    }

    pub async fn find_index(&self, shard_id: Uuid, kind: IndexKind, name: Option<&str>) -> sqlx::Result<Index> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind AS "kind: IndexKind", name FROM indexes where shard_id = $1 AND kind = $2 AND name = $3"#,
            shard_id,
            kind as IndexKind,
            name
        ).fetch_one(&self.pool).await
    }

    pub async fn get_indexes_for_shard(&self, shard_id: Uuid) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind AS "kind: IndexKind", name FROM indexes where shard_id = $1"#,
            shard_id
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn create_segment(&self, index_id: i64) -> sqlx::Result<Segment> {
        let id = sqlx::query!(r#"INSERT INTO segments (index_id) VALUES ($1) RETURNING id"#, index_id)
            .fetch_one(&self.pool)
            .await?
            .id;
        Ok(Segment {
            id,
            index_id,
        })
    }

    pub async fn get_segments(&self, index_id: i64) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(Segment, r#"SELECT id, index_id FROM segments WHERE index_id = $1"#, index_id)
            .fetch_all(&self.pool)
            .await
    }
}

#[cfg(test)]
mod tests {
    use sqlx;
    use uuid::Uuid;

    use super::*;

    #[sqlx::test(migrations = false)]
    async fn create_and_find_index(pool: sqlx::PgPool) {
        let meta = NidxMetadata::new_with_pool(pool).await.unwrap();
        let kbid = Uuid::new_v4();
        let shard = meta.create_shard(kbid).await.unwrap();
        assert_eq!(shard.kbid, kbid);

        let index = meta.create_index(shard.id, IndexKind::Vector, Some("multilingual")).await.unwrap();
        assert_eq!(index.shard_id, shard.id);
        assert_eq!(index.kind, IndexKind::Vector);
        assert_eq!(index.name, Some("multilingual".into()));

        let found = meta.find_index(shard.id, IndexKind::Vector, Some("multilingual")).await.unwrap();
        assert_eq!(found.id, index.id);
        assert_eq!(found.shard_id, shard.id);
        assert_eq!(found.kind, IndexKind::Vector);
        assert_eq!(found.name, Some("multilingual".into()));
    }
}
