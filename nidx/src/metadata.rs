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

mod index;
mod segment;
mod shard;

pub use index::Index;
pub use index::IndexKind;
pub use segment::Segment;
pub use shard::Shard;

pub struct NidxMetadata {
    pub pool: sqlx::PgPool,
}

#[derive(Debug)]
struct MultiMigrator(Vec<sqlx::migrate::Migrator>);
impl<'s> sqlx::migrate::MigrationSource<'s> for MultiMigrator {
    fn resolve(
        self,
    ) -> futures::future::BoxFuture<'s, Result<Vec<sqlx::migrate::Migration>, sqlx::error::BoxDynError>> {
        Box::pin(async move { Ok(self.0.iter().flat_map(|m| m.iter().cloned()).collect()) })
    }
}

impl NidxMetadata {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new().connect(database_url).await?;

        Self::new_with_pool(pool).await
    }

    pub(crate) async fn new_with_pool(pool: sqlx::PgPool) -> Result<Self, sqlx::Error> {
        sqlx::migrate::Migrator::new(MultiMigrator(vec![
            sqlx::migrate!("./migrations"),
            apalis::postgres::PostgresStorage::migrations(),
        ]))
        .await?
        .run(&pool)
        .await?;
        Ok(NidxMetadata {
            pool,
        })
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
        let shard = Shard::create(&meta, kbid).await.unwrap();
        assert_eq!(shard.kbid, kbid);

        let index = Index::create(&meta, shard.id, IndexKind::Vector, Some("multilingual")).await.unwrap();
        assert_eq!(index.shard_id, shard.id);
        assert_eq!(index.kind, IndexKind::Vector);
        assert_eq!(index.name, Some("multilingual".into()));

        let found = Index::find(&meta, shard.id, IndexKind::Vector, Some("multilingual")).await.unwrap();
        assert_eq!(found.id, index.id);
        assert_eq!(found.shard_id, shard.id);
        assert_eq!(found.kind, IndexKind::Vector);
        assert_eq!(found.name, Some("multilingual".into()));
    }
}
