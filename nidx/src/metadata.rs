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

mod deletion;
mod index;
mod index_request;
mod merge_job;
mod segment;
mod shard;

use std::time::Duration;

pub use deletion::*;
pub use index::*;
pub use index_request::*;
pub use merge_job::*;
pub use segment::*;
pub use shard::*;

use crate::settings::MetadataSettings;

/// A random ID to identify the lock we use during migration
const MIGRATION_LOCK_ID: i64 = 5324678839066546102;

#[derive(Clone, Debug)]
pub struct NidxMetadata {
    pub pool: sqlx::PgPool,
}

impl NidxMetadata {
    pub async fn new(settings: &MetadataSettings) -> sqlx::Result<Self> {
        let connect_options: sqlx::postgres::PgConnectOptions = settings
            .database_url
            .parse()
            .map_err(|e| sqlx::Error::Configuration(format!("Invalid database URL: {e}").into()))?;

        let mut pool_options = sqlx::postgres::PgPoolOptions::new().acquire_timeout(Duration::from_secs(2));

        if let Some(idle_timeout_seconds) = settings.idle_timeout_seconds {
            pool_options = pool_options.idle_timeout(Duration::from_secs(idle_timeout_seconds));
        }

        if let Some(max_lifetime_seconds) = settings.max_lifetime_seconds {
            pool_options = pool_options.max_lifetime(Duration::from_secs(max_lifetime_seconds));
        }

        let pool = pool_options.connect_with(connect_options).await?;

        if !settings.disable_migrations {
            Self::run_migrations(&pool).await?;
        }

        Ok(NidxMetadata { pool })
    }

    pub async fn new_with_pool(pool: sqlx::PgPool) -> sqlx::Result<Self> {
        Self::run_migrations(&pool).await?;
        Ok(NidxMetadata { pool })
    }

    async fn run_migrations(pool: &sqlx::PgPool) -> sqlx::Result<()> {
        // Run migrations inside a transaction that holds a global lock, avoids races
        let mut tx = pool.begin().await?;
        sqlx::query!("SELECT pg_advisory_xact_lock($1)", MIGRATION_LOCK_ID)
            .execute(&mut *tx)
            .await?;
        sqlx::migrate!("./migrations").run(pool).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn transaction(&self) -> sqlx::Result<sqlx::Transaction<'_, sqlx::Postgres>> {
        self.pool.begin().await
    }

    /// Used by nidx_binding to insert in seq order (we don't have NATS to keep sequence)
    pub async fn max_seq(&self) -> sqlx::Result<i64> {
        let seqs = sqlx::query_scalar!(
            r#"SELECT COALESCE(MAX(seq), 1) AS "seq!" FROM segments
               UNION
               SELECT COALESCE(MAX(seq), 1) AS "seq!" FROM deletions"#
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(*seqs.iter().max().unwrap_or(&1))
    }
}

#[cfg(test)]
mod tests {
    use nidx_vector::config::{VectorConfig, VectorType};
    use shard::Shard;
    use uuid::Uuid;

    use super::*;

    const VECTOR_CONFIG: VectorConfig = VectorConfig::for_paragraphs(VectorType::DenseF32 { dimension: 3 });

    #[sqlx::test(migrations = false)]
    async fn create_and_find_index(pool: sqlx::PgPool) {
        let meta = NidxMetadata::new_with_pool(pool).await.unwrap();
        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await.unwrap();
        assert_eq!(shard.kbid, kbid);

        let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into())
            .await
            .unwrap();
        assert_eq!(index.shard_id, shard.id);
        assert_eq!(index.kind, IndexKind::Vector);
        assert_eq!(index.name, "multilingual");

        let found = Index::find(&meta.pool, shard.id, IndexKind::Vector, "multilingual")
            .await
            .unwrap();
        assert_eq!(found.id, index.id);
        assert_eq!(found.shard_id, shard.id);
        assert_eq!(found.kind, IndexKind::Vector);
        assert_eq!(found.name, "multilingual");
    }
}
