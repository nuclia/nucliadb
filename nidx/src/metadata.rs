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

/// A random ID to identify the lock we use during migration
const MIGRATION_LOCK_ID: i64 = 5324678839066546102;

#[derive(Clone, Debug)]
pub struct NidxMetadata {
    pub pool: sqlx::PgPool,
}

impl NidxMetadata {
    pub async fn new(database_url: &str) -> sqlx::Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(2))
            .connect(database_url)
            .await?;

        Self::new_with_pool(pool).await
    }

    pub async fn new_with_pool(pool: sqlx::PgPool) -> sqlx::Result<Self> {
        // Run migrations inside a transaction that holds a global lock, avoids races
        let mut tx = pool.begin().await?;
        sqlx::query!("SELECT pg_advisory_xact_lock($1)", MIGRATION_LOCK_ID)
            .execute(&mut *tx)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        tx.commit().await?;

        Ok(NidxMetadata { pool })
    }

    pub async fn transaction(&self) -> sqlx::Result<sqlx::Transaction<sqlx::Postgres>> {
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
    use nidx_vector::config::{Similarity, VectorCardinality, VectorConfig, VectorType};
    use shard::Shard;
    use uuid::Uuid;

    use super::*;

    const VECTOR_CONFIG: VectorConfig = VectorConfig {
        similarity: Similarity::Cosine,
        normalize_vectors: false,
        vector_type: VectorType::DenseF32 { dimension: 3 },
        flags: vec![],
        vector_cardinality: VectorCardinality::Single,
    };

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
