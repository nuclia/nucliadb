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
use super::{IndexId, NidxMetadata, Segment, SegmentId};
use nidx_types::Seq;
use sqlx::{Executor, Postgres, types::time::PrimitiveDateTime};
use tracing::*;

#[derive(Clone)]
pub struct MergeJob {
    pub id: i64,
    pub index_id: IndexId,
    pub retries: i16,
    pub seq: Seq,
    pub enqueued_at: PrimitiveDateTime,
    pub started_at: Option<PrimitiveDateTime>,
    pub running_at: Option<PrimitiveDateTime>,
    pub priority: i32,
}

impl MergeJob {
    pub async fn create(
        meta: &NidxMetadata,
        index_id: IndexId,
        segment_ids: &[SegmentId],
        seq: Seq,
        priority: i32,
    ) -> sqlx::Result<MergeJob> {
        let mut tx = meta.transaction().await?;
        let job = sqlx::query_as!(
            MergeJob,
            "INSERT INTO merge_jobs (seq, index_id, priority) VALUES ($1, $2, $3) RETURNING *",
            i64::from(seq),
            index_id as IndexId,
            priority
        )
        .fetch_one(&mut *tx)
        .await?;
        let affected = sqlx::query!(
            "UPDATE segments SET merge_job_id = $1 WHERE id = ANY($2) AND merge_job_id IS NULL",
            job.id,
            segment_ids as &[SegmentId]
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if affected != segment_ids.len() as u64 {
            warn!("Not all segments added to the merge job");
            tx.rollback().await?;
            Err(sqlx::Error::RowNotFound)
        } else {
            tx.commit().await?;
            Ok(job)
        }
    }

    pub async fn segments(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Segment>> {
        Segment::in_merge_job(meta, self.id).await
    }

    pub async fn take(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Option<MergeJob>> {
        sqlx::query_as!(
            MergeJob,
            "WITH job AS (
                 SELECT id FROM merge_jobs
                 WHERE started_at IS NULL ORDER BY priority DESC, id LIMIT 1
                 FOR UPDATE SKIP LOCKED
             )
             UPDATE merge_jobs
             SET started_at = NOW(), running_at = NOW()
             FROM job
             WHERE merge_jobs.id = job.id
             RETURNING merge_jobs.*"
        )
        .fetch_optional(meta)
        .await
    }

    pub async fn keep_alive(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("UPDATE merge_jobs SET running_at = NOW() WHERE id = $1", self.id,)
            .execute(meta)
            .await?;
        Ok(())
    }

    pub async fn finish(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("DELETE FROM merge_jobs WHERE id = $1", self.id,)
            .execute(meta)
            .await?;
        Ok(())
    }

    pub async fn delete_many_by_index(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: IndexId,
    ) -> sqlx::Result<()> {
        sqlx::query!("DELETE FROM merge_jobs WHERE index_id = $1", index_id as IndexId)
            .execute(meta)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nidx_vector::config::{VectorConfig, VectorType};
    use tokio::task::JoinSet;
    use uuid::Uuid;

    use crate::{
        NidxMetadata,
        metadata::{Index, MergeJob, Shard},
    };

    const VECTOR_CONFIG: VectorConfig = VectorConfig::for_paragraphs(VectorType::DenseF32 { dimension: 3 });

    #[sqlx::test]
    async fn test_merge_job_priority(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await?;
        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await?;
        let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?;

        let job_low = MergeJob::create(&meta, index.id, &[], 1i64.into(), 1).await?;
        let job_high = MergeJob::create(&meta, index.id, &[], 1i64.into(), 5).await?;
        let job_low2 = MergeJob::create(&meta, index.id, &[], 1i64.into(), 1).await?;

        assert_eq!(MergeJob::take(&meta.pool).await?.unwrap().id, job_high.id);
        assert_eq!(MergeJob::take(&meta.pool).await?.unwrap().id, job_low.id);
        assert_eq!(MergeJob::take(&meta.pool).await?.unwrap().id, job_low2.id);
        assert!(MergeJob::take(&meta.pool).await?.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn test_merge_job_take(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await?;
        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await?;
        let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?;

        // If this test fails, it doesn't do so consistently, so retry a few times
        for attempt in 0..10 {
            MergeJob::create(&meta, index.id, &[], 1i64.into(), 1).await?;

            let mut tasks = JoinSet::new();
            for _ in 0..8 {
                let meta2 = meta.clone();
                tasks.spawn(async move {
                    let job = MergeJob::take(&meta2.pool).await.unwrap();
                    job.is_some()
                });
            }
            let results = tasks.join_all().await;
            let taken_jobs = results.iter().filter(|x| **x).count();
            assert_eq!(taken_jobs, 1, "Multiple jobs taken at attempt {attempt}");
        }

        Ok(())
    }
}
