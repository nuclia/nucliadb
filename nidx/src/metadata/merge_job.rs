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
use super::{IndexId, NidxMetadata, Segment, SegmentId};
use nidx_types::Seq;
use sqlx::{types::time::PrimitiveDateTime, Executor, Postgres};

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
            println!("Not all segments updated");
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
        sqlx::query!("UPDATE merge_jobs SET running_at = NOW() WHERE id = $1", self.id,).execute(meta).await?;
        Ok(())
    }

    pub async fn finish(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("DELETE FROM merge_jobs WHERE id = $1", self.id,).execute(meta).await?;
        Ok(())
    }

    pub async fn delete_many_by_index(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: IndexId,
    ) -> sqlx::Result<()> {
        sqlx::query!("DELETE FROM merge_jobs WHERE index_id = $1", index_id as IndexId).execute(meta).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nidx_vector::config::VectorConfig;
    use uuid::Uuid;

    use crate::{
        metadata::{Index, MergeJob, Shard},
        NidxMetadata,
    };

    #[sqlx::test]
    async fn test_merge_job_priority(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await?;
        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await?;
        let index = Index::create(&meta.pool, shard.id, "multilingual", VectorConfig::default().into()).await?;

        let job_low = MergeJob::create(&meta, index.id, &[], 1i64.into(), 1).await?;
        let job_high = MergeJob::create(&meta, index.id, &[], 1i64.into(), 5).await?;
        let job_low2 = MergeJob::create(&meta, index.id, &[], 1i64.into(), 1).await?;

        assert_eq!(MergeJob::take(&meta.pool).await?.unwrap().id, job_high.id);
        assert_eq!(MergeJob::take(&meta.pool).await?.unwrap().id, job_low.id);
        assert_eq!(MergeJob::take(&meta.pool).await?.unwrap().id, job_low2.id);
        assert!(MergeJob::take(&meta.pool).await?.is_none());

        Ok(())
    }
}
