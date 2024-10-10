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
use super::{NidxMetadata, Segment};
use sqlx::{types::time::PrimitiveDateTime, Executor, Postgres};

#[derive(Clone)]
pub struct MergeJob {
    pub id: i64,
    pub retries: i16,
    pub enqueued_at: PrimitiveDateTime,
    pub started_at: Option<PrimitiveDateTime>,
    pub running_at: Option<PrimitiveDateTime>,
}

impl MergeJob {
    pub async fn create(meta: &NidxMetadata, segment_ids: &[i64]) -> sqlx::Result<MergeJob> {
        let mut tx = meta.transaction().await?;
        let job =
            sqlx::query_as!(MergeJob, "INSERT INTO merge_jobs DEFAULT VALUES RETURNING *").fetch_one(&mut *tx).await?;
        let affected = sqlx::query!(
            "UPDATE segments SET merge_job_id = $1 WHERE id = ANY($2) AND merge_job_id IS NULL",
            job.id,
            segment_ids
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

    pub async fn segments(&self, meta: &NidxMetadata) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(Segment, "SELECT * FROM segments WHERE merge_job_id = $1", self.id).fetch_all(&meta.pool).await
    }

    pub async fn take(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Option<MergeJob>> {
        sqlx::query_as!(
            MergeJob,
            "WITH job AS (SELECT id FROM merge_jobs WHERE started_at IS NULL ORDER BY id LIMIT 1)
            UPDATE merge_jobs SET started_at = NOW(), running_at = NOW() FROM job WHERE merge_jobs.id = job.id RETURNING merge_jobs.*"
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
}
