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
use super::{IndexId, NidxMetadata};
use nidx_types::Seq;
use sqlx::{types::time::PrimitiveDateTime, Executor, Postgres};

#[derive(Copy, Clone, Debug, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct SegmentId(i64);
impl From<i64> for SegmentId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}
impl SegmentId {
    pub fn storage_key(&self) -> String {
        format!("segment/{}", self.0)
    }

    #[deprecated]
    pub fn local_path(&self) -> String {
        format!("{}", self.0)
    }
}

pub struct Segment {
    pub id: SegmentId,
    pub index_id: IndexId,
    pub seq: Seq,
    pub records: Option<i64>,
    pub size_bytes: Option<i64>,
    pub merge_job_id: Option<i64>,
    pub delete_at: Option<PrimitiveDateTime>,
}

impl Segment {
    pub async fn create(meta: &NidxMetadata, index_id: IndexId, seq: Seq) -> sqlx::Result<Segment> {
        sqlx::query_as!(
            Segment,
            r#"INSERT INTO segments (index_id, seq) VALUES ($1, $2) RETURNING *"#,
            index_id as IndexId,
            i64::from(&seq)
        )
        .fetch_one(&meta.pool)
        .await
    }

    pub async fn mark_ready(
        &self,
        meta: impl Executor<'_, Database = Postgres>,
        records: i64,
        size_bytes: i64,
    ) -> sqlx::Result<()> {
        sqlx::query!(
            "UPDATE segments SET delete_at = NULL, records = $1, size_bytes = $2 WHERE id = $3",
            records,
            size_bytes,
            self.id as SegmentId,
        )
        .execute(meta)
        .await?;
        Ok(())
    }

    pub async fn delete_many(
        meta: impl Executor<'_, Database = Postgres>,
        segment_ids: &[SegmentId],
    ) -> sqlx::Result<()> {
        let affected = sqlx::query!("DELETE FROM segments WHERE id = ANY($1)", segment_ids as &[SegmentId])
            .execute(meta)
            .await?
            .rows_affected();
        if affected != segment_ids.len() as u64 {
            Err(sqlx::Error::RowNotFound)
        } else {
            Ok(())
        }
    }
}
