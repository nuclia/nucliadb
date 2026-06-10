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

use super::IndexId;
use nidx_types::{SegmentMetadata, Seq};
use serde::{Deserialize, Serialize};
use sqlx::{
    Executor, Postgres,
    types::{JsonValue, time::PrimitiveDateTime},
};
use std::path::PathBuf;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct SegmentId(i64);
impl From<i64> for SegmentId {
    fn from(value: i64) -> Self {
        Self(value)
    }
}
impl SegmentId {
    pub fn storage_key(&self) -> object_store::path::Path {
        format!("segment/{}", self.0).into()
    }

    pub fn local_path(&self, index_id: &IndexId) -> PathBuf {
        format!("{}/{}", index_id.0, self.0).into()
    }
}

pub struct Segment {
    pub id: SegmentId,
    pub index_id: IndexId,
    pub seq: Seq,
    pub records: i64,
    pub size_bytes: Option<i64>,
    pub merge_job_id: Option<i64>,
    index_metadata: JsonValue,
    pub delete_at: Option<PrimitiveDateTime>,
}

pub struct NewSegment {
    pub(crate) records: i64,
    pub(crate) index_metadata: JsonValue,
}

impl<M: Serialize> From<SegmentMetadata<M>> for NewSegment {
    fn from(value: SegmentMetadata<M>) -> Self {
        Self {
            records: value.records as i64,
            index_metadata: serde_json::to_value(&value.index_metadata).unwrap(),
        }
    }
}

impl Segment {
    pub async fn create(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: IndexId,
        seq: Seq,
        records: i64,
        index_metadata: JsonValue,
    ) -> sqlx::Result<Segment> {
        sqlx::query_as!(
            Segment,
            r#"INSERT INTO segments (index_id, seq, records, index_metadata) VALUES ($1, $2, $3, $4) RETURNING *"#,
            index_id as IndexId,
            i64::from(&seq),
            records,
            index_metadata
        )
        .fetch_one(meta)
        .await
    }

    pub async fn mark_ready(&self, meta: impl Executor<'_, Database = Postgres>, size_bytes: i64) -> sqlx::Result<()> {
        sqlx::query!(
            "UPDATE segments SET delete_at = NULL, size_bytes = $1 WHERE id = $2",
            size_bytes,
            self.id as SegmentId,
        )
        .execute(meta)
        .await?;
        Ok(())
    }

    pub async fn mark_delete_by_index(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: IndexId,
    ) -> sqlx::Result<()> {
        sqlx::query!(
            "UPDATE segments
             SET delete_at = NOW() + INTERVAL '5 minutes'
             WHERE index_id = $1",
            index_id as IndexId,
        )
        .execute(meta)
        .await?;
        Ok(())
    }

    /// Lists segments marked as deleted, i.e., segments that can be deleted
    pub async fn marked_as_deleted(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<SegmentId>> {
        sqlx::query_scalar!(
            r#"SELECT segments.id AS "id: SegmentId"
               FROM segments
               LEFT JOIN indexes ON segments.index_id = indexes.id
               WHERE segments.delete_at < NOW() OR indexes.deleted_at IS NOT NULL"#
        )
        .fetch_all(meta)
        .await
    }

    pub async fn mark_many_for_deletion(
        meta: impl Executor<'_, Database = Postgres>,
        segment_ids: &[SegmentId],
    ) -> sqlx::Result<()> {
        let affected = sqlx::query!(
            "UPDATE segments SET delete_at = NOW() + INTERVAL '5 minutes' WHERE id = ANY($1)",
            segment_ids as &[SegmentId]
        )
        .execute(meta)
        .await?
        .rows_affected();
        if affected != segment_ids.len() as u64 {
            Err(sqlx::Error::RowNotFound)
        } else {
            Ok(())
        }
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

    pub async fn select_many(
        meta: impl Executor<'_, Database = Postgres>,
        segment_ids: &[SegmentId],
    ) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(
            Segment,
            "SELECT * FROM segments WHERE id = ANY($1)",
            segment_ids as &[SegmentId]
        )
        .fetch_all(meta)
        .await
    }

    pub async fn in_index(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: IndexId,
    ) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(
            Segment,
            "SELECT * FROM segments WHERE index_id = $1 AND delete_at IS NULL",
            index_id as IndexId
        )
        .fetch_all(meta)
        .await
    }

    pub async fn in_indexes(
        meta: impl Executor<'_, Database = Postgres>,
        indexes: &[IndexId],
    ) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(
            Segment,
            "SELECT * FROM segments WHERE index_id = ANY($1) AND delete_at IS NULL",
            indexes as &[IndexId]
        )
        .fetch_all(meta)
        .await
    }

    pub async fn in_merge_job(
        meta: impl Executor<'_, Database = Postgres>,
        merge_job_id: i64,
    ) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(
            Segment,
            "SELECT * FROM segments WHERE merge_job_id = $1 ORDER BY seq ASC",
            merge_job_id
        )
        .fetch_all(meta)
        .await
    }

    pub fn metadata<T: for<'de> Deserialize<'de>>(&self, path: PathBuf) -> SegmentMetadata<T> {
        let metadata = serde_json::from_value(self.index_metadata.clone()).unwrap();
        SegmentMetadata {
            path,
            records: self.records as usize,
            index_metadata: metadata,
        }
    }
}
