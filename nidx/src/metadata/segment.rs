use std::{collections::HashSet, path::PathBuf};

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
use super::IndexId;
use nidx_types::{SegmentMetadata, Seq};
use serde::{Deserialize, Serialize};
use sqlx::{
    types::{time::PrimitiveDateTime, JsonValue},
    Executor, Postgres,
};

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
        sqlx::query_as!(Segment, "SELECT * FROM segments WHERE id = ANY($1)", segment_ids as &[SegmentId])
            .fetch_all(meta)
            .await
    }

    pub async fn in_index(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: IndexId,
    ) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(Segment, "SELECT * FROM segments WHERE index_id = $1", index_id as IndexId)
            .fetch_all(meta)
            .await
    }

    pub async fn in_merge_job(
        meta: impl Executor<'_, Database = Postgres>,
        merge_job_id: i64,
    ) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(Segment, "SELECT * FROM segments WHERE merge_job_id = $1", merge_job_id).fetch_all(meta).await
    }

    pub fn metadata<T: for<'de> Deserialize<'de>>(&self, path: PathBuf) -> SegmentMetadata<T> {
        let metadata = serde_json::from_value(self.index_metadata.clone()).unwrap();
        SegmentMetadata {
            path,
            records: self.records as usize,
            tags: HashSet::new(),
            index_metadata: metadata,
        }
    }
}
