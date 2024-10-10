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
use super::NidxMetadata;
use sqlx::{types::time::PrimitiveDateTime, Executor, Postgres};

pub struct Segment {
    pub id: i64,
    pub index_id: i64,
    pub ready: bool,
    pub seq: i64,
    pub records: Option<i64>,
    pub size_bytes: Option<i64>,
    pub created_at: PrimitiveDateTime,
    pub deleted_at: Option<PrimitiveDateTime>,
}

impl Segment {
    pub async fn create(meta: &NidxMetadata, index_id: i64, seq: i64) -> sqlx::Result<Segment> {
        sqlx::query_as!(Segment, r#"INSERT INTO segments (index_id, seq) VALUES ($1, $2) RETURNING *"#, index_id, seq)
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
            "UPDATE segments SET ready = true, records = $1, size_bytes = $2 WHERE id = $3",
            records,
            size_bytes,
            self.id,
        )
        .execute(meta)
        .await?;
        Ok(())
    }
}
