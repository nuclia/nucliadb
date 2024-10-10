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
use super::{index::*, NidxMetadata};
use sqlx::{self, Executor, Postgres};
use uuid::Uuid;

pub struct Deletion {
    pub index_id: i64,
    pub seq: i64,
    pub key: String,
}

impl Deletion {
    pub async fn create(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: i64,
        seq: i64,
        key: &str,
    ) -> Result<Deletion, sqlx::Error> {
        sqlx::query_as!(
            Deletion,
            "INSERT INTO deletions (index_id, seq, key) VALUES ($1, $2, $3) RETURNING *",
            index_id,
            seq,
            key
        )
        .fetch_one(meta)
        .await
    }
}
