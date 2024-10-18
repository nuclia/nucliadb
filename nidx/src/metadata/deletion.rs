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
use sqlx::{self, Executor, Postgres};

use nidx_types::Seq;

use super::IndexId;

pub struct Deletion {
    pub index_id: IndexId,
    pub seq: Seq,
    pub keys: Vec<String>,
}

impl Deletion {
    pub async fn create(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: IndexId,
        seq: Seq,
        keys: &[String],
    ) -> Result<Deletion, sqlx::Error> {
        sqlx::query_as!(
            Deletion,
            "INSERT INTO deletions (index_id, seq, keys) VALUES ($1, $2, $3) RETURNING *",
            index_id as IndexId,
            i64::from(&seq),
            keys
        )
        .fetch_one(meta)
        .await
    }

    pub async fn for_index_and_seq(
        meta: impl Executor<'_, Database = Postgres>,
        index_id: IndexId,
        seq: Seq,
    ) -> Result<Vec<Deletion>, sqlx::Error> {
        sqlx::query_as!(
            Deletion,
            "SELECT * FROM deletions WHERE index_id = $1 AND seq <= $2",
            index_id as IndexId,
            i64::from(&seq)
        )
        .fetch_all(meta)
        .await
    }
}
