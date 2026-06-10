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
            "SELECT * FROM deletions WHERE index_id = $1 AND seq <= $2 ORDER BY seq ASC",
            index_id as IndexId,
            i64::from(&seq)
        )
        .fetch_all(meta)
        .await
    }
}
