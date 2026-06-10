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

use nidx_types::Seq;
use sqlx::{self, Executor, Postgres};

pub struct IndexRequest {
    seq: i64,
}

impl IndexRequest {
    pub fn seq(&self) -> Seq {
        self.seq.into()
    }

    pub async fn create(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<IndexRequest> {
        let seq = sqlx::query_scalar!("INSERT INTO index_requests DEFAULT VALUES RETURNING seq",)
            .fetch_one(meta)
            .await?;
        Ok(IndexRequest { seq })
    }

    pub async fn delete(&self, meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("DELETE FROM index_requests WHERE seq = $1", self.seq)
            .execute(meta)
            .await?;
        Ok(())
    }

    pub async fn delete_old(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<()> {
        sqlx::query!("DELETE FROM index_requests WHERE received_at < NOW() - INTERVAL '1 minute'")
            .execute(meta)
            .await?;
        Ok(())
    }

    pub async fn last_ack_seq(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<i64> {
        sqlx::query_scalar!(
            "SELECT COALESCE(MIN(seq) - 1, (SELECT last_value FROM index_requests_seq_seq)) AS \"seq!\"
             FROM index_requests WHERE received_at > NOW() - INTERVAL '1 minute'"
        )
        .fetch_one(meta)
        .await
    }
}
