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
