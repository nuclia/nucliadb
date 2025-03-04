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

#![allow(dead_code)] // clippy doesn't check for usage in other tests modules

pub mod services;

pub mod metadata {
    use sqlx::{Executor, Postgres};

    pub async fn count_shards(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<usize> {
        let count = sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!: i64" FROM shards"#)
            .fetch_one(meta)
            .await?;
        Ok(count as usize)
    }

    pub async fn count_indexes(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<usize> {
        let count = sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!: i64" FROM indexes"#)
            .fetch_one(meta)
            .await?;
        Ok(count as usize)
    }

    pub async fn count_segments(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<usize> {
        let count = sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!: i64" FROM segments"#)
            .fetch_one(meta)
            .await?;
        Ok(count as usize)
    }

    pub async fn count_merge_jobs(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<usize> {
        let count = sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!: i64" FROM merge_jobs"#)
            .fetch_one(meta)
            .await?;
        Ok(count as usize)
    }

    pub async fn count_deletions(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<usize> {
        let count = sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!: i64" FROM deletions"#)
            .fetch_one(meta)
            .await?;
        Ok(count as usize)
    }
}
