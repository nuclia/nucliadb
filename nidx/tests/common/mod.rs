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

#![allow(dead_code)] // clippy doesn't check for usage in other tests modules

pub mod cluster;
pub mod services;
pub mod utils;

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
