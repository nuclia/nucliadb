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

#![allow(dead_code)] // rust doesn't detect all is used in our integration tests...

pub mod metadata {
    use nidx::metadata::{Index, MergeJob, Segment, Deletion, Shard, IndexKind};
    use sqlx::{Executor, Postgres};

    pub async fn get_all_shards(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Shard>> {
        sqlx::query_as!(Shard, r#"SELECT * FROM shards"#).fetch_all(meta).await
    }

    pub async fn get_all_indexes(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Index>> {
        sqlx::query_as!(
            Index,
            r#"SELECT id, shard_id, kind as "kind: IndexKind", name, configuration, updated_at, deleted_at
               FROM indexes"#,
        )
            .fetch_all(meta)
            .await
    }

    pub async fn get_all_segments(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Segment>> {
        sqlx::query_as!(Segment, r#"SELECT * FROM segments"#).fetch_all(meta).await
    }

    pub async fn get_all_merge_jobs(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<MergeJob>> {
        sqlx::query_as!(MergeJob, r#"SELECT * FROM merge_jobs"#).fetch_all(meta).await
    }

    pub async fn get_all_deletions(meta: impl Executor<'_, Database = Postgres>) -> sqlx::Result<Vec<Deletion>> {
        sqlx::query_as!(Deletion, r#"SELECT * FROM deletions"#).fetch_all(meta).await
    }
}
