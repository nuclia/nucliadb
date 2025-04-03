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

use crate::{
    NidxMetadata,
    metrics::{
        self,
        scheduler::{JobFamily, JobState},
    },
};

pub async fn update_merge_job_metric(metadb: &NidxMetadata) -> anyhow::Result<()> {
    let job_states = sqlx::query!(
        r#"
        SELECT
        COUNT(*) FILTER (WHERE started_at IS NULL AND enqueued_at < NOW() - INTERVAL '1 minute') AS "queued!",
        COUNT(*) FILTER (WHERE started_at IS NULL AND enqueued_at > NOW() - INTERVAL '1 minute') AS "recently_queued!",
        COUNT(*) FILTER (WHERE started_at IS NOT NULL) AS "running!"
        FROM merge_jobs;
        "#
    )
    .fetch_one(&metadb.pool)
    .await?;
    metrics::scheduler::QUEUED_JOBS
        .get_or_create(&JobFamily {
            state: JobState::Queued,
        })
        .set(job_states.queued);
    metrics::scheduler::QUEUED_JOBS
        .get_or_create(&JobFamily {
            state: JobState::RecentlyQueued,
        })
        .set(job_states.queued);
    metrics::scheduler::QUEUED_JOBS
        .get_or_create(&JobFamily {
            state: JobState::Running,
        })
        .set(job_states.running);

    Ok(())
}
