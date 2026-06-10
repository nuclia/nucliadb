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
