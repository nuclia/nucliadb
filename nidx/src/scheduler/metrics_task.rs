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
    metrics::{
        self,
        scheduler::{JobFamily, JobState},
    },
    NidxMetadata,
};

pub async fn update_metrics(metadb: &NidxMetadata) -> anyhow::Result<()> {
    update_merge_job_metric(metadb).await?;

    Ok(())
}

pub async fn update_merge_job_metric(metadb: &NidxMetadata) -> anyhow::Result<()> {
    let job_states = sqlx::query!(
        r#"
        SELECT running_at IS NOT NULL AS "running!",
        COUNT(*) AS "count!"
        FROM merge_jobs GROUP BY 1"#
    )
    .fetch_all(&metadb.pool)
    .await?;
    for record in job_states {
        let state = if record.running {
            JobState::Running
        } else {
            JobState::Queued
        };
        metrics::scheduler::QUEUED_JOBS
            .get_or_create(&JobFamily {
                state,
            })
            .set(record.count);
    }

    Ok(())
}
