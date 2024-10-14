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

use std::time::Duration;

use tokio::time::sleep;

use crate::{metadata::MergeJob, NidxMetadata, Settings};

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    MergeJob::create(&meta, &[1, 2]).await?;

    loop {
        let retry_jobs = sqlx::query_as!(MergeJob, "UPDATE merge_jobs SET started_at = NULL, running_at = NULL, retries = retries + 1 WHERE running_at < NOW() - INTERVAL '5 seconds' AND retries < 2 RETURNING *").fetch_all(&meta.pool).await?;
        for j in retry_jobs {
            println!("Retrying {} for {} time", j.id, j.retries);
        }

        let failed_jobs = sqlx::query_as!(
            MergeJob,
            "DELETE FROM merge_jobs WHERE running_at < NOW() - INTERVAL '5 seconds' AND retries >= 2 RETURNING *"
        )
        .fetch_all(&meta.pool)
        .await?;
        for j in failed_jobs {
            println!("Failed job {}", j.id);
        }

        let long_jobs =
            sqlx::query_as!(MergeJob, "SELECT * FROM merge_jobs WHERE running_at - started_at > INTERVAL '1 minute'")
                .fetch_all(&meta.pool)
                .await?;
        for j in long_jobs {
            println!("Job has been running for a while {}", j.id);
        }

        // TODO: Delete non-ready segments

        sleep(Duration::from_secs(5)).await;
    }
}
