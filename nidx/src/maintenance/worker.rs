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

use anyhow::Ok;
use sqlx::postgres::PgListener;

use crate::{metadata::MergeJob, NidxMetadata, Settings};

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    loop {
        let job = MergeJob::take(&meta.pool).await?;
        if let Some(job) = job {
            println!("Running job {}", job.id);
            run_job(&meta, &job).await?;
        } else {
            println!("No jobs, waiting for more");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    Ok(())
}

pub async fn run_job(meta: &NidxMetadata, job: &MergeJob) -> anyhow::Result<()> {
    let segments = job.segments(meta).await?;

    // Start keep alive to mark progress
    let pool = meta.pool.clone();
    let job2 = (*job).clone();
    let keepalive = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            job2.keep_alive(&pool).await.unwrap();
        }
    });

    // Do merge
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Stop keep alives
    keepalive.abort();

    // Delete task if successful. Mark as failed otherwise?
    // The scheduler will requeue this if no activity in a while
    // job.finish(meta).await?;

    Ok(())
}
