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

use apalis::{postgres::PostgresStorage, prelude::*};

use crate::{NidxMetadata, Settings};

use super::merge::send_email;

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    let storage = PostgresStorage::new(meta.pool.clone());
    Monitor::<TokioExecutor>::new()
        .register(WorkerBuilder::new(format!("email-worker")).data(0usize).with_storage(storage).build_fn(send_email))
        .run()
        .await;

    // tokio::signal::ctrl_c().await.expect("failed to listen for event");

    Ok(())
}
