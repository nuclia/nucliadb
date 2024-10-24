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

mod grpc;
mod metadata;
mod shard_search;
mod sync;

use metadata::SearchMetadata;
use sync::run_sync;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use tempfile::tempdir;

use crate::{
    metadata::{IndexId, SegmentId},
    NidxMetadata, Settings,
};

pub async fn run() -> anyhow::Result<()> {
    let work_dir = tempdir()?;
    let settings = Settings::from_env();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;
    let storage = settings.storage.expect("Storage settings needed").object_store.client();

    let index_metadata = Arc::new(SearchMetadata::new(work_dir.path().to_path_buf()));

    let sync_task = tokio::task::spawn(run_sync(meta.clone(), storage.clone(), index_metadata.clone()));

    let api = grpc::SearchServer::new(meta.clone(), index_metadata.clone());
    let api_task = tokio::task::spawn(api.serve());

    tokio::select! {
        r = sync_task => {
            println!("sync_task() completed first {:?}", r)
        }
        r = api_task => {
            println!("api_task() completed first {:?}", r)
        }
    }

    Ok(())
}

fn segment_path(work_dir: &Path, index_id: &IndexId, segment_id: &SegmentId) -> PathBuf {
    work_dir.join(format!("{index_id:?}/{segment_id:?}"))
}
