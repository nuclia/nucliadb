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

use async_trait::async_trait;
use nucliadb_protos::{Resource, ResourceId};
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::graph::*;

pub struct RelationWriterService {
    index: StorageSystem,
}
impl WService for RelationWriterService {}

impl std::fmt::Debug for RelationWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationWriterService").finish()
    }
}

#[async_trait]
impl ServiceChild for RelationWriterService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping relation writer Service");
        Ok(())
    }
    fn count(&self) -> usize {
        let txn = self.index.ro_txn();
        let count = self.index.no_nodes(&txn);
        txn.commit().unwrap();
        count as usize
    }
}

impl WriterChild for RelationWriterService {
    fn delete_resource(&mut self, _resource_id: &ResourceId) -> InternalResult<()> {
        todo!();
    }
    fn set_resource(&mut self, _resource: &Resource) -> InternalResult<()> {
        todo!();
    }
    fn garbage_collection(&mut self) {}
}

impl RelationWriterService {
    pub async fn start(config: &RelationsServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Ok(RelationWriterService::new(config).await.unwrap())
        } else {
            Ok(RelationWriterService::open(config).await.unwrap())
        }
    }
    pub async fn new(config: &RelationsServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            tokio::fs::create_dir_all(path).await.unwrap();

            Ok(RelationWriterService {
                index: StorageSystem::create(path),
            })
        }
    }

    pub async fn open(config: &RelationsServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(RelationWriterService {
                index: StorageSystem::open(path),
            })
        }
    }
}
