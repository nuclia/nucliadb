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
