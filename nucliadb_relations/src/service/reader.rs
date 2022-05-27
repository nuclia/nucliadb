use std::fmt::Debug;

use async_trait::async_trait;
use nucliadb_protos::{RelationSearchRequest, RelationSearchResponse};
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::graph::*;

pub struct RelationReaderService {
    index: StorageSystem,
}
impl RService for RelationReaderService {}
impl Debug for RelationReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationReaderService").finish()
    }
}

#[async_trait]
impl ServiceChild for RelationReaderService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping relation reader Service");
        Ok(())
    }
    fn count(&self) -> usize {
        let txn = self.index.ro_txn();
        let count = self.index.no_nodes(&txn);
        txn.commit().unwrap();
        count as usize
    }
}
impl ReaderChild for RelationReaderService {
    type Request = RelationSearchRequest;
    type Response = RelationSearchResponse;
    fn search(&self, _: &Self::Request) -> InternalResult<Self::Response> {
        Ok(RelationSearchResponse {})
    }
    fn stored_ids(&self) -> Vec<String> {
        let txn = self.index.ro_txn();
        let keys: Vec<_> = self.index.get_keys(&txn).collect();
        txn.commit().unwrap();
        keys
    }
    fn reload(&self) {}
}

impl RelationReaderService {
    pub async fn start(config: &RelationsServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Ok(RelationReaderService::new(config).await.unwrap())
        } else {
            Ok(RelationReaderService::open(config).await.unwrap())
        }
    }
    pub async fn new(config: &RelationsServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            tokio::fs::create_dir_all(path).await.unwrap();

            Ok(RelationReaderService {
                index: StorageSystem::create(path),
            })
        }
    }

    pub async fn open(config: &RelationsServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(RelationReaderService {
                index: StorageSystem::open(path),
            })
        }
    }
}
