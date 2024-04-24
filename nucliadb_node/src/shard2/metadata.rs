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
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use nucliadb_core::{node_error, protos, Channel, NodeResult};
use serde::*;

use crate::disk_structure;

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct ShardContext {
    pub kbid: String,
    pub id: String,
    pub channel: Channel,
}

#[derive(Default, Debug)]
pub struct ShardMetadata {
    shard_path: PathBuf,
    id: String,
    kbid: String,
    channel: Channel,
    // A generation id is a way to track if a shard has changed.
    // A new id means that something in the shard has changed.
    // This is used by replication to track which shards have changed
    // and to efficiently replicate them.
    generation_id: RwLock<Option<String>>,
}

impl ShardMetadata {
    pub fn open(shard_path: PathBuf) -> NodeResult<ShardMetadata> {
        let metadata_path = shard_path.join(disk_structure::METADATA_FILE);
        if !metadata_path.exists() {
            return Err(node_error!("Shard metadata file does not exist"));
        }
        let requested_shard_id = shard_path.file_name().unwrap().to_str().unwrap().to_string();

        let mut reader = BufReader::new(File::open(metadata_path)?);
        let metadata: ShardContext = serde_json::from_reader(&mut reader)?;
        Ok(ShardMetadata {
            shard_path,
            kbid: metadata.kbid,
            id: metadata.id.unwrap_or(requested_shard_id),
            channel: metadata.channel,
            generation_id: RwLock::new(None),
        })
    }

    pub fn new(shard_path: PathBuf, context: ShardContext) -> ShardMetadata {
        ShardMetadata {
            shard_path,
            kbid: context.kbid,
            id: context.id,
            channel: context.channel,
            generation_id: RwLock::new(None),
        }
    }

    pub fn exists(shard_path: &Path) -> bool {
        let metadata_path = shard_path.join(disk_structure::METADATA_FILE);
        metadata_path.exists()
    }

    pub fn serialize_metadata(&self) -> NodeResult<()> {
        let metadata_path = self.shard_path.join(disk_structure::METADATA_FILE);
        let temp_metadata_path = metadata_path.with_extension("tmp");

        let mut writer = BufWriter::new(File::create(temp_metadata_path.clone())?);
        serde_json::to_writer(
            &mut writer,
            &ShardContext {
                kbid: self.kbid.clone(),
                id: self.id.clone(),
                channel: self.channel,
            },
        )?;
        writer.flush()?;

        std::fs::rename(temp_metadata_path, metadata_path)?;

        self.new_generation_id();

        Ok(())
    }

    pub fn shard_path(&self) -> PathBuf {
        self.shard_path.clone()
    }

    pub fn kbid(&self) -> String {
        self.kbid.clone()
    }

    pub fn channel(&self) -> Channel {
        self.channel.unwrap_or_default()
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn get_generation_id(&self) -> Option<String> {
        if let Ok(gen_id_read) = self.generation_id.read() {
            match &*gen_id_read {
                Some(value) => {
                    return Some(value.clone());
                }
                None => {}
            }
        }

        let filepath = self.shard_path.join(disk_structure::GENERATION_FILE);
        // check if file does not exist
        if filepath.exists() {
            let gen_id = std::fs::read_to_string(filepath).unwrap();
            if let Ok(mut gen_id_write) = self.generation_id.write() {
                *gen_id_write = Some(gen_id.clone());
            }
            return Some(gen_id);
        }
        None
    }

    pub fn new_generation_id(&self) -> String {
        let generation_id = uuid::Uuid::new_v4().to_string();
        self.set_generation_id(generation_id.clone());
        generation_id
    }

    pub fn set_generation_id(&self, generation_id: String) {
        let filepath = self.shard_path.join(disk_structure::GENERATION_FILE);
        std::fs::write(filepath, generation_id.clone()).unwrap();
        if let Ok(mut gen_id_write) = self.generation_id.write() {
            *gen_id_write = Some(generation_id.clone());
        }
    }
}

#[derive(Default, Debug)]
pub struct ShardsMetadataManager {
    shards_path: PathBuf,
    metadatas: RwLock<HashMap<String, Arc<ShardMetadata>>>,
}

impl ShardsMetadataManager {
    pub fn new(shards_path: PathBuf) -> Self {
        Self {
            metadatas: RwLock::new(HashMap::new()),
            shards_path,
        }
    }
    pub fn add_metadata(&self, metadata: Arc<ShardMetadata>) {
        if let Ok(mut shards) = self.metadatas.write() {
            shards.insert(metadata.id().clone(), metadata);
        }
    }

    pub fn get(&self, shard_id: String) -> Option<Arc<ShardMetadata>> {
        if let Ok(shards) = self.metadatas.read() {
            if shards.contains_key(&shard_id) {
                return Some(Arc::clone(shards.get(&shard_id).unwrap()));
            }
        }
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path, &shard_id);
        if !ShardMetadata::exists(&shard_path) {
            return None;
        }
        let sm = ShardMetadata::open(shard_path);
        if let Ok(sm) = sm {
            if let Ok(mut shards) = self.metadatas.write() {
                let sm = Arc::new(sm);
                shards.insert(shard_id.clone(), Arc::clone(&sm));
                return Some(sm);
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;
    #[test]
    fn create() {
        let dir = TempDir::new().unwrap();
        let context = ShardContext {
            kbid: "KB".to_string(),
            id: "ID".to_string(),
            channel: Channel::EXPERIMENTAL,
        };
        let meta = ShardMetadata::new(dir.path().to_path_buf(), context);
        meta.serialize_metadata().unwrap();
        let meta_disk = ShardMetadata::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(meta.kbid, meta_disk.kbid);
        assert_eq!(meta.similarity, meta_disk.similarity);
        assert_eq!(meta.id, meta_disk.id);
        assert_eq!(meta.channel, meta_disk.channel);
        assert_eq!(meta.normalize_vectors, meta_disk.normalize_vectors);
    }

    #[test]
    fn open_empty() {
        let dir = TempDir::new().unwrap();
        assert!(!ShardMetadata::exists(dir.path()));
        let meta = ShardMetadata::open(dir.path().to_path_buf());
        assert!(meta.is_err());
    }

    #[test]
    fn test_cache_generation_id() {
        let dir = TempDir::new().unwrap();
        let context = ShardContext {
            kbid: "KB".to_string(),
            id: "ID".to_string(),
            channel: Channel::EXPERIMENTAL,
        };
        let meta = ShardMetadata::new(dir.path().to_path_buf(), context);
        let gen_id = meta.get_generation_id();
        assert_eq!(gen_id, meta.get_generation_id());
        // assert!(meta.generation_id.read().unwrap().is_none());
        // let gen_id = meta.get_generation_id();
        // assert_eq!(
        //     *meta.generation_id.read().unwrap().as_ref().unwrap(),
        //     gen_id
        // );

        // let new_id = meta.new_generation_id();
        // assert_ne!(gen_id, new_id);
        // assert_eq!(
        //     *meta.generation_id.read().unwrap().as_ref().unwrap(),
        //     new_id
        // );
        // assert_eq!(meta.get_generation_id(), new_id);

        // let set_id = "set_id".to_string();
        // meta.set_generation_id(set_id.clone());
        // assert_eq!(meta.get_generation_id(), set_id);
    }
}
