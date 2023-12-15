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
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use nucliadb_core::{node_error, protos, Channel, NodeResult};
use serde::*;

use crate::disk_structure;

#[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Similarity {
    #[default]
    Cosine,
    Dot,
}

impl From<protos::VectorSimilarity> for Similarity {
    fn from(value: protos::VectorSimilarity) -> Self {
        match value {
            protos::VectorSimilarity::Cosine => Similarity::Cosine,
            protos::VectorSimilarity::Dot => Similarity::Dot,
        }
    }
}
impl ToString for Similarity {
    fn to_string(&self) -> String {
        match self {
            Similarity::Cosine => "Cosine".to_string(),
            Similarity::Dot => "Dot".to_string(),
        }
    }
}
impl From<String> for Similarity {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Cosine" => Similarity::Cosine,
            "Dot" => Similarity::Dot,
            _ => Similarity::Cosine,
        }
    }
}
impl From<Similarity> for protos::VectorSimilarity {
    fn from(value: Similarity) -> Self {
        match value {
            Similarity::Cosine => protos::VectorSimilarity::Cosine,
            Similarity::Dot => protos::VectorSimilarity::Dot,
        }
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct ShardMetadataFile {
    pub kbid: Option<String>,
    pub similarity: Option<Similarity>,
    pub id: Option<String>,
    #[serde(default)]
    pub channel: Option<Channel>,
}

#[derive(Default, Debug)]
pub struct ShardMetadata {
    shard_path: PathBuf,
    id: String,
    kbid: Option<String>,
    similarity: Option<Similarity>,
    channel: Option<Channel>,
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
        let requested_shard_id = shard_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut reader = BufReader::new(File::open(metadata_path)?);
        let metadata: ShardMetadataFile = serde_json::from_reader(&mut reader)?;
        Ok(ShardMetadata {
            shard_path,
            kbid: metadata.kbid,
            similarity: metadata.similarity,
            id: metadata.id.unwrap_or(requested_shard_id),
            channel: metadata.channel,
            generation_id: RwLock::new(None),
        })
    }
    pub fn new(
        shard_path: PathBuf,
        id: String,
        kbid: Option<String>,
        similarity: Similarity,
        channel: Option<Channel>,
    ) -> ShardMetadata {
        ShardMetadata {
            shard_path,
            kbid,
            similarity: Some(similarity),
            id,
            channel,
            generation_id: RwLock::new(None),
        }
    }
    pub fn exists(shard_path: PathBuf) -> bool {
        let metadata_path = shard_path.join(disk_structure::METADATA_FILE);
        metadata_path.exists()
    }
    pub fn serialize_metadata(&self) -> NodeResult<()> {
        let metadata_path = self.shard_path.join(disk_structure::METADATA_FILE);
        let temp_metadata_path = metadata_path.with_extension("tmp");

        let mut writer = BufWriter::new(File::create(temp_metadata_path.clone())?);
        serde_json::to_writer(
            &mut writer,
            &ShardMetadataFile {
                kbid: self.kbid.clone(),
                similarity: self.similarity,
                id: Some(self.id.clone()),
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
    pub fn kbid(&self) -> Option<String> {
        self.kbid.clone()
    }
    pub fn similarity(&self) -> protos::VectorSimilarity {
        self.similarity.unwrap_or(Similarity::Cosine).into()
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
        if !ShardMetadata::exists(shard_path.clone()) {
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
        let meta = ShardMetadata::new(
            dir.path().to_path_buf(),
            "ID".to_string(),
            Some("KB".to_string()),
            Similarity::Cosine,
            Some(Channel::EXPERIMENTAL),
        );
        meta.serialize_metadata().unwrap();
        let meta_disk = ShardMetadata::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(meta.kbid, meta_disk.kbid);
        assert_eq!(meta.similarity, meta_disk.similarity);
    }
    #[test]
    fn open_empty() {
        let dir = TempDir::new().unwrap();
        assert!(!ShardMetadata::exists(dir.path().to_path_buf()));
        let meta = ShardMetadata::open(dir.path().to_path_buf());
        assert!(meta.is_err());
    }

    #[test]
    fn test_similarity_mappings() {
        assert_eq!(
            Similarity::Cosine,
            Similarity::from(protos::VectorSimilarity::Cosine)
        );
        assert_eq!(
            Similarity::Dot,
            Similarity::from(protos::VectorSimilarity::Dot)
        );
        assert_eq!(
            protos::VectorSimilarity::Cosine,
            protos::VectorSimilarity::from(Similarity::Cosine)
        );
        assert_eq!(
            protos::VectorSimilarity::Dot,
            protos::VectorSimilarity::from(Similarity::Dot)
        );
        assert_eq!("Cosine", Similarity::Cosine.to_string());
        assert_eq!("Dot", Similarity::Dot.to_string());
        assert_eq!(Similarity::Cosine, Similarity::from("Cosine".to_string()));
        assert_eq!(Similarity::Dot, Similarity::from("Dot".to_string()));

        assert_eq!(
            Some(protos::VectorSimilarity::Cosine).map(|i| i.into()),
            Some(Similarity::Cosine)
        );
    }

    #[test]
    fn test_cache_generation_id() {
        let dir = TempDir::new().unwrap();
        let meta = ShardMetadata::new(
            dir.path().to_path_buf(),
            "ID".to_string(),
            Some("KB".to_string()),
            Similarity::Cosine,
            Some(Channel::EXPERIMENTAL),
        );
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
