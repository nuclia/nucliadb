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
use std::path::Path;

use nucliadb_core::protos::GetShardRequest;
use nucliadb_core::tracing::*;
use nucliadb_core::{node_error, NodeResult};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};

use crate::reader::NodeReaderService;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct ShardMetadata {
    kbid: String,
    load_score: f32,
}

impl From<ShardMetadata> for nucliadb_core::protos::node_metadata::ShardMetadata {
    fn from(shard_metadata: ShardMetadata) -> Self {
        nucliadb_core::protos::node_metadata::ShardMetadata {
            kbid: shard_metadata.kbid,
            load_score: shard_metadata.load_score,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct NodeMetadata {
    load_score: f32,
    shards: HashMap<String, ShardMetadata>,
}

impl Serialize for NodeMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut s = serializer.serialize_struct("NodeMetadata", 3)?;
        s.serialize_field("load_score", &self.load_score())?;
        s.serialize_field("shard_count", &self.shard_count())?;
        s.serialize_field("shards", &self.shards)?;

        s.end()
    }
}

impl From<NodeMetadata> for nucliadb_core::protos::NodeMetadata {
    fn from(node_metadata: NodeMetadata) -> Self {
        nucliadb_core::protos::NodeMetadata {
            load_score: node_metadata.load_score(),
            shard_count: node_metadata.shard_count(),
            shards: node_metadata
                .shards
                .into_iter()
                .map(|(id, shard)| (id, shard.into()))
                .collect(),
        }
    }
}

impl NodeMetadata {
    pub fn load_score(&self) -> f32 {
        self.load_score
    }

    pub fn shard_count(&self) -> u64 {
        self.shards.len() as u64
    }

    pub fn new_shard(&mut self, shard_id: String, kbid: String, load_score: f32) {
        let load_score = if let Some(shard) = self
            .shards
            .insert(shard_id, ShardMetadata { kbid, load_score })
        {
            load_score - shard.load_score
        } else {
            load_score
        };

        self.load_score += load_score;
    }

    pub fn delete_shard(&mut self, shard_id: String) {
        if let Some(shard) = self.shards.remove(&shard_id) {
            self.load_score -= shard.load_score;
        }
    }

    pub fn update_shard(&mut self, shard_id: String, paragraph_count: u64) {
        if let Some(mut shard) = self.shards.get_mut(&shard_id) {
            let load_score = paragraph_count as f32;

            self.load_score += load_score - shard.load_score;
            shard.load_score = load_score;
        }
    }

    pub async fn load_or_create(path: &Path) -> NodeResult<Self> {
        if !path.exists() {
            info!("Node metadata file does not exist.");

            let node_metadata = Self::create(path).unwrap_or_else(|e| {
                warn!("Cannot create metadata file '{}': {e}", path.display());
                info!("Create default metadata file '{}'", path.display());

                Self::default()
            });

            node_metadata.save(path)?;

            Ok(node_metadata)
        } else {
            Self::load(path)
        }
    }

    pub fn save(&self, path: &Path) -> NodeResult<()> {
        info!("Saving node metadata file '{}'", path.display());

        let file =
            File::create(path).map_err(|e| node_error!("Cannot open node metadata file: {e}"))?;

        let mut writer = BufWriter::new(file);

        serde_json::to_writer(&mut writer, &self)
            .map_err(|e| node_error!("Cannot serialize node metadata: {e}"))?;

        Ok(writer.flush()?)
    }

    pub fn load(path: &Path) -> NodeResult<Self> {
        info!("Loading node metadata file '{}'", path.display());

        let file =
            File::open(path).map_err(|e| node_error!("Cannot open node metadata file: {e}"))?;

        let reader = BufReader::new(file);

        serde_json::from_reader(reader)
            .map_err(|e| node_error!("Cannot deserialize node metadata: {e}"))
    }

    pub fn create(path: &Path) -> NodeResult<Self> {
        info!("Creating node metadata file '{}'", path.display());

        let mut reader = NodeReaderService::new();

        reader
            .iter_shards()
            .map_err(|e| node_error!("Cannot read shards folder: {e}"))?
            .try_fold(NodeMetadata::default(), |mut node_metadata, shard| {
                let shard = shard.map_err(|e| node_error!("Cannot load shard: {e}"))?;

                match shard.get_info(&GetShardRequest::default()) {
                    Ok(shard_info) if shard_info.metadata.is_some() => {
                        node_metadata.new_shard(
                            shard.id,
                            shard_info.metadata.unwrap().kbid,
                            shard_info.paragraphs as f32,
                        );

                        Ok(node_metadata)
                    }
                    Ok(_) => Err(node_error!("Missing shard metadata for {}", shard.id)),
                    Err(e) => Err(node_error!("Cannot get metrics for {}: {e:?}", shard.id)),
                }
            })
    }
}
