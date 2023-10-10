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

/// Shard metadata, defined at the moment of creation.
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use nucliadb_core::protos::{NewShardRequest, ShardMetadata as GrpcMetadata, VectorSimilarity};
use nucliadb_core::{node_error, Channel, NodeResult};
use serde::*;

#[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Similarity {
    #[default]
    Cosine,
    Dot,
}

impl From<VectorSimilarity> for Similarity {
    fn from(value: VectorSimilarity) -> Self {
        match value {
            VectorSimilarity::Cosine => Similarity::Cosine,
            VectorSimilarity::Dot => Similarity::Dot,
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
impl From<Similarity> for VectorSimilarity {
    fn from(value: Similarity) -> Self {
        match value {
            Similarity::Cosine => VectorSimilarity::Cosine,
            Similarity::Dot => VectorSimilarity::Dot,
        }
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct ShardMetadata {
    pub kbid: Option<String>,
    pub similarity: Option<Similarity>,
    pub id: Option<String>,
    #[serde(default)]
    pub channel: Option<Channel>,
}

impl From<ShardMetadata> for GrpcMetadata {
    fn from(x: ShardMetadata) -> GrpcMetadata {
        GrpcMetadata {
            kbid: x.kbid.unwrap_or_default(),
            release_channel: x.channel.unwrap_or_default() as i32,
        }
    }
}
impl From<NewShardRequest> for ShardMetadata {
    fn from(value: NewShardRequest) -> Self {
        ShardMetadata {
            similarity: Some(value.similarity().into()),
            kbid: Some(value.kbid).filter(|s| !s.is_empty()),
            channel: Some(Channel::from(value.release_channel)),
            id: None,
        }
    }
}

impl ShardMetadata {
    pub fn open(metadata: &Path) -> NodeResult<ShardMetadata> {
        if !metadata.exists() {
            return Ok(ShardMetadata::default());
        }

        let mut reader = BufReader::new(File::open(metadata)?);
        Ok(serde_json::from_reader(&mut reader)?)
    }
    pub fn serialize(&self, metadata: &Path) -> NodeResult<()> {
        if metadata.exists() {
            return Err(node_error!("Metadata file already exists at {metadata:?}"));
        }

        let mut writer = BufWriter::new(File::create(metadata)?);
        serde_json::to_writer(&mut writer, self)?;
        Ok(writer.flush()?)
    }
    pub fn kbid(&self) -> Option<&str> {
        self.kbid.as_deref()
    }
    pub fn similarity(&self) -> VectorSimilarity {
        self.similarity.unwrap_or(Similarity::Cosine).into()
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;
    #[test]
    fn create() {
        let dir = TempDir::new().unwrap();
        let metadata_path = dir.path().join("metadata.json");
        let meta = ShardMetadata {
            kbid: Some("KB".to_string()),
            similarity: Some(Similarity::Cosine),
            channel: Some(Channel::EXPERIMENTAL),
            id: None,
        };
        meta.serialize(&metadata_path).unwrap();
        let meta_disk = ShardMetadata::open(&metadata_path).unwrap();
        assert_eq!(meta.kbid, meta_disk.kbid);
        assert_eq!(meta.similarity, meta_disk.similarity);
    }
    #[test]
    fn open_empty() {
        let dir = TempDir::new().unwrap();
        let metadata_path = dir.path().join("metadata.json");
        let meta_disk = ShardMetadata::open(&metadata_path).unwrap();
        assert!(meta_disk.kbid.is_none());
    }
}
