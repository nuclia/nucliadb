use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use nucliadb_core::protos::ShardMetadata as GrpcMetadata;
use nucliadb_core::{node_error, NodeResult};
use serde::*;
pub const SHARD_METADATA: &str = "metadata.json";

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct ShardMetadata {
    pub kb_id: Option<String>,
}

impl From<ShardMetadata> for GrpcMetadata {
    fn from(x: ShardMetadata) -> GrpcMetadata {
        GrpcMetadata {
            kb_id: x.kb_id.unwrap_or_default(),
        }
    }
}
impl From<GrpcMetadata> for ShardMetadata {
    fn from(value: GrpcMetadata) -> Self {
        ShardMetadata {
            kb_id: if value.kb_id.is_empty() {
                None
            } else {
                Some(value.kb_id)
            },
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
            kb_id: Some("KB".to_string()),
        };
        meta.serialize(&metadata_path).unwrap();
        let meta_disk = ShardMetadata::open(&metadata_path).unwrap();
        assert_eq!(meta.kb_id, meta_disk.kb_id);
    }
    #[test]
    fn open_empty() {
        let dir = TempDir::new().unwrap();
        let metadata_path = dir.path().join("metadata.json");
        let meta_disk = ShardMetadata::open(&metadata_path).unwrap();
        assert!(meta_disk.kb_id.is_none());
    }
}
