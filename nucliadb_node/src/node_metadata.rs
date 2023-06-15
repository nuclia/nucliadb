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

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use nucliadb_core::tracing::*;
use nucliadb_core::NodeResult;
use serde::{Deserialize, Serialize};

use crate::env;

fn number_of_shards() -> NodeResult<usize> {
    Ok(std::fs::read_dir(env::shards_path())?
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .count())
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct NodeMetadata {
    #[serde(default)]
    shard_count: u64,
}

impl From<NodeMetadata> for nucliadb_core::protos::NodeMetadata {
    fn from(node_metadata: NodeMetadata) -> Self {
        nucliadb_core::protos::NodeMetadata {
            shard_count: node_metadata.shard_count,
            ..Default::default()
        }
    }
}

impl NodeMetadata {
    pub fn shard_count(&self) -> u64 {
        self.shard_count
    }

    pub fn new_shard(&mut self) {
        self.shard_count += 1;
    }

    pub fn delete_shard(&mut self) {
        self.shard_count -= 1;
    }
    pub fn load_or_create(path: &Path) -> NodeResult<Self> {
        if !path.exists() {
            debug!("Node metadata file does not exist.");

            let node_metadata = Self::create(path).unwrap_or_else(|e| {
                warn!("Cannot create metadata file '{}': {e}", path.display());
                debug!("Create default metadata file '{}'", path.display());

                Self::default()
            });

            node_metadata.save(path)?;
            Ok(node_metadata)
        } else {
            Self::load(path)
        }
    }

    pub fn save(&self, path: &Path) -> NodeResult<()> {
        debug!("Saving node metadata file '{}'", path.display());
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &self)?;
        Ok(writer.flush()?)
    }

    pub fn load(path: &Path) -> NodeResult<Self> {
        debug!("Loading node metadata file '{}'", path.display());
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }

    pub fn create(path: &Path) -> NodeResult<Self> {
        debug!("Creating node metadata file '{}'", path.display());
        number_of_shards()
            .map(|i| i as u64)
            .map(|shard_count| NodeMetadata { shard_count })
    }
}
