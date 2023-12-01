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
use std::time::SystemTime;

use nucliadb_core::prelude::*;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId};
use nucliadb_core::tracing::{self, *};
use nucliadb_core::IndexFiles;
use nucliadb_procs::measure;

use super::utils::*;
use crate::index::*;

pub struct RelationsWriterService {
    wmode: WMode,
    index: Index,
}
impl RelationsWriterService {
    #[tracing::instrument(skip_all)]
    fn delete_node(&self, writer: &mut GraphWriter, id: Entity) -> NodeResult<()> {
        let time = SystemTime::now();
        let affects = writer.delete_node(&self.wmode, id)?;
        for affected in affects {
            let affected_value = writer.get_node(affected)?;
            let no_in = writer.get_inedges(affected)?.count();
            let no_out = writer.get_outedges(affected)?.count();
            if no_in == 0 && no_out == 0 && !affected_value.defined_by_user() {
                writer.delete_node(&self.wmode, affected)?;
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Ending at {v} ms")
        }
        Ok(())
    }
}

impl RelationWriter for RelationsWriterService {}

impl std::fmt::Debug for RelationsWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationWriterService").finish()
    }
}

impl WriterChild for RelationsWriterService {
    #[measure(actor = "relations", metric = "count")]
    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        let time = SystemTime::now();

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("Count starting at {v} ms");
        }
        let count = self
            .index
            .start_reading()
            .and_then(|reader| reader.no_nodes())?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("Ending at {v} ms")
        }

        Ok(count as usize)
    }

    #[measure(actor = "relations", metric = "delete_resource")]
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, x: &ResourceId) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = Some(&x.shard_id);
        let node = IoNode::new(x.uuid.clone(), dictionary::ENTITY.to_string(), None);
        let mut writer = self.index.start_writing()?;
        if let Some(id) = writer.get_node_id(node.hash())? {
            self.delete_node(&mut writer, id)?;
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Ending at {v} ms")
        }

        Ok(())
    }

    #[measure(actor = "relations", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = Some(&resource.shard_id);
        if resource.status != ResourceStatus::Delete as i32 {
            if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
                debug!("{id:?} - Populating the graph: starts {v} ms");
            }
            let iter = resource
                .relations
                .iter()
                .filter(|rel| rel.to.is_some() || rel.source.is_some());
            let mut writer = self.index.start_writing()?;
            for rel in iter {
                let edge = relation_type_parsing(rel.relation(), &rel.relation_label);
                let from = rel.source.as_ref().unwrap();
                let from_type = node_type_parsing(from.ntype(), &from.subtype);
                let to = rel.to.as_ref().unwrap();
                let to_type = node_type_parsing(to.ntype(), &to.subtype);
                let from = IoNode::system_node(
                    from.value.clone(),
                    from_type.0.to_string(),
                    from_type.1.map(|s| s.to_string()),
                );
                let to = IoNode::system_node(
                    to.value.clone(),
                    to_type.0.to_string(),
                    to_type.1.map(|s| s.to_string()),
                );
                let edge = IoEdge::new(edge.0.to_string(), edge.1.map(|s| s.to_string()));
                let metadata = rel.metadata.clone().map(IoEdgeMetadata::from);
                writer.connect(&self.wmode, &from, &to, &edge, metadata.as_ref())?;
            }
            writer.commit(&mut self.wmode)?;
            if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
                debug!("{id:?} - Populating the graph: ends {v} ms");
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Ending at {v} ms")
        }

        Ok(())
    }
    fn garbage_collection(&mut self) -> NodeResult<()> {
        Ok(())
    }
    fn get_segment_ids(&self) -> NodeResult<Vec<String>> {
        // not implemented, not supported right now
        Ok(Vec::new())
    }

    fn get_index_files(&self, _ignored_segment_ids: &[String]) -> NodeResult<IndexFiles> {
        // not implemented, not supported right now
        Ok(IndexFiles {
            metadata_files: HashMap::new(),
            files: Vec::new(),
        })
    }
}

impl RelationsWriterService {
    pub fn start(config: &RelationConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            match RelationsWriterService::new(config) {
                Err(e) if path.exists() => {
                    std::fs::remove_dir(path)?;
                    Err(e)
                }
                Err(e) => Err(e),
                Ok(v) => Ok(v),
            }
        } else {
            Ok(RelationsWriterService::open(config)?)
        }
    }
    pub fn new(config: &RelationConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(node_error!("Shard does exist".to_string()))
        } else {
            std::fs::create_dir(path)?;
            let (index, wmode) = Index::new_writer(path)?;
            Ok(RelationsWriterService { index, wmode })
        }
    }

    pub fn open(config: &RelationConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(node_error!("Shard does not exist".to_string()))
        } else {
            let (index, wmode) = Index::new_writer(path)?;
            Ok(RelationsWriterService { index, wmode })
        }
    }
}
