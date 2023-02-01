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
use nucliadb_core::protos::{DeleteGraphNodes, JoinGraph, Resource, ResourceId};
use nucliadb_core::tracing::{self, *};

use super::utils::*;
use crate::errors::RelationsErr as InnerErr;
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
            info!("{id:?} - Ending at {v} ms")
        }
        Ok(())
    }
}
impl RelationWriter for RelationsWriterService {
    #[tracing::instrument(skip_all)]
    fn delete_nodes(&mut self, graph: &DeleteGraphNodes) -> NodeResult<()> {
        let id = graph.shard_id.as_ref().map(|s| &s.id);
        let time = SystemTime::now();
        let mut writer = self.index.start_writing()?;
        for node in graph.nodes.iter() {
            let name = node.value.clone();
            let (xtype, subtype) = node_type_parsing(node.ntype(), &node.subtype);
            let node = IoNode::new(name, xtype.to_string(), subtype.map(|s| s.to_string()));
            if let Some(id) = writer.get_node_id(node.hash())? {
                self.delete_node(&mut writer, id)?;
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms")
        }
        Ok(writer.commit(&mut self.wmode)?)
    }
    #[tracing::instrument(skip_all)]
    fn join_graph(&mut self, graph: &JoinGraph) -> NodeResult<()> {
        let time = SystemTime::now();
        let mut writer = self.index.start_writing()?;

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("Creating nodes: starts {v} ms");
        }
        let nodes: HashMap<_, _> = graph
            .nodes
            .iter()
            .map(|(k, v)| (k, v.value.clone(), node_type_parsing(v.ntype(), &v.subtype)))
            .map(|(key, value, (xtype, subtype))| (key, value, xtype, subtype))
            .map(|(key, value, xtype, subtype)| (key, value, xtype.to_string(), subtype))
            .map(|(key, value, xtype, subtype)| (key, value, xtype, subtype.map(|s| s.to_string())))
            .map(|(&key, value, xtype, subtype)| (key, IoNode::user_node(value, xtype, subtype)))
            .collect();
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("Creating nodes: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("Populating the graph: starts {v} ms");
        }
        let ubehaviour = || Err(InnerErr::UBehaviour);
        for protos_edge in graph.edges.iter() {
            let from = nodes.get(&protos_edge.source).map_or_else(ubehaviour, Ok)?;
            let to = nodes.get(&protos_edge.target).map_or_else(ubehaviour, Ok)?;
            let edge = relation_type_parsing(protos_edge.rtype(), &protos_edge.rsubtype);
            let edge = IoEdge::new(edge.0.to_string(), edge.1.map(|s| s.to_string()));
            let metadata = protos_edge.metadata.clone().map(IoEdgeMetadata::from);
            writer.connect(&self.wmode, from, to, &edge, metadata.as_ref())?;
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("Populating the graph: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("Ending at {v} ms")
        }
        Ok(writer.commit(&mut self.wmode)?)
    }
}
impl std::fmt::Debug for RelationsWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationWriterService").finish()
    }
}

impl WriterChild for RelationsWriterService {
    #[tracing::instrument(skip_all)]
    fn stop(&mut self) -> NodeResult<()> {
        info!("Stopping relation writer Service");
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn count(&self) -> usize {
        let time = SystemTime::now();
        let count = self
            .index
            .start_reading()
            .and_then(|reader| reader.no_nodes())
            .map_err(|err| error!("{err:?}"))
            .unwrap_or_default();
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("Ending at {v} ms")
        }
        count as usize
    }
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, x: &ResourceId) -> NodeResult<()> {
        let id = Some(&x.shard_id);
        let time = SystemTime::now();
        let node = IoNode::new(x.uuid.clone(), dictionary::ENTITY.to_string(), None);
        let mut writer = self.index.start_writing()?;
        if let Some(id) = writer.get_node_id(node.hash())? {
            self.delete_node(&mut writer, id)?;
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms")
        }
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let id = Some(&resource.shard_id);
        let time = SystemTime::now();
        if resource.status != ResourceStatus::Delete as i32 {
            if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
                info!("{id:?} - Populating the graph: starts {v} ms");
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
                info!("{id:?} - Populating the graph: ends {v} ms");
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms")
        }
        Ok(())
    }
    fn garbage_collection(&mut self) {}
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
            std::fs::create_dir_all(path)?;
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
