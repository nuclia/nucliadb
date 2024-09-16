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

use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::sync::Arc;

use crate::collect_garbage::{garbage_collection_loop, GCParameters};
use crate::errors::{op_status_error, IndexNodeException, LoadShardError};
use crate::RawProtos;
use nucliadb_core::merge::MergerError;
use nucliadb_core::protos::*;
use nucliadb_node::analytics::blocking::send_analytics_event;
use nucliadb_node::analytics::payload::AnalyticsEvent;
use nucliadb_node::cache::ShardWriterCache;
use nucliadb_node::settings::load_settings;
use nucliadb_node::shards::indexes::DEFAULT_VECTORS_INDEX_NAME;
use nucliadb_node::shards::shard_writer::NewShard;
use nucliadb_node::shards::writer::ShardWriter;
use nucliadb_node::{lifecycle, VectorConfig};
use prost::Message;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::thread::JoinHandle;
use std::time::Duration;

const GC_LOOP_INTERVAL: Duration = Duration::from_secs(60);

#[pyclass]
pub struct NodeWriter {
    #[allow(unused)]
    gc_loop_handle: JoinHandle<()>,
    shards: Arc<ShardWriterCache>,
}

impl NodeWriter {
    fn obtain_shard(&self, shard_id: String) -> Result<Arc<ShardWriter>, PyErr> {
        self.shards
            .get(&shard_id)
            .map_err(|error| LoadShardError::new_err(format!("Error loading shard {}: {}", shard_id, error)))
    }
}

#[pymethods]
impl NodeWriter {
    #[new]
    pub fn new() -> PyResult<Self> {
        let settings = load_settings().unwrap();
        let shard_cache = Arc::new(ShardWriterCache::new(settings.clone()));
        let shards_gc_loop_copy = Arc::clone(&shard_cache);
        let gc_parameters = GCParameters {
            shards_path: settings.shards_path(),
            loop_interval: GC_LOOP_INTERVAL,
        };
        let gc_loop_handle = std::thread::spawn(|| {
            garbage_collection_loop(gc_parameters, shards_gc_loop_copy);
        });

        if let Err(error) = lifecycle::initialize_writer(settings.clone()) {
            return Err(IndexNodeException::new_err(format!("Unable to initialize writer: {error}")));
        };

        match lifecycle::initialize_merger(Arc::clone(&shard_cache), settings.clone()) {
            Ok(()) => (),
            Err(MergerError::GlobalMergerAlreadyInstalled) => (),
        }

        Ok(Self {
            gc_loop_handle,
            shards: shard_cache,
        })
    }

    pub fn new_shard<'p>(&self, metadata: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        send_analytics_event(AnalyticsEvent::Create);

        let request = NewShardRequest::decode(&mut Cursor::new(metadata)).expect("Error decoding arguments");

        let kbid = request.kbid.clone();
        let shard_id = uuid::Uuid::new_v4().to_string();

        #[allow(deprecated)]
        let vector_configs = if !request.vectorsets_configs.is_empty() {
            // create shard maybe with multiple vectorsets
            let mut configs = HashMap::with_capacity(request.vectorsets_configs.len());
            for (vectorset_id, config) in request.vectorsets_configs {
                configs.insert(
                    vectorset_id,
                    VectorConfig::try_from(config).map_err(|e| IndexNodeException::new_err(e.to_string()))?,
                );
            }
            configs
        } else if let Some(vector_index_config) = request.config {
            // DEPRECATED bw/c code, remove when shard creators populate
            // vector_index_configs
            HashMap::from([(
                DEFAULT_VECTORS_INDEX_NAME.to_string(),
                VectorConfig::try_from(vector_index_config).map_err(|e| IndexNodeException::new_err(e.to_string()))?,
            )])
        } else {
            // DEPRECATED bw/c code, remove when shard creators populate
            // vector_index_configs
            HashMap::from([(
                DEFAULT_VECTORS_INDEX_NAME.to_string(),
                VectorConfig {
                    similarity: request.similarity().into(),
                    normalize_vectors: request.normalize_vectors,
                    ..Default::default()
                },
            )])
        };

        let new_shard = self.shards.create(NewShard {
            kbid,
            shard_id,
            vector_configs,
        });
        match new_shard {
            Ok(new_shard) => Ok(PyList::new(
                py,
                ShardCreated {
                    document_service: new_shard.document_version() as i32,
                    paragraph_service: new_shard.paragraph_version() as i32,
                    vector_service: new_shard.vector_version() as i32,
                    relation_service: new_shard.relation_version() as i32,
                    id: new_shard.id.clone(),
                }
                .encode_to_vec(),
            )),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn delete_shard<'p>(&mut self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        send_analytics_event(AnalyticsEvent::Delete);
        let shard_id = ShardId::decode(&mut Cursor::new(shard_id)).expect("Error decoding arguments");
        let deleted = self.shards.delete(&shard_id.id.clone());
        match deleted {
            Ok(_) => Ok(PyList::new(py, shard_id.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn list_shards<'p>(&mut self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let entries = fs::read_dir(self.shards.shards_path.clone())?;
        let mut shard_ids = Vec::new();
        for entry in entries {
            let entry_path = entry.unwrap().path();
            if entry_path.is_dir() {
                if let Some(id) = entry_path.file_name().map(|s| s.to_str().map(String::from)) {
                    shard_ids.push(ShardId {
                        id: id.unwrap(),
                    });
                }
            }
        }
        Ok(PyList::new(
            py,
            (ShardIds {
                ids: shard_ids,
            })
            .encode_to_vec(),
        ))
    }

    pub fn set_resource<'p>(&mut self, resource: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let resource = Resource::decode(&mut Cursor::new(resource)).expect("Error decoding arguments");
        let shard_id = resource.shard_id.clone();
        let shard = self.obtain_shard(shard_id.clone())?;
        let result = shard.set_resource(resource);
        let status = match result {
            Ok(()) => OpStatus {
                status: op_status::Status::Ok as i32,
                detail: "Success!".to_string(),
                ..Default::default()
            },
            Err(error) => OpStatus {
                status: op_status::Status::Error as i32,
                detail: error.to_string(),
                ..Default::default()
            },
        };
        Ok(PyList::new(py, status.encode_to_vec()))
    }

    pub fn set_resource_from_storage<'p>(&mut self, _index_message: RawProtos, _py: Python<'p>) -> PyResult<&'p PyAny> {
        Err(IndexNodeException::new_err("Not implemented"))
    }

    pub fn remove_resource<'p>(&mut self, resource: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let resource = ResourceId::decode(&mut Cursor::new(resource)).expect("Error decoding arguments");
        let shard_id = resource.shard_id.clone();
        let shard = self.obtain_shard(shard_id.clone())?;
        let result = shard.remove_resource(&resource);
        let status = match result {
            Ok(()) => OpStatus {
                status: op_status::Status::Ok as i32,
                detail: "Success!".to_string(),
                ..Default::default()
            },
            Err(error) => OpStatus {
                status: op_status::Status::Error as i32,
                detail: error.to_string(),
                ..Default::default()
            },
        };
        Ok(PyList::new(py, status.encode_to_vec()))
    }

    pub fn add_vectorset<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let request = NewVectorSetRequest::decode(&mut Cursor::new(request)).expect("Error decoding arguments");

        let Some(VectorSetId {
            shard: Some(ShardId {
                id: shard_id,
            }),
            vectorset,
        }) = request.id
        else {
            return Ok(op_status_error(py, "Vectorset ID must be provided"));
        };
        let config = match request.config.map(VectorConfig::try_from) {
            Some(Ok(config)) => config,
            Some(Err(error)) => return Ok(op_status_error(py, error.to_string())),
            None => return Ok(op_status_error(py, "Vector index config must be provided")),
        };

        let shard = self.obtain_shard(shard_id.clone())?;
        let result = shard.create_vectors_index(vectorset, config);

        let status = match result {
            Ok(()) => OpStatus {
                status: op_status::Status::Ok.into(),
                detail: "Vectorset successfully created".to_string(),
                ..Default::default()
            },
            Err(error) => OpStatus {
                status: op_status::Status::Error.into(),
                detail: error.to_string(),
                ..Default::default()
            },
        };
        Ok(PyList::new(py, status.encode_to_vec()))
    }

    pub fn list_vectorsets<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let request = ShardId::decode(&mut Cursor::new(request)).expect("Error decoding arguments");

        let shard_id = request.id;
        let shard = self.obtain_shard(shard_id.clone())?;
        let vectorsets = shard.list_vectors_indexes();

        let vectorsets = VectorSetList {
            shard: Some(ShardId {
                id: shard_id,
            }),
            vectorsets,
        };
        Ok(PyList::new(py, vectorsets.encode_to_vec()))
    }

    pub fn remove_vectorset<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let request = VectorSetId::decode(&mut Cursor::new(request)).expect("Error decoding arguments");

        let VectorSetId {
            shard: Some(ShardId {
                id: shard_id,
            }),
            vectorset,
        } = request
        else {
            return Ok(PyList::new(
                py,
                OpStatus {
                    status: op_status::Status::Error.into(),
                    detail: "Vectorset ID must be provided".to_string(),
                    ..Default::default()
                }
                .encode_to_vec(),
            ));
        };

        let shard = self.obtain_shard(shard_id.clone())?;
        let result = shard.remove_vectors_index(vectorset);

        let status = match result {
            Ok(()) => OpStatus {
                status: op_status::Status::Ok.into(),
                detail: "Vectorset successfully deleted".to_string(),
                ..Default::default()
            },
            Err(error) => OpStatus {
                status: op_status::Status::Error.into(),
                detail: error.to_string(),
                ..Default::default()
            },
        };
        Ok(PyList::new(py, status.encode_to_vec()))
    }

    pub fn gc<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        send_analytics_event(AnalyticsEvent::GarbageCollect);
        let shard_id = ShardId::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard = self.obtain_shard(shard_id.id)?;
        let result = shard.force_garbage_collection();
        match result {
            Ok(_) => {
                let response = EmptyResponse {};
                Ok(PyList::new(py, response.encode_to_vec()))
            }
            Err(error) => {
                let message = format!("Garbage collection failed: {}", error);
                Err(IndexNodeException::new_err(message))
            }
        }
    }
}
