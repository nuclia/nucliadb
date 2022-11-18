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

use std::io::Cursor;

use nucliadb_node::config::Configuration;
use nucliadb_node::reader::NodeReaderService as RustReaderService;
use nucliadb_node::writer::NodeWriterService as RustWriterService;
use nucliadb_protos::{
    op_status, DocumentSearchRequest, OpStatus, ParagraphSearchRequest, RelationSearchRequest,
    Resource, ResourceId, SearchRequest, Shard as ShardPB, ShardId, SuggestRequest,
    VectorSearchRequest,
};
use prost::Message;
use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::PyList;
use tracing::*;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
type RawProtos = Vec<u8>;

#[pyclass]
pub struct NodeReader {
    reader: RustReaderService,
}
impl Default for NodeReader {
    fn default() -> NodeReader {
        NodeReader::new()
    }
}

#[pymethods]
impl NodeReader {
    #[staticmethod]
    pub fn new() -> NodeReader {
        NodeReader {
            reader: RustReaderService::new(),
        }
    }

    pub fn get_shard<'p>(&mut self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shard_id = ShardId::decode(&mut Cursor::new(shard_id)).unwrap();
        self.reader.load_shard(&shard_id);
        if let Some(shard) = self.reader.get_shard(&shard_id) {
            let stats = shard.get_info();
            let shard_pb = ShardPB {
                shard_id: String::from(&shard.id),
                resources: stats.resources as u64,
                paragraphs: stats.paragraphs as u64,
                sentences: stats.sentences as u64,
            };
            Ok(PyList::new(py, shard_pb.encode_to_vec()))
        } else {
            Err(exceptions::PyTypeError::new_err("Not found"))
        }
    }

    pub fn get_shards<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shards = self.reader.get_shards();
        Ok(PyList::new(py, shards.encode_to_vec()))
    }

    pub fn search<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let search_request = SearchRequest::decode(&mut Cursor::new(request)).unwrap();
        let shard_id = ShardId {
            id: search_request.shard.clone(),
        };
        self.reader.load_shard(&shard_id);
        let response = self.reader.search(&shard_id, search_request);
        match response {
            Some(Ok(response)) => Ok(PyList::new(py, response.encode_to_vec())),
            Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
            None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
        }
    }

    pub fn suggest<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let suggest_request = SuggestRequest::decode(&mut Cursor::new(request)).unwrap();
        let shard_id = ShardId {
            id: suggest_request.shard.clone(),
        };
        self.reader.load_shard(&shard_id);
        let response = self.reader.suggest(&shard_id, suggest_request);
        match response {
            Some(Ok(response)) => Ok(PyList::new(py, response.encode_to_vec())),
            Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
            None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
        }
    }

    pub fn vector_search<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let vector_request = VectorSearchRequest::decode(&mut Cursor::new(request)).unwrap();
        let shard_id = ShardId {
            id: vector_request.id.clone(),
        };
        self.reader.load_shard(&shard_id);
        let response = self.reader.vector_search(&shard_id, vector_request);
        match response {
            Some(Ok(response)) => Ok(PyList::new(py, response.encode_to_vec())),
            Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
            None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
        }
    }

    pub fn document_search<'p>(
        &mut self,
        request: RawProtos,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let document_request = DocumentSearchRequest::decode(&mut Cursor::new(request)).unwrap();
        let shard_id = ShardId {
            id: document_request.id.clone(),
        };
        self.reader.load_shard(&shard_id);
        let response = self.reader.document_search(&shard_id, document_request);
        match response {
            Some(Ok(response)) => Ok(PyList::new(py, response.encode_to_vec())),
            Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
            None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
        }
    }

    pub fn paragraph_search<'p>(
        &mut self,
        request: RawProtos,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let paragraph_request = ParagraphSearchRequest::decode(&mut Cursor::new(request)).unwrap();
        let shard_id = ShardId {
            id: paragraph_request.id.clone(),
        };
        self.reader.load_shard(&shard_id);
        let response = self.reader.paragraph_search(&shard_id, paragraph_request);
        match response {
            Some(Ok(response)) => Ok(PyList::new(py, response.encode_to_vec())),
            Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
            None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
        }
    }

    pub fn relation_search<'p>(
        &mut self,
        request: RawProtos,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let paragraph_request = RelationSearchRequest::decode(&mut Cursor::new(request)).unwrap();
        let shard_id = ShardId {
            id: paragraph_request.id.clone(),
        };
        self.reader.load_shard(&shard_id);
        let response = self.reader.relation_search(&shard_id, paragraph_request);
        match response {
            Some(Ok(response)) => Ok(PyList::new(py, response.encode_to_vec())),
            Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
            None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
        }
    }
}

#[pyclass]
pub struct NodeWriter {
    writer: RustWriterService,
}

impl Default for NodeWriter {
    fn default() -> NodeWriter {
        NodeWriter::new()
    }
}
#[pymethods]
impl NodeWriter {
    #[staticmethod]
    pub fn new() -> NodeWriter {
        NodeWriter {
            writer: RustWriterService::new(),
        }
    }

    pub fn get_shard<'p>(&mut self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shard_id = ShardId::decode(&mut Cursor::new(shard_id)).unwrap();
        self.writer.load_shard(&shard_id);
        match self.writer.get_shard(&shard_id) {
            Some(_) => Ok(PyList::new(py, shard_id.encode_to_vec())),
            None => Err(exceptions::PyTypeError::new_err("Not found")),
        }
    }

    pub fn new_shard<'p>(&mut self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shard = self.writer.new_shard();
        Ok(PyList::new(py, shard.encode_to_vec()))
    }

    pub fn delete_shard<'p>(&mut self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shard_id = ShardId::decode(&mut Cursor::new(shard_id)).unwrap();
        self.writer.load_shard(&shard_id);
        match self.writer.delete_shard(&shard_id) {
            Some(Ok(_)) => Ok(PyList::new(py, shard_id.encode_to_vec())),
            Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
            None => Err(exceptions::PyTypeError::new_err("Shard not found")),
        }
    }

    pub fn clean_and_upgrade_shard<'p>(
        &mut self,
        shard_id: RawProtos,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let shard_id = ShardId::decode(&mut Cursor::new(shard_id)).unwrap();
        self.writer.load_shard(&shard_id);
        match self.writer.clean_and_upgrade_shard(&shard_id) {
            Ok(clean_data) => Ok(PyList::new(py, clean_data.encode_to_vec())),
            Err(e) => Err(exceptions::PyTypeError::new_err(e.to_string())),
        }
    }

    pub fn list_shards<'p>(&mut self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shard_ids = self.writer.get_shard_ids();
        Ok(PyList::new(py, shard_ids.encode_to_vec()))
    }

    pub fn set_resource<'p>(&mut self, resource: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let resource = Resource::decode(&mut Cursor::new(resource)).unwrap();
        let shard_id = ShardId {
            id: resource.shard_id.clone(),
        };
        self.writer.load_shard(&shard_id);
        match self.writer.set_resource(&shard_id, &resource) {
            Some(Ok(count)) => {
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: count as u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Some(Err(e)) => {
                let status = op_status::Status::Error as i32;
                let detail = format!("Error: {}", e);
                let op_status = OpStatus {
                    status,
                    detail,
                    count: 0_u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(PyList::new(py, op_status.encode_to_vec()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(exceptions::PyTypeError::new_err(message))
            }
        }
    }

    pub fn remove_resource<'p>(
        &mut self,
        resource: RawProtos,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let resource = ResourceId::decode(&mut Cursor::new(resource)).unwrap();
        let shard_id = ShardId {
            id: resource.shard_id.clone(),
        };
        self.writer.load_shard(&shard_id);
        match self.writer.remove_resource(&shard_id, &resource) {
            Some(Ok(count)) => {
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: count as u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Some(Err(e)) => {
                let status = op_status::Status::Error as i32;
                let detail = format!("Error: {}", e);
                let op_status = OpStatus {
                    status,
                    detail,
                    count: 0_u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(PyList::new(py, op_status.encode_to_vec()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(exceptions::PyTypeError::new_err(message))
            }
        }
    }

    pub fn gc<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shard_id = ShardId::decode(&mut Cursor::new(request)).unwrap();
        self.writer.load_shard(&shard_id);
        match self.writer.gc(&shard_id) {
            Some(Ok(_)) => {
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: 0,
                    shard_id: shard_id.id.clone(),
                };
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Some(Err(e)) => {
                let status = op_status::Status::Error as i32;
                let detail = format!("Error: {}", e);
                let op_status = OpStatus {
                    status,
                    detail,
                    count: 0_u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(PyList::new(py, op_status.encode_to_vec()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(exceptions::PyTypeError::new_err(message))
            }
        }
    }
}

#[pymodule]
fn nucliadb_node_binding(_py: Python, m: &PyModule) -> PyResult<()> {
    let log_levels = Configuration::log_level();

    let mut layers = Vec::new();
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_filter(Targets::new().with_targets(log_levels))
        .boxed();

    layers.push(stdout_layer);

    let _reg = tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .map_err(|e| {
            error!("Try init error: {e}");
        });
    m.add_class::<NodeWriter>()?;
    m.add_class::<NodeReader>()?;
    Ok(())
}
