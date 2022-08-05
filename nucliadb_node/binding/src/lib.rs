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

use nucliadb_node::config::Configuration;
use std::io::Cursor;
use std::sync::Arc;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use nucliadb_node::reader::NodeReaderService as RustReaderService;
use nucliadb_node::writer::NodeWriterService as RustWriterService;
use nucliadb_protos::{
    op_status, DocumentSearchRequest, OpStatus, ParagraphSearchRequest, RelationSearchRequest,
    Resource, ResourceId, SearchRequest, ShardId, VectorSearchRequest,
};
use prost::Message;
use pyo3::exceptions;
use pyo3::prelude::*;
use tokio::sync::RwLock;
use tracing::*;
type RawProtos = Vec<u8>;

#[pyclass]
pub struct NodeReader {
    reader: Arc<RwLock<RustReaderService>>,
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
            reader: Arc::new(RwLock::new(RustReaderService::new())),
        }
    }
    pub fn get_shard<'p>(&self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let reader = self.reader.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let shard_id = ShardId::decode(&mut Cursor::new(shard_id)).unwrap();

            let mut lock = reader.write().await;
            match lock.get_shard(&shard_id).await {
                Some(_) => Ok(shard_id.encode_to_vec()),
                None => Err(exceptions::PyTypeError::new_err("Not found")),
            }
        })
    }
    pub fn get_shards<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let reader = self.reader.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let lock = reader.write().await;
            let shards = lock.get_shards().await;
            Ok(shards.encode_to_vec())
        })
    }
    pub fn search<'p>(&self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let reader = self.reader.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut lock = reader.write().await;
            let search_request = SearchRequest::decode(&mut Cursor::new(request)).unwrap();
            let shard_id = ShardId {
                id: search_request.shard.clone(),
            };
            let response = lock.search(&shard_id, search_request).await;
            match response {
                Some(Ok(response)) => Ok(response.encode_to_vec()),
                Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
                None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
            }
        })
    }
    pub fn vector_search<'p>(&self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let reader = self.reader.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut lock = reader.write().await;
            let vector_request = VectorSearchRequest::decode(&mut Cursor::new(request)).unwrap();
            let shard_id = ShardId {
                id: vector_request.id.clone(),
            };
            let response = lock.vector_search(&shard_id, vector_request).await;
            match response {
                Some(Ok(response)) => Ok(response.encode_to_vec()),
                Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
                None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
            }
        })
    }
    pub fn document_search<'p>(&self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let reader = self.reader.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut lock = reader.write().await;
            let document_request =
                DocumentSearchRequest::decode(&mut Cursor::new(request)).unwrap();
            let shard_id = ShardId {
                id: document_request.id.clone(),
            };
            let response = lock.document_search(&shard_id, document_request).await;
            match response {
                Some(Ok(response)) => Ok(response.encode_to_vec()),
                Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
                None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
            }
        })
    }
    pub fn paragraph_search<'p>(&self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let reader = self.reader.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut lock = reader.write().await;
            let paragraph_request =
                ParagraphSearchRequest::decode(&mut Cursor::new(request)).unwrap();
            let shard_id = ShardId {
                id: paragraph_request.id.clone(),
            };
            let response = lock.paragraph_search(&shard_id, paragraph_request).await;
            match response {
                Some(Ok(response)) => Ok(response.encode_to_vec()),
                Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
                None => Err(exceptions::PyTypeError::new_err("Error loading shard")),
            }
        })
    }
    pub fn relation_search<'p>(&self, request: RawProtos, _py: Python<'p>) -> PyResult<&'p PyAny> {
        let _ = RelationSearchRequest::decode(&mut Cursor::new(request)).unwrap();
        todo!()
    }
}

#[pyclass]
pub struct NodeWriter {
    writer: Arc<RwLock<RustWriterService>>,
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
            writer: Arc::new(RwLock::new(RustWriterService::new())),
        }
    }

    pub fn get_shard<'p>(&self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let writer = self.writer.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let shard_id = ShardId::decode(&mut Cursor::new(shard_id)).unwrap();

            let mut lock = writer.write().await;
            match lock.get_shard(&shard_id).await {
                Some(_) => Ok(shard_id.encode_to_vec()),
                None => Err(exceptions::PyTypeError::new_err("Not found")),
            }
        })
    }

    pub fn new_shard<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let writer = self.writer.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut w = writer.write().await;
            let shard = w.new_shard().await;
            Ok(shard.encode_to_vec())
        })
    }

    pub fn delete_shard<'p>(&self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let writer = self.writer.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let shard_id = ShardId::decode(&mut Cursor::new(shard_id)).unwrap();

            let mut w = writer.write().await;
            match w.delete_shard(&shard_id).await {
                Some(Ok(_)) => Ok(shard_id.encode_to_vec()),
                Some(Err(e)) => Err(exceptions::PyTypeError::new_err(e.to_string())),
                None => Err(exceptions::PyTypeError::new_err("Shard not found")),
            }
        })
    }

    pub fn list_shards<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let writer = self.writer.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let w = writer.read().await;
            let shard_ids = w.get_shard_ids();
            Ok(shard_ids.encode_to_vec())
        })
    }

    pub fn set_resource<'p>(&self, resource: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let writer = self.writer.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut lock = writer.write().await;
            let resource = Resource::decode(&mut Cursor::new(resource)).unwrap();

            let shard_id = ShardId {
                id: resource.shard_id.clone(),
            };
            match lock.set_resource(&shard_id, &resource).await {
                Some(Ok(count)) => {
                    let status = OpStatus {
                        status: 0,
                        detail: "Success!".to_string(),
                        count: count as u64,
                        shard_id: shard_id.id.clone(),
                    };
                    Ok(status.encode_to_vec())
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
                    Ok(op_status.encode_to_vec())
                }
                None => {
                    let message = format!("Error loading shard {:?}", shard_id);
                    Err(exceptions::PyTypeError::new_err(message))
                }
            }
        })
    }

    pub fn remove_resource<'p>(&self, resource: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let writer = self.writer.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut lock = writer.write().await;
            let resource = ResourceId::decode(&mut Cursor::new(resource)).unwrap();

            let shard_id = ShardId {
                id: resource.shard_id.clone(),
            };
            match lock.remove_resource(&shard_id, &resource).await {
                Some(Ok(count)) => {
                    let status = OpStatus {
                        status: 0,
                        detail: "Success!".to_string(),
                        count: count as u64,
                        shard_id: shard_id.id.clone(),
                    };
                    Ok(status.encode_to_vec())
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
                    Ok(op_status.encode_to_vec())
                }
                None => {
                    let message = format!("Error loading shard {:?}", shard_id);
                    Err(exceptions::PyTypeError::new_err(message))
                }
            }
        })
    }

    pub fn gc<'p>(&self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let writer = self.writer.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut lock = writer.write().await;
            let request = ShardId::decode(&mut Cursor::new(request)).unwrap();
            match lock.gc(&request).await {
                Some(Ok(_)) => {
                    let status = OpStatus {
                        status: 0,
                        detail: "Success!".to_string(),
                        count: 0,
                        shard_id: request.id.clone(),
                    };
                    Ok(status.encode_to_vec())
                }
                Some(Err(e)) => {
                    let status = op_status::Status::Error as i32;
                    let detail = format!("Error: {}", e);
                    let op_status = OpStatus {
                        status,
                        detail,
                        count: 0_u64,
                        shard_id: request.id.clone(),
                    };
                    Ok(op_status.encode_to_vec())
                }
                None => {
                    let message = format!("Error loading shard {:?}", request);
                    Err(exceptions::PyTypeError::new_err(message))
                }
            }
        })
    }
}

#[pymodule]
fn nucliadb_node_binding(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
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
