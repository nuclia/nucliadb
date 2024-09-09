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

use crate::errors::{IndexNodeException, LoadShardError};
use crate::update::{update_loop, UpdateParameters};
use crate::RawProtos;
use nucliadb_core::paragraphs::ParagraphIterator;
use nucliadb_core::protos::*;
use nucliadb_core::texts::DocumentIterator;
use nucliadb_node::cache::ShardReaderCache;
use nucliadb_node::lifecycle;
use nucliadb_node::settings::load_settings;
use nucliadb_node::shards::reader::ShardReader;
use prost::Message;
use pyo3::exceptions::{PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::PyErr;
use std::io::Cursor;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

const REFRESH_RATE: Duration = Duration::from_millis(10);

#[pyclass]
pub struct PyParagraphProducer {
    inner: ParagraphIterator,
}
#[pymethods]
impl PyParagraphProducer {
    pub fn next<'p>(&mut self, py: Python<'p>) -> PyResult<&'p PyAny> {
        match self.inner.next() {
            None => Err(PyStopIteration::new_err("Empty iterator")),
            Some(item) => Ok(PyList::new(py, item.encode_to_vec())),
        }
    }
}

#[pyclass]
pub struct PyDocumentProducer {
    inner: DocumentIterator,
}
#[pymethods]
impl PyDocumentProducer {
    pub fn next<'p>(&mut self, py: Python<'p>) -> PyResult<&'p PyAny> {
        match self.inner.next() {
            None => Err(PyStopIteration::new_err("Empty iterator")),
            Some(item) => Ok(PyList::new(py, item.encode_to_vec())),
        }
    }
}

#[pyclass]
pub struct NodeReader {
    #[allow(unused)]
    update_loop_handle: JoinHandle<()>,
    shards: Arc<ShardReaderCache>,
}

impl NodeReader {
    fn obtain_shard(&self, shard_id: String) -> Result<Arc<ShardReader>, PyErr> {
        self.shards
            .get(&shard_id)
            .map_err(|error| LoadShardError::new_err(format!("Error loading shard {}: {}", shard_id, error)))
    }
}

#[pymethods]
impl NodeReader {
    #[new]
    pub fn new() -> Self {
        let settings = load_settings().unwrap();
        lifecycle::initialize_reader(settings.clone());

        let shards = Arc::new(ShardReaderCache::new(settings.clone()));
        let shards_update_loop_copy = Arc::clone(&shards);
        let update_parameters = UpdateParameters {
            shards_path: settings.shards_path(),
            refresh_rate: REFRESH_RATE,
        };
        let update_loop_handle = std::thread::spawn(|| {
            update_loop(update_parameters, shards_update_loop_copy);
        });

        NodeReader {
            shards,
            update_loop_handle,
        }
    }

    pub fn paragraphs(&mut self, shard_id: RawProtos) -> PyResult<PyParagraphProducer> {
        let request = StreamRequest::decode(&mut Cursor::new(shard_id)).expect("Error decoding arguments");
        let Some(shard_id) = request.shard_id.clone() else {
            return Err(PyValueError::new_err("Missing shard_id field"));
        };
        let shard = self.obtain_shard(shard_id.id)?;
        match shard.paragraph_iterator(request) {
            Ok(iterator) => Ok(PyParagraphProducer {
                inner: iterator,
            }),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn documents(&mut self, shard_id: RawProtos) -> PyResult<PyDocumentProducer> {
        let request = StreamRequest::decode(&mut Cursor::new(shard_id)).expect("Error decoding arguments");
        let Some(shard_id) = request.shard_id.clone() else {
            return Err(PyValueError::new_err("Missing shard_id field"));
        };
        let shard = self.obtain_shard(shard_id.id)?;
        match shard.document_iterator(request) {
            Ok(iterator) => Ok(PyDocumentProducer {
                inner: iterator,
            }),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn get_shard<'p>(&mut self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let request = GetShardRequest::decode(&mut Cursor::new(shard_id)).expect("Error decoding arguments");
        let Some(shard_id) = request.shard_id.clone() else {
            return Err(PyValueError::new_err("Missing shard_id field"));
        };
        let shard = self.obtain_shard(shard_id.id)?;
        match shard.get_info() {
            Ok(shard) => Ok(PyList::new(py, shard.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn search<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let search_request = SearchRequest::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard_id = search_request.shard.clone();
        let shard = self.obtain_shard(shard_id)?;
        let response = shard.search(search_request);
        match response {
            Ok(response) => Ok(PyList::new(py, response.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn suggest<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let suggest_request = SuggestRequest::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard_id = suggest_request.shard.clone();
        let shard = self.obtain_shard(shard_id)?;
        let response = shard.suggest(suggest_request);
        match response {
            Ok(response) => Ok(PyList::new(py, response.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn vector_search<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let vector_request = VectorSearchRequest::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard_id = vector_request.id.clone();
        let shard = self.obtain_shard(shard_id)?;
        let response = shard.vector_search(vector_request);
        match response {
            Ok(response) => Ok(PyList::new(py, response.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn document_search<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let document_request =
            DocumentSearchRequest::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard_id = document_request.id.clone();
        let shard = self.obtain_shard(shard_id)?;
        let response = shard.document_search(document_request);
        match response {
            Ok(response) => Ok(PyList::new(py, response.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn paragraph_search<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let paragraph_request =
            ParagraphSearchRequest::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard_id = paragraph_request.id.clone();
        let shard = self.obtain_shard(shard_id)?;
        let response = shard.paragraph_search(paragraph_request);
        match response {
            Ok(response) => Ok(PyList::new(py, response.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn relation_search<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let relation_request =
            RelationSearchRequest::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard_id = relation_request.shard_id.clone();
        let shard = self.obtain_shard(shard_id)?;
        let response = shard.relation_search(relation_request);
        match response {
            Ok(response) => Ok(PyList::new(py, response.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn relation_edges<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shard_id = ShardId::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard = self.obtain_shard(shard_id.id)?;
        let response = shard.get_relations_edges();
        match response {
            Ok(response) => Ok(PyList::new(py, response.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn vector_ids<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let request = VectorSetId::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let VectorSetId {
            shard: Some(ShardId {
                id: shard_id,
            }),
            vectorset: vectorset_id,
        } = request
        else {
            return Err(IndexNodeException::new_err("Shard ID must be provided".to_string()));
        };
        let shard = self.obtain_shard(shard_id)?;
        let response = shard.get_vectors_keys(&vectorset_id);
        match response {
            Ok(response) => Ok(PyList::new(
                py,
                IdCollection {
                    ids: response,
                }
                .encode_to_vec(),
            )),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }
}

impl Default for NodeReader {
    fn default() -> Self {
        Self::new()
    }
}
