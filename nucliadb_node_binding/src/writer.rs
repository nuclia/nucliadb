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

use std::fs;
use std::io::Cursor;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use nucliadb_core::protos::*;
use nucliadb_core::tracing;
use nucliadb_node::shards::metadata::ShardMetadata;
use nucliadb_node::shards::providers::unbounded_cache::UnboundedShardWriterCache;
use nucliadb_node::shards::providers::ShardWriterProvider;
use nucliadb_node::shards::writer::ShardWriter;
use nucliadb_node::{env, lifecycle};
use nucliadb_telemetry::blocking::send_telemetry_event;
use nucliadb_telemetry::payload::TelemetryEvent;
use prost::Message;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::errors::{IndexNodeException, LoadShardError};
use crate::RawProtos;

fn load_merge_interval_secs() -> u64 {
    let val = std::env::var("MERGE_INTERVAL_SECS").ok();
    val.and_then(|value| value.parse().ok()).unwrap_or(10800)
}

fn load_gc_interval_secs() -> u64 {
    let val = std::env::var("GC_INTERVAL_SECS").ok();
    val.and_then(|value| value.parse().ok()).unwrap_or(86400)
}

fn load_update_interval_secs() -> u64 {
    60
}

fn scheduler_frequency_secs() -> u64 {
    5
}

struct Scheduler {
    frequency: Duration,
    update_interval: u64,
    merge_interval: u64,
    gc_interval: u64,
    update_timer: SystemTime,
    merge_timer: SystemTime,
    gc_timer: SystemTime,
    receiver: Receiver<Arc<ShardWriter>>,
    workload: Vec<Arc<ShardWriter>>,
}
impl Scheduler {
    fn writer_is_alive(&mut self) -> bool {
        match self.receiver.try_recv() {
            Err(TryRecvError::Disconnected) => false,
            Err(TryRecvError::Empty) => true,
            Ok(shard) => {
                self.workload.push(shard);
                true
            }
        }
    }

    fn update_workload(&mut self) {
        while let Ok(work) = self.receiver.try_recv() {
            self.workload.push(work);
        }
    }

    fn perform_merge(&self) {
        for work in &self.workload {
            let Err(error) = work.merge() else {
                continue;
            };
            tracing::info!("merge error: {error}");
        }
    }

    fn perform_gc(&self) {
        for work in &self.workload {
            let Err(error) = work.gc() else {
                continue;
            };
            tracing::info!("merge error: {error}");
        }
    }

    fn should_update(&mut self) -> bool {
        let elapsed = self.update_timer.elapsed().unwrap_or_default();
        elapsed.as_secs() >= self.update_interval
    }

    fn should_merge(&mut self) -> bool {
        let elapsed = self.merge_timer.elapsed().unwrap_or_default();
        elapsed.as_secs() >= self.merge_interval
    }

    fn should_gc(&mut self) -> bool {
        let elapsed = self.gc_timer.elapsed().unwrap_or_default();
        elapsed.as_secs() >= self.gc_interval
    }

    pub fn new(receiver: Receiver<Arc<ShardWriter>>) -> Scheduler {
        Scheduler {
            receiver,
            frequency: Duration::from_secs(scheduler_frequency_secs()),
            merge_interval: load_merge_interval_secs(),
            gc_interval: load_gc_interval_secs(),
            update_interval: load_update_interval_secs(),
            update_timer: SystemTime::now(),
            merge_timer: SystemTime::now(),
            gc_timer: SystemTime::now(),
            workload: vec![],
        }
    }
    pub fn run(mut self) {
        loop {
            std::thread::sleep(self.frequency);

            if self.should_update() {
                self.update_workload();
                self.update_timer = SystemTime::now();
            }

            if self.should_merge() {
                self.perform_merge();
                self.merge_timer = SystemTime::now();
            }

            if self.should_gc() {
                self.perform_gc();
                self.gc_timer = SystemTime::now();
            }

            if !self.writer_is_alive() {
                break;
            }
        }
    }
}

#[pyclass]
pub struct NodeWriter {
    scheduler_sender: Sender<Arc<ShardWriter>>,
    shards: UnboundedShardWriterCache,
}

impl NodeWriter {
    fn obtain_shard(&self, shard_id: String) -> Result<Arc<ShardWriter>, PyErr> {
        if let Some(shard) = self.shards.get(shard_id.clone()) {
            return Ok(shard);
        }
        match self.shards.load(shard_id.clone()) {
            Ok(shard) => {
                let scheduler_shard = Arc::clone(&shard);
                let _ = self.scheduler_sender.send(scheduler_shard);
                Ok(shard)
            }
            Err(error) => Err(LoadShardError::new_err(format!(
                "Error loading shard {}: {}",
                shard_id, error
            ))),
        }
    }
}

#[pymethods]
impl NodeWriter {
    #[new]
    pub fn new() -> PyResult<Self> {
        let data_path = env::data_path();
        let shards_path = env::shards_path();
        let (scheduler_sender, scheduler_receiver) = mpsc::channel();
        let scheduler = Scheduler::new(scheduler_receiver);

        lifecycle::initialize_writer(&data_path, &shards_path)
            .map_err(|error| format!("Unable to initialize writer: {error}"))
            .map_err(IndexNodeException::new_err)?;
        std::thread::spawn(|| scheduler.run());

        Ok(Self {
            scheduler_sender,
            shards: UnboundedShardWriterCache::new(shards_path),
        })
    }

    pub fn new_shard<'p>(&self, metadata: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        send_telemetry_event(TelemetryEvent::Create);

        let request =
            NewShardRequest::decode(&mut Cursor::new(metadata)).expect("Error decoding arguments");
        let metadata = ShardMetadata::from(request);
        let new_shard = self.shards.create(metadata);
        match new_shard {
            Ok(new_shard) => Ok(PyList::new(
                py,
                ShardCreated {
                    document_service: new_shard.document_version() as i32,
                    paragraph_service: new_shard.paragraph_version() as i32,
                    vector_service: new_shard.vector_version() as i32,
                    relation_service: new_shard.relation_version() as i32,
                    id: new_shard.id,
                }
                .encode_to_vec(),
            )),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn delete_shard<'p>(&mut self, shard_id: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        send_telemetry_event(TelemetryEvent::Delete);
        let shard_id =
            ShardId::decode(&mut Cursor::new(shard_id)).expect("Error decoding arguments");
        let deleted = self.shards.delete(shard_id.id.clone());
        match deleted {
            Ok(_) => Ok(PyList::new(py, shard_id.encode_to_vec())),
            Err(error) => Err(IndexNodeException::new_err(error.to_string())),
        }
    }

    pub fn clean_and_upgrade_shard<'p>(
        &mut self,
        shard_id: RawProtos,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let shard_id =
            ShardId::decode(&mut Cursor::new(shard_id)).expect("Error decoding arguments");
        let upgraded = self.shards.upgrade(shard_id.id);
        match upgraded {
            Ok(upgrade_details) => Ok(PyList::new(py, upgrade_details.encode_to_vec())),
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
                    shard_ids.push(ShardId { id: id.unwrap() });
                }
            }
        }
        Ok(PyList::new(
            py,
            (ShardIds { ids: shard_ids }).encode_to_vec(),
        ))
    }

    pub fn set_resource<'p>(&mut self, resource: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let resource =
            Resource::decode(&mut Cursor::new(resource)).expect("Error decoding arguments");
        let shard_id = resource.shard_id.clone();
        let shard = self.obtain_shard(shard_id.clone())?;
        let status = shard
            .set_resource(&resource)
            .and_then(|()| shard.get_opstatus());

        match status {
            Ok(mut status) => {
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Err(error) => {
                let status = OpStatus {
                    status: op_status::Status::Error as i32,
                    detail: error.to_string(),
                    field_count: 0_u64,
                    shard_id,
                    ..Default::default()
                };
                Ok(PyList::new(py, status.encode_to_vec()))
            }
        }
    }

    pub fn remove_resource<'p>(
        &mut self,
        resource: RawProtos,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let resource =
            ResourceId::decode(&mut Cursor::new(resource)).expect("Error decoding arguments");
        let shard_id = resource.shard_id.clone();
        let shard = self.obtain_shard(shard_id.clone())?;
        let status = shard
            .remove_resource(&resource)
            .and_then(|()| shard.get_opstatus());
        match status {
            Ok(mut status) => {
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Err(error) => {
                let status = OpStatus {
                    status: op_status::Status::Error as i32,
                    detail: error.to_string(),
                    field_count: 0_u64,
                    shard_id,
                    ..Default::default()
                };
                Ok(PyList::new(py, status.encode_to_vec()))
            }
        }
    }

    pub fn join_graph<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let request =
            SetGraph::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let Some(shard_id) = request.shard_id else {
            return Err(PyValueError::new_err("Missing shard_id field"));
        };
        let Some(graph) = request.graph else {
            return Err(PyValueError::new_err("Missing graph field"));
        };
        let shard = self.obtain_shard(shard_id.id.clone())?;
        let status = shard
            .join_relations_graph(&graph)
            .and_then(|()| shard.get_opstatus());
        match status {
            Ok(mut status) => {
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Err(error) => {
                let status = OpStatus {
                    status: op_status::Status::Error as i32,
                    detail: error.to_string(),
                    field_count: 0_u64,
                    shard_id: shard_id.id,
                    ..Default::default()
                };
                Ok(PyList::new(py, status.encode_to_vec()))
            }
        }
    }

    pub fn delete_relation_nodes<'p>(
        &mut self,
        request: RawProtos,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let request =
            DeleteGraphNodes::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let Some(ref shard_id) = request.shard_id else {
            return Err(PyValueError::new_err("Missing shard_id field"));
        };
        let shard = self.obtain_shard(shard_id.id.clone())?;
        let status = shard
            .delete_relation_nodes(&request)
            .and_then(|()| shard.get_opstatus());
        match status {
            Ok(mut status) => {
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Err(error) => {
                let status = OpStatus {
                    status: op_status::Status::Error as i32,
                    detail: error.to_string(),
                    field_count: 0_u64,
                    shard_id: shard_id.id.clone(),
                    ..Default::default()
                };
                Ok(PyList::new(py, status.encode_to_vec()))
            }
        }
    }

    // TODO: rename to list_vectorsets
    pub fn get_vectorset<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let shard_id =
            ShardId::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard = self.obtain_shard(shard_id.id.clone())?;
        let vector_sets = shard.list_vectorsets();
        match vector_sets {
            Ok(vector_sets) => {
                let response = VectorSetList {
                    shard: Some(shard_id),
                    vectorset: vector_sets,
                };
                Ok(PyList::new(py, response.encode_to_vec()))
            }
            Err(error) => {
                let message = format!("Error listing vectorsets: {}", error);
                Err(IndexNodeException::new_err(message))
            }
        }
    }

    // TODO
    pub fn set_vectorset<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let request = NewVectorSetRequest::decode(&mut Cursor::new(request))
            .expect("Error decoding arguments");
        let Some(ref vectorset_id) = request.id else {
            return Err(PyValueError::new_err("Missing vectorset id field"));
        };
        let Some(ref shard_id) = vectorset_id.shard else {
            return Err(PyValueError::new_err("Missing shard id field"));
        };
        let shard = self.obtain_shard(shard_id.id.clone())?;
        let status = shard
            .add_vectorset(vectorset_id, request.similarity())
            .and_then(|()| shard.get_opstatus());
        match status {
            Ok(mut status) => {
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Err(error) => {
                let op_status = OpStatus {
                    status: op_status::Status::Error as i32,
                    detail: error.to_string(),
                    field_count: 0_u64,
                    shard_id: shard_id.id.clone(),
                    ..Default::default()
                };
                Ok(PyList::new(py, op_status.encode_to_vec()))
            }
        }
    }

    // TODO
    pub fn del_vectorset<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        let vectorset =
            VectorSetId::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let Some(ref shard_id) = vectorset.shard else {
            return Err(PyValueError::new_err("Missing shard id field"));
        };
        let shard = self.obtain_shard(shard_id.id.clone())?;
        let status = shard
            .remove_vectorset(&vectorset)
            .and_then(|()| shard.get_opstatus());
        match status {
            Ok(mut status) => {
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(PyList::new(py, status.encode_to_vec()))
            }
            Err(error) => {
                let op_status = OpStatus {
                    status: op_status::Status::Error as i32,
                    detail: error.to_string(),
                    field_count: 0_u64,
                    shard_id: shard_id.id.clone(),
                    ..Default::default()
                };
                Ok(PyList::new(py, op_status.encode_to_vec()))
            }
        }
    }

    pub fn gc<'p>(&mut self, request: RawProtos, py: Python<'p>) -> PyResult<&'p PyAny> {
        send_telemetry_event(TelemetryEvent::GarbageCollect);
        let shard_id =
            ShardId::decode(&mut Cursor::new(request)).expect("Error decoding arguments");
        let shard = self.obtain_shard(shard_id.id)?;
        let result = shard.gc();
        match result {
            Ok(()) => {
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
