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

use prometheus_client::encoding;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use crate::metrics::metric::grpc_ops::{GrpcOpKey, GrpcOpMetric, GrpcOpValue};
use crate::metrics::metric::request_time::{RequestTimeKey, RequestTimeMetric, RequestTimeValue};
use crate::metrics::metric::tokio_runtime::TokioRuntimeObserver;
use crate::metrics::metric::tokio_tasks::TokioTasksObserver;
use crate::metrics::metric::{grpc_ops, replication, request_time, shard_cache, vectors};
use crate::metrics::task_monitor::{Monitor, TaskId};
use crate::tracing::{debug, error};
use crate::NodeResult;

const TIME_BUCKETS: [f64; 12] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 2.5, 5.0, 10.0, 30.0, 60.0];

pub struct PrometheusMeter {
    registry: Registry,
    request_time_metric: RequestTimeMetric,
    grpc_op_metric: GrpcOpMetric,
    tokio_tasks_observer: TokioTasksObserver,
    tokio_runtime_observer: TokioRuntimeObserver,
    replicated_bytes_metric: replication::ReplicatedBytesMetric,
    replication_ops_metric: replication::ReplicationOpsMetric,
    open_shards_metric: shard_cache::OpenShardsMetric,
    evicted_shards_metric: shard_cache::EvictedShardsMetric,

    pub indexing_resource_download_histogram: Histogram,
    pub vectors_metrics: vectors::VectorsMetrics,
}

impl Default for PrometheusMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusMeter {
    pub fn export(&self) -> NodeResult<String> {
        self.tokio_tasks_observer.observe();
        let runtime_observation = self.tokio_runtime_observer.observe();
        if let Err(error) = runtime_observation {
            error!("{error:?}");
        }

        let mut buf = String::new();
        encoding::text::encode(&mut buf, &self.registry)?;
        Ok(buf)
    }

    pub fn record_request_time(&self, metric: RequestTimeKey, value: RequestTimeValue) {
        debug!("{metric:?} : {value:?}");
        self.request_time_metric.get_or_create(&metric).observe(value);
    }

    pub fn record_grpc_op(&self, method: GrpcOpKey, value: GrpcOpValue) {
        self.grpc_op_metric.get_or_create(&method).observe(value);
    }

    pub fn task_monitor(&self, task_id: TaskId) -> Option<Monitor> {
        Some(self.tokio_tasks_observer.get_monitor(task_id))
    }

    pub fn record_replicated_bytes(&self, value: u64) {
        self.replicated_bytes_metric.get_or_create(&replication::ReplicatedBytesKey {}).inc_by(value);
    }
    pub fn record_replication_op(&self, key: replication::ReplicationOpsKey) {
        self.replication_ops_metric.get_or_create(&key).inc();
    }

    pub fn set_shard_cache_gauge(&self, value: i64) {
        self.open_shards_metric.get_or_create(&()).set(value);
    }

    pub fn record_shard_cache_eviction(&self) {
        self.evicted_shards_metric.get_or_create(&()).inc();
    }
}

impl PrometheusMeter {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let request_time_metric = request_time::register_request_time(&mut registry);
        let grpc_op_metric = grpc_ops::register_grpc_ops(&mut registry);
        let replicated_bytes_metric = replication::register_replicated_bytes_ops(&mut registry);
        let replication_ops_metric = replication::register_replication_operations(&mut registry);
        let open_shards_metric = shard_cache::register_open_shards_metric(&mut registry);
        let evicted_shards_metric = shard_cache::register_evicted_shards_metric(&mut registry);

        let vectors_metrics = vectors::VectorsMetrics::new(registry.sub_registry_with_prefix("nucliadb_vectors"));

        let prefixed_subregistry = registry.sub_registry_with_prefix("nucliadb_node");
        let tokio_tasks_observer = TokioTasksObserver::new(prefixed_subregistry);
        let tokio_runtime_observer = TokioRuntimeObserver::new(prefixed_subregistry);

        let indexing_resource_download_histogram = Histogram::new(TIME_BUCKETS.iter().copied());
        prefixed_subregistry.register(
            "indexing_resource_download_seconds",
            "Time to download indexing resources from object storage",
            indexing_resource_download_histogram.clone(),
        );

        Self {
            registry,
            request_time_metric,
            grpc_op_metric,
            tokio_tasks_observer,
            tokio_runtime_observer,
            replicated_bytes_metric,
            replication_ops_metric,
            open_shards_metric,
            evicted_shards_metric,
            vectors_metrics,
            indexing_resource_download_histogram,
        }
    }
}
