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

use std::sync::{Mutex, TryLockError};

use prometheus_client::encoding;
use prometheus_client::registry::Registry;
use tokio_metrics::{RuntimeIntervals, RuntimeMonitor};

use crate::metrics::meters::Meter;
use crate::metrics::metric::request_time::{RequestTimeKey, RequestTimeMetric, RequestTimeValue};
use crate::metrics::metric::tokio_runtime::TokioRuntimeMetrics;
use crate::metrics::metric::tokio_tasks::{TaskLabels, TokioTaskMetrics};
use crate::metrics::metric::{request_time, tokio_runtime, tokio_tasks};
use crate::metrics::task_monitor::{Monitor, MultiTaskMonitor, TaskId};
use crate::tracing::error;
use crate::{node_error, NodeResult};

pub struct PrometheusMeter {
    registry: Registry,
    request_time_metric: RequestTimeMetric,
    tokio_runtime_metrics: TokioRuntimeMetrics,
    tokio_task_metrics: TokioTaskMetrics,
    tasks_monitor: MultiTaskMonitor,

    // We need a Mutex here because Meters are Send and we need a mutable
    // RuntimeIntervals. We need to store the RuntimeIntervals iterator instead
    // of RuntimeMontior because metrics are cumulative in it. Creating a
    // RuntimeIntervals object per export does not get metrics correctly
    runtime_intervals: Mutex<RuntimeIntervals>,
}

impl Default for PrometheusMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl Meter for PrometheusMeter {
    fn export(&self) -> NodeResult<String> {
        self.tasks_monitor
            .export_all()
            .for_each(|(task_id, metrics)| {
                let labels = TaskLabels { request: task_id };
                self.tokio_task_metrics.collect(labels, metrics.to_owned());
            });

        if let Err(error) = self.collect_runtime_metrics() {
            error!("{error:?}");
        }

        let mut buf = String::new();
        encoding::text::encode(&mut buf, &self.registry)?;
        Ok(buf)
    }

    fn record_request_time(&self, metric: RequestTimeKey, value: RequestTimeValue) {
        self.request_time_metric
            .get_or_create(&metric)
            .observe(value);
    }

    fn task_monitor(&self, task_id: TaskId) -> Option<Monitor> {
        Some(self.tasks_monitor.task_monitor(task_id))
    }
}

impl PrometheusMeter {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        // This must be done for every metric
        let request_time_metric = request_time::register_request_time(&mut registry);

        let prefixed_subregistry = registry.sub_registry_with_prefix("nucliadb_node");
        let tokio_runtime_metrics =
            tokio_runtime::register_tokio_runtime_metrics(prefixed_subregistry);
        let tokio_task_metrics = tokio_tasks::register_tokio_task_metrics(prefixed_subregistry);

        let tasks_monitor = MultiTaskMonitor::new();
        let runtime_monitor = RuntimeMonitor::new(&tokio::runtime::Handle::current());
        let runtime_intervals = Mutex::new(runtime_monitor.intervals());

        Self {
            registry,
            request_time_metric,
            tokio_runtime_metrics,
            tokio_task_metrics,
            tasks_monitor,
            runtime_intervals,
        }
    }

    fn collect_runtime_metrics(&self) -> NodeResult<()> {
        let mut intervals = match self.runtime_intervals.try_lock() {
            Ok(intervals) => intervals,
            Err(TryLockError::Poisoned(inner)) => {
                // This should never happen, as only one thread should be
                // calling this. Still, if it happens, we recover and continue
                // exporting metrics
                inner.into_inner()
            }
            Err(TryLockError::WouldBlock) => {
                return Err(node_error!(
                    "Cannot acquire runtime metrics lock. There's a concurrent export going on?"
                ));
            }
        };

        if let Some(metrics) = intervals.next() {
            self.tokio_runtime_metrics.collect(metrics);
        } else {
            return Err(node_error!(
                "Cannot export tokio runtime metrics, iterator didn't return values"
            ));
        }

        Ok(())
    }
}
