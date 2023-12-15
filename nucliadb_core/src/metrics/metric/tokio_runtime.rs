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

use std::sync::{Mutex, MutexGuard, TryLockError};

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use tokio;
use tokio::runtime::Handle;
use tokio_metrics::{RuntimeIntervals, RuntimeMonitor};

use crate::tracing::info;
use crate::{node_error, NodeResult};

pub struct TokioRuntimeObserver {
    runtime: Option<Handle>,
    intervals: Option<Mutex<RuntimeIntervals>>,
    metrics: TokioRuntimeMetrics,
}

impl TokioRuntimeObserver {
    pub fn new(registry: &mut Registry) -> Self {
        if let Ok(runtime) = Handle::try_current() {
            let monitor = RuntimeMonitor::new(&runtime);
            // We need to store RuntimeIntervals iterator instead of RuntimeMonitor to
            // get incremental values. We need a Mutex to be Send and Sync (for our
            // Meter users)
            let intervals = Mutex::new(monitor.intervals());
            Self {
                runtime: Some(runtime),
                intervals: Some(intervals),
                metrics: TokioRuntimeMetrics::new(registry),
            }
        } else {
            info!("Cannot export tokio runtime metrics, no runtime available");
            Self {
                runtime: None,
                intervals: None,
                metrics: TokioRuntimeMetrics::new(registry),
            }
        }
    }

    pub fn observe(&self) -> NodeResult<()> {
        if let Some(runtime) = self.runtime.as_ref() {
            let interval = self.next_interval()?;
            let raw_metrics = runtime.metrics();

            self.metrics.update(raw_metrics, interval);
        }

        Ok(())
    }

    fn next_interval(&self) -> NodeResult<tokio_metrics::RuntimeMetrics> {
        let mut intervals = self.unpoisoned_intervals()?;
        match intervals.next() {
            Some(metrics) => Ok(metrics),
            None => Err(node_error!(
                "Cannot export tokio runtime metrics, iterator didn't return values"
            )),
        }
    }

    fn unpoisoned_intervals(&self) -> NodeResult<MutexGuard<'_, RuntimeIntervals>> {
        if let Some(intervals) = self.intervals.as_ref() {
            match intervals.try_lock() {
                Ok(intervals) => Ok(intervals),
                Err(TryLockError::Poisoned(inner)) => Ok(inner.into_inner()),
                Err(TryLockError::WouldBlock) => Err(node_error!(
                    "Cannot acquire runtime metrics lock. There's a concurrent export going on?"
                )),
            }
        } else {
            Err(node_error!(
                "Cannot export tokio runtime metrics, no runtime available"
            ))
        }
    }
}

pub struct TokioRuntimeMetrics {
    // From tokio_metrics::RuntimeMetrics
    workers_count: Gauge,
    total_park_count: Counter,
    max_park_count: Gauge,
    min_park_count: Gauge,
    mean_poll_duration: Histogram,
    mean_poll_duration_worker_min: Histogram,
    mean_poll_duration_worker_max: Histogram,
    total_noop_count: Counter,
    max_noop_count: Gauge,
    min_noop_count: Gauge,
    total_steal_count: Counter,
    max_steal_count: Gauge,
    min_steal_count: Gauge,
    total_steal_operations: Counter,
    max_steal_operations: Gauge,
    min_steal_operations: Gauge,
    num_remote_schedules: Gauge,
    total_local_schedule_count: Counter,
    max_local_schedule_count: Gauge,
    min_local_schedule_count: Gauge,
    total_overflow_count: Counter,
    max_overflow_count: Gauge,
    min_overflow_count: Gauge,
    total_polls_count: Counter,
    max_polls_count: Gauge,
    min_polls_count: Gauge,
    total_busy_duration: Histogram,
    max_busy_duration: Histogram,
    min_busy_duration: Histogram,
    injection_queue_depth: Gauge,
    total_local_queue_depth: Counter,
    max_local_queue_depth: Gauge,
    min_local_queue_depth: Gauge,
    elapsed: Histogram,
    budget_forced_yield_count: Gauge,
    io_driver_ready_count: Gauge,

    // From tokio::runtime::RuntimeMetrics
    blocking_threads_count: Gauge,
    idle_blocking_threads_count: Gauge,
    active_tasks_count: Gauge,
    blocking_queue_depth: Gauge,

    // Derived metrics
    busy_ratio: Gauge,
    mean_polls_per_park: Gauge,
}

// TODO we are trying bucket values for everything. After an evaluation on
// production, we should reconsider changing them and customize for every
// Histogram metric
const BUCKETS: [f64; 15] = [
    0.000010, 0.000025, 0.000050, 0.000100, 0.000250, 0.000500, 0.001, 0.002, 0.005, 0.010, 0.100,
    0.250, 0.500, 1.0, 5.0,
];

impl TokioRuntimeMetrics {
    fn new(registry: &mut Registry) -> Self {
        let workers_count = Gauge::default();
        registry.register(
            "workers_count",
            "The number of worker threads used by the runtime. This metric is static for a runtime",
            workers_count.clone(),
        );

        let total_park_count = Counter::default();
        registry.register(
            "total_park_count",
            "The number of times worker threads parked. [...]",
            total_park_count.clone(),
        );

        let max_park_count = Gauge::default();
        registry.register(
            "max_park_count",
            "The maximum number of times any worker thread parked",
            max_park_count.clone(),
        );

        let min_park_count = Gauge::default();
        registry.register(
            "min_park_count",
            "The minimum number of times any worker thread parked",
            min_park_count.clone(),
        );

        let mean_poll_duration = Histogram::new(BUCKETS.iter().copied());
        registry.register(
            "mean_poll_duration",
            "The average duration of a single invocation of poll on a task. [...]",
            mean_poll_duration.clone(),
        );

        let mean_poll_duration_worker_min = Histogram::new(BUCKETS.iter().copied());
        registry.register(
            "mean_poll_duration_worker_min",
            "The average duration of a single invocation of poll on a task oin the worker with \
             the lowest volume. [...]",
            mean_poll_duration_worker_min.clone(),
        );

        let mean_poll_duration_worker_max = Histogram::new(BUCKETS.iter().copied());
        registry.register(
            "mean_poll_duration_worker_max",
            "The average duration of a single invocation of poll on a task on the worker with the \
             highest value. [...]",
            mean_poll_duration_worker_max.clone(),
        );

        // This metric must be explicitly enabled (and we don't)
        // let poll_count_histogram = Histogram::new(BUCKETS.iter().copied());
        // registry.register(
        //     "poll_count_histogram",
        //     "A histogram of task polls since the previous probe grouped by poll times. [...]",
        //     poll_count_histogram.clone(),
        // );

        let total_noop_count = Counter::default();
        registry.register(
            "total_noop_count",
            "The number of times worker threads unparked but performed no work before parking \
             again. [...]",
            total_noop_count.clone(),
        );

        let max_noop_count = Gauge::default();
        registry.register(
            "max_noop_count",
            "The maximum number of times any worker thread unparked but performed no work before \
             parking again",
            max_noop_count.clone(),
        );

        let min_noop_count = Gauge::default();
        registry.register(
            "min_noop_count",
            "The minimum number of times any worker thread unparked but performed no work before \
             parking again",
            min_noop_count.clone(),
        );

        let total_steal_count = Counter::default();
        registry.register(
            "total_steal_count",
            "The number of tasks worker threads stole from another worker thread. [...]",
            total_steal_count.clone(),
        );

        let max_steal_count = Gauge::default();
        registry.register(
            "max_steal_count",
            "The maximum number of tasks any worker thread stole from another worker thread",
            max_steal_count.clone(),
        );

        let min_steal_count = Gauge::default();
        registry.register(
            "min_steal_count",
            "The minimum number of tasks any worker thread stole from another worker thread",
            min_steal_count.clone(),
        );

        let total_steal_operations = Counter::default();
        registry.register(
            "total_steal_operations",
            "The number of times worker threads stole tasks from another worker thread. [...]",
            total_steal_operations.clone(),
        );

        let max_steal_operations = Gauge::default();
        registry.register(
            "max_steal_operations",
            "The maximum number of times worker any thread stole tasks from another worker thread",
            max_steal_operations.clone(),
        );

        let min_steal_operations = Gauge::default();
        registry.register(
            "min_steal_operations",
            "The minimum number of times worker any thread stole tasks from another worker thread",
            min_steal_operations.clone(),
        );

        let num_remote_schedules = Gauge::default();
        registry.register(
            "num_remote_schedules",
            "The number of tasks scheduled from outside the runtime. [...]",
            num_remote_schedules.clone(),
        );

        let total_local_schedule_count = Counter::default();
        registry.register(
            "total_local_schedule_count",
            "The number of tasks scheduled from worker threads. [...]",
            total_local_schedule_count.clone(),
        );

        let max_local_schedule_count = Gauge::default();
        registry.register(
            "max_local_schedule_count",
            "The maximum number of tasks scheduled from any one worker thread",
            max_local_schedule_count.clone(),
        );

        let min_local_schedule_count = Gauge::default();
        registry.register(
            "min_local_schedule_count",
            "The minimum number of tasks scheduled from any one worker thread",
            min_local_schedule_count.clone(),
        );

        let total_overflow_count = Counter::default();
        registry.register(
            "total_overflow_count",
            "The number of times worker threads staurated their local queues. [...]",
            total_overflow_count.clone(),
        );

        let max_overflow_count = Gauge::default();
        registry.register(
            "max_overflow_count",
            "The maximum number of times any one worker saturated its local queue",
            max_overflow_count.clone(),
        );

        let min_overflow_count = Gauge::default();
        registry.register(
            "min_overflow_count",
            "The minimum number of times any on worker saturated its local queue",
            min_overflow_count.clone(),
        );

        let total_polls_count = Counter::default();
        registry.register(
            "total_polls_count",
            "The number of tasks that have been polled across all worker threads. [...]",
            total_polls_count.clone(),
        );

        let max_polls_count = Gauge::default();
        registry.register(
            "max_polls_count",
            "The maximum number of tasks that have been polled in any worker thread",
            max_polls_count.clone(),
        );

        let min_polls_count = Gauge::default();
        registry.register(
            "min_polls_count",
            "The minimum number of tasks that have been polled in any worker thread",
            min_polls_count.clone(),
        );

        let total_busy_duration = Histogram::new(BUCKETS.iter().copied());
        registry.register(
            "total_busy_duration",
            "The amount of time worker threads were busy. [...]",
            total_busy_duration.clone(),
        );

        let max_busy_duration = Histogram::new(BUCKETS.iter().copied());
        registry.register(
            "max_busy_duration",
            "The maximum amount of time a worker thread was busy",
            max_busy_duration.clone(),
        );

        let min_busy_duration = Histogram::new(BUCKETS.iter().copied());
        registry.register(
            "min_busy_duration",
            "The minimum amount of time a worker thread was busy",
            min_busy_duration.clone(),
        );

        let injection_queue_depth = Gauge::default();
        registry.register(
            "injection_queue_depth",
            "The number of tasks currently scheduled in the runtime's injection queue. [...]",
            injection_queue_depth.clone(),
        );

        let total_local_queue_depth = Counter::default();
        registry.register(
            "total_local_queue_depth",
            "The total number of tasks currently scheduled in workers' local queues. [...]",
            total_local_queue_depth.clone(),
        );

        let max_local_queue_depth = Gauge::default();
        registry.register(
            "max_local_queue_depth",
            "The maximum number of tasks currently scheduled any worker's local queue",
            max_local_queue_depth.clone(),
        );

        let min_local_queue_depth = Gauge::default();
        registry.register(
            "min_local_queue_depth",
            "The minimum number of tasks currenly scheduled any worker's local queue",
            min_local_queue_depth.clone(),
        );

        let elapsed = Histogram::new(BUCKETS.iter().copied());
        registry.register(
            "elapsed",
            "Total amount of time elapsed since observing runtime metrics",
            elapsed.clone(),
        );

        let budget_forced_yield_count = Gauge::default();
        registry.register(
            "budget_forced_yied_count",
            "Returns the number of times that tasks have been forced to yield back to the \
             scheduled after exhausting their task budgets. [...]",
            budget_forced_yield_count.clone(),
        );

        let io_driver_ready_count = Gauge::default();
        registry.register(
            "io_driver_ready_count",
            "Returns the number of ready events processed by runtime's I/O driver",
            io_driver_ready_count.clone(),
        );

        let blocking_threads_count = Gauge::default();
        registry.register(
            "blocking_threads_count",
            "Number of additional threads spawned by the runtime (using spawn_blocking)",
            blocking_threads_count.clone(),
        );

        let idle_blocking_threads_count = Gauge::default();
        registry.register(
            "idle_blocking_threads_count",
            "Number of idle threads spawned by the runtime (using spawn_blocking)",
            idle_blocking_threads_count.clone(),
        );

        let active_tasks_count = Gauge::default();
        registry.register(
            "active_tasks_count",
            "Number of active tasks in the runtime",
            active_tasks_count.clone(),
        );

        let blocking_queue_depth = Gauge::default();
        registry.register(
            "blocking_queue_depth",
            "number of tasks currently scheduled in the blocking thread pool, spawned using \
             spawn_blocking",
            blocking_queue_depth.clone(),
        );

        let busy_ratio = Gauge::default();
        registry.register(
            "busy_ratio",
            "Busy ratio during scrapes (busy duration / elapsed)",
            busy_ratio.clone(),
        );

        let mean_polls_per_park = Gauge::default();
        registry.register(
            "mean_polls_per_park",
            "Mean polls done per useful park (park - noop). 0 means either no polls or no parks",
            mean_polls_per_park.clone(),
        );

        Self {
            workers_count,
            total_park_count,
            max_park_count,
            min_park_count,
            mean_poll_duration,
            mean_poll_duration_worker_min,
            mean_poll_duration_worker_max,
            total_noop_count,
            max_noop_count,
            min_noop_count,
            total_steal_count,
            max_steal_count,
            min_steal_count,
            total_steal_operations,
            max_steal_operations,
            min_steal_operations,
            num_remote_schedules,
            total_local_schedule_count,
            max_local_schedule_count,
            min_local_schedule_count,
            total_overflow_count,
            max_overflow_count,
            min_overflow_count,
            total_polls_count,
            max_polls_count,
            min_polls_count,
            total_busy_duration,
            max_busy_duration,
            min_busy_duration,
            injection_queue_depth,
            total_local_queue_depth,
            max_local_queue_depth,
            min_local_queue_depth,
            elapsed,
            budget_forced_yield_count,
            io_driver_ready_count,
            blocking_threads_count,
            idle_blocking_threads_count,
            active_tasks_count,
            blocking_queue_depth,
            busy_ratio,
            mean_polls_per_park,
        }
    }

    fn update(
        &self,
        raw_metrics: tokio::runtime::RuntimeMetrics,
        metrics: tokio_metrics::RuntimeMetrics,
    ) {
        self.workers_count.set(metrics.workers_count as i64);
        self.total_park_count.inc_by(metrics.total_park_count);
        self.max_park_count.set(metrics.total_park_count as i64);
        self.min_park_count.set(metrics.min_park_count as i64);
        self.mean_poll_duration
            .observe(metrics.mean_poll_duration.as_secs_f64());
        self.mean_poll_duration_worker_min
            .observe(metrics.mean_poll_duration_worker_min.as_secs_f64());
        self.mean_poll_duration_worker_max
            .observe(metrics.mean_poll_duration_worker_max.as_secs_f64());
        self.total_noop_count.inc_by(metrics.total_noop_count);
        self.max_noop_count.set(metrics.max_noop_count as i64);
        self.min_noop_count.set(metrics.min_noop_count as i64);
        self.total_steal_count.inc_by(metrics.total_steal_count);
        self.max_steal_count.set(metrics.max_steal_count as i64);
        self.min_steal_count.set(metrics.min_steal_count as i64);
        self.total_steal_operations
            .inc_by(metrics.total_steal_operations);
        self.max_steal_operations
            .set(metrics.max_steal_operations as i64);
        self.min_steal_operations
            .set(metrics.min_steal_operations as i64);
        self.num_remote_schedules
            .set(metrics.num_remote_schedules as i64);
        self.total_local_schedule_count
            .inc_by(metrics.total_local_schedule_count);
        self.max_local_schedule_count
            .set(metrics.max_local_schedule_count as i64);
        self.min_local_schedule_count
            .set(metrics.min_local_schedule_count as i64);
        self.total_overflow_count
            .inc_by(metrics.total_overflow_count);
        self.max_overflow_count
            .set(metrics.max_overflow_count as i64);
        self.min_overflow_count
            .set(metrics.min_overflow_count as i64);
        self.total_polls_count.inc_by(metrics.total_polls_count);
        self.max_polls_count.set(metrics.max_polls_count as i64);
        self.min_polls_count.set(metrics.min_polls_count as i64);
        self.total_busy_duration
            .observe(metrics.total_busy_duration.as_secs_f64());
        self.max_busy_duration
            .observe(metrics.max_busy_duration.as_secs_f64());
        self.min_busy_duration
            .observe(metrics.min_busy_duration.as_secs_f64());
        self.injection_queue_depth
            .set(metrics.injection_queue_depth as i64);
        self.total_local_queue_depth
            .inc_by(metrics.total_local_queue_depth as u64);
        self.max_local_queue_depth
            .set(metrics.max_local_queue_depth as i64);
        self.min_local_queue_depth
            .set(metrics.min_local_queue_depth as i64);
        self.elapsed.observe(metrics.elapsed.as_secs_f64());
        self.budget_forced_yield_count
            .set(metrics.budget_forced_yield_count as i64);
        self.io_driver_ready_count
            .set(metrics.io_driver_ready_count as i64);

        self.blocking_threads_count
            .set(raw_metrics.num_blocking_threads() as i64);
        self.idle_blocking_threads_count
            .set(raw_metrics.num_idle_blocking_threads() as i64);
        self.active_tasks_count
            .set(raw_metrics.active_tasks_count() as i64);
        self.blocking_queue_depth
            .set(raw_metrics.blocking_queue_depth() as i64);

        // Gauges are signed integers but our ratio is a float. We multiply by
        // 10^3 to get 3 decimals and lose less precision (it should be divided
        // by 1000 on its dashboard)
        self.busy_ratio.set((metrics.busy_ratio() * 1000.0) as i64);
        self.mean_polls_per_park
            .set((metrics.mean_polls_per_park() * 1000.0) as i64);
    }
}
