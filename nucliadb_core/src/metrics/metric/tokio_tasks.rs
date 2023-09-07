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

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use tokio_metrics::TaskMetrics;

pub struct TokioTaskMetrics {
    instrumented_count: Family<TaskLabels, Counter>,
    dropped_count: Family<TaskLabels, Counter>,
    first_poll_count: Family<TaskLabels, Counter>,
    total_first_poll_delay: Family<TaskLabels, Histogram>,
    total_idled_count: Family<TaskLabels, Counter>,
    total_idle_duration: Family<TaskLabels, Histogram>,
    total_scheduled_count: Family<TaskLabels, Counter>,
    total_scheduled_duration: Family<TaskLabels, Histogram>,
    total_poll_count: Family<TaskLabels, Counter>,
    total_poll_duration: Family<TaskLabels, Histogram>,
    total_fast_poll_count: Family<TaskLabels, Counter>,
    total_fast_poll_duration: Family<TaskLabels, Histogram>,
    total_slow_poll_count: Family<TaskLabels, Counter>,
    total_slow_poll_duration: Family<TaskLabels, Histogram>,
    total_short_delay_count: Family<TaskLabels, Counter>,
    total_long_delay_count: Family<TaskLabels, Counter>,
    total_short_delay_duration: Family<TaskLabels, Histogram>,
    total_long_delay_duration: Family<TaskLabels, Histogram>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TaskLabels {
    pub request: String, // gRPC method (/NewShard, /SetResource...)
}

// Tasks in tokio should never block longer than 10-100µs

// TODO we are trying bucket values for everything. After an evaluation on
// production, we should reconsider changing them and customize for every
// Histogram metric
const BUCKETS: [f64; 15] = [
    0.000010, 0.000025, 0.000050, 0.000100, 0.000250, 0.000500, 0.001, 0.002, 0.005, 0.010, 0.100,
    0.250, 0.500, 1.0, 5.0,
];

impl TokioTaskMetrics {
    fn new(registry: &mut Registry) -> Self {
        let histogram_constructor = || Histogram::new(BUCKETS.iter().copied());

        let instrumented_count = Family::default();
        registry.register(
            "instrumented_count",
            "The number of tasks instrumented",
            instrumented_count.clone(),
        );

        let dropped_count = Family::default();
        registry.register(
            "dropped_count",
            "The number of tasks dropped",
            dropped_count.clone(),
        );

        let first_poll_count = Family::default();
        registry.register(
            "first_poll_count",
            "The number of tasks polled for the first time",
            first_poll_count.clone(),
        );

        let total_first_poll_delay =
            Family::<TaskLabels, Histogram>::new_with_constructor(histogram_constructor);
        registry.register(
            "total_first_poll_delay",
            "The total duration elapsed between the instant tasks are instrumented, and the \
             instant they are first polled",
            total_first_poll_delay.clone(),
        );

        let total_idled_count = Family::default();
        registry.register(
            "total_idled_count",
            "The total number of times that tasks idled, waiting to be awoken. [...]",
            total_idled_count.clone(),
        );

        let total_idle_duration =
            Family::<TaskLabels, Histogram>::new_with_constructor(histogram_constructor);
        registry.register(
            "total_idle_duration",
            "The total duration that tasks idled. [...]",
            total_idle_duration.clone(),
        );

        let total_scheduled_count = Family::default();
        registry.register(
            "total_scheduled_count",
            "The total number of times that tasks were awoken (and then, presumably, scheduled \
             for execution)",
            total_scheduled_count.clone(),
        );

        let total_scheduled_duration =
            Family::<TaskLabels, Histogram>::new_with_constructor(histogram_constructor);
        registry.register(
            "total_scheduled_duration",
            "The total duration that tasks spent waiting to be polled after awakening",
            total_scheduled_duration.clone(),
        );

        let total_poll_count = Family::default();
        registry.register(
            "total_poll_count",
            "The total number of times that tasks where polled",
            total_poll_count.clone(),
        );

        let total_poll_duration =
            Family::<TaskLabels, Histogram>::new_with_constructor(histogram_constructor);
        registry.register(
            "total_poll_duration",
            "The total duration elapsed during polls",
            total_poll_duration.clone(),
        );

        let total_fast_poll_count = Family::default();
        registry.register(
            "total_fast_poll_count",
            "The total number of times that polling tasks completed swiftly. [...]",
            total_fast_poll_count.clone(),
        );

        let total_fast_poll_duration =
            Family::<TaskLabels, Histogram>::new_with_constructor(histogram_constructor);
        registry.register(
            "total_fast_poll_duration",
            "The total duration of fast polls. [...]",
            total_fast_poll_duration.clone(),
        );

        let total_slow_poll_count = Family::default();
        registry.register(
            "total_slow_poll_count",
            "The total number of times that polling tasks completed slowly. [...]",
            total_slow_poll_count.clone(),
        );

        let total_slow_poll_duration =
            Family::<TaskLabels, Histogram>::new_with_constructor(histogram_constructor);
        registry.register(
            "total_slow_poll_duration",
            "The total duration of slow polls. [...]",
            total_slow_poll_duration.clone(),
        );

        let total_short_delay_count = Family::default();
        registry.register(
            "total_short_delay_count",
            "The total count of tasks with short scheduling delays. [...]",
            total_short_delay_count.clone(),
        );

        let total_long_delay_count = Family::default();
        registry.register(
            "total_long_delay_count",
            "The total count of tasks with long scheduling delays. [...]",
            total_long_delay_count.clone(),
        );

        let total_short_delay_duration =
            Family::<TaskLabels, Histogram>::new_with_constructor(histogram_constructor);
        registry.register(
            "total_short_delay_duration",
            "The total duration of tasks with short scheduling delays. [...]",
            total_short_delay_duration.clone(),
        );

        let total_long_delay_duration =
            Family::<TaskLabels, Histogram>::new_with_constructor(histogram_constructor);
        registry.register(
            "total_long_delay_duration",
            "The total duration of tasks with long scheduling delays. [...]",
            total_long_delay_duration.clone(),
        );

        Self {
            instrumented_count,
            dropped_count,
            first_poll_count,
            total_first_poll_delay,
            total_idled_count,
            total_idle_duration,
            total_scheduled_count,
            total_scheduled_duration,
            total_poll_count,
            total_poll_duration,
            total_fast_poll_count,
            total_fast_poll_duration,
            total_slow_poll_count,
            total_slow_poll_duration,
            total_short_delay_count,
            total_long_delay_count,
            total_short_delay_duration,
            total_long_delay_duration,
        }
    }

    /// Collected metrics must belong to the interval from last `collect` call
    /// until the current call.
    pub fn collect(&self, labels: TaskLabels, metrics: TaskMetrics) {
        // TODO? Should we add a declarative macro to improve this?

        self.instrumented_count
            .get_or_create(&labels)
            .inc_by(metrics.instrumented_count);

        self.dropped_count
            .get_or_create(&labels)
            .inc_by(metrics.dropped_count);

        self.first_poll_count
            .get_or_create(&labels)
            .inc_by(metrics.first_poll_count);

        self.total_first_poll_delay
            .get_or_create(&labels)
            .observe(metrics.total_first_poll_delay.as_secs_f64());

        self.total_idled_count
            .get_or_create(&labels)
            .inc_by(metrics.total_idled_count);

        self.total_idle_duration
            .get_or_create(&labels)
            .observe(metrics.total_idle_duration.as_secs_f64());

        self.total_scheduled_count
            .get_or_create(&labels)
            .inc_by(metrics.total_scheduled_count);

        self.total_scheduled_duration
            .get_or_create(&labels)
            .observe(metrics.total_scheduled_duration.as_secs_f64());

        self.total_poll_count
            .get_or_create(&labels)
            .inc_by(metrics.total_poll_count);

        self.total_poll_duration
            .get_or_create(&labels)
            .observe(metrics.total_poll_duration.as_secs_f64());

        self.total_fast_poll_count
            .get_or_create(&labels)
            .inc_by(metrics.total_fast_poll_count);

        self.total_fast_poll_duration
            .get_or_create(&labels)
            .observe(metrics.total_fast_poll_duration.as_secs_f64());

        self.total_slow_poll_count
            .get_or_create(&labels)
            .inc_by(metrics.total_slow_poll_count);

        self.total_slow_poll_duration
            .get_or_create(&labels)
            .observe(metrics.total_slow_poll_duration.as_secs_f64());

        self.total_short_delay_count
            .get_or_create(&labels)
            .inc_by(metrics.total_short_delay_count);

        self.total_long_delay_count
            .get_or_create(&labels)
            .inc_by(metrics.total_long_delay_count);

        self.total_short_delay_duration
            .get_or_create(&labels)
            .observe(metrics.total_short_delay_duration.as_secs_f64());

        self.total_long_delay_duration
            .get_or_create(&labels)
            .observe(metrics.total_long_delay_duration.as_secs_f64());
    }
}

pub fn register_tokio_task_metrics(registry: &mut Registry) -> TokioTaskMetrics {
    TokioTaskMetrics::new(registry)
}
