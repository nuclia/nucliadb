use prometheus_client::encoding;
use prometheus_client::registry::Registry;

use crate::metrics::meters::Meter;
use crate::metrics::metrics::tokio_tasks::TaskLabels;
use crate::metrics::metrics::{request_time, tokio_tasks};
use crate::metrics::task_monitor::{Monitor, MultiTaskMonitor, TaskId};
use crate::NodeResult;

pub struct PrometheusMeter {
    registry: Registry,
    request_time_metric: request_time::RequestTimeMetric,
    tokio_task_metrics: tokio_tasks::TokioTaskMetrics,
    tasks_monitor: MultiTaskMonitor,
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
            .into_iter()
            .for_each(|(task_id, metrics)| {
                let labels = TaskLabels { request: task_id };
                self.tokio_task_metrics.collect(labels, metrics.to_owned());
            });

        let mut buf = String::new();
        encoding::text::encode(&mut buf, &self.registry)?;
        println!("EXPORT: {buf}");
        Ok(buf)
    }

    fn record_request_time(
        &self,
        metric: request_time::RequestTimeKey,
        value: request_time::RequestTimeValue,
    ) {
        self.request_time_metric
            .get_or_create(&metric)
            .observe(value);
    }

    fn task_monitor(&self, task_id: TaskId) -> Option<Monitor> {
        Some(self.tasks_monitor.task_monitor(task_id))
    }
}

impl PrometheusMeter {
    pub fn new() -> PrometheusMeter {
        let mut registry = Registry::default();

        // This must be done for every metric
        let request_time_metric = request_time::register_request_time(&mut registry);
        let tokio_task_metrics = tokio_tasks::register_tokio_task_metrics(&mut registry);

        let tasks_monitor = MultiTaskMonitor::new();

        PrometheusMeter {
            registry,
            request_time_metric,
            tokio_task_metrics,
            tasks_monitor,
        }
    }
}
