use dashmap::DashMap;
use tokio_metrics::{TaskMetrics, TaskMonitor};
use tokio_metrics::Instrumented;
use lazy_static::lazy_static;

pub type TaskId = String;

lazy_static! {
    static ref MONITOR: MultiTaskMonitor = MultiTaskMonitor::new();
}

struct MultiTaskMonitor {
    task_monitors: DashMap<TaskId, TaskMonitor>,
}

impl MultiTaskMonitor {
    pub fn new() -> Self {
        Self {
            task_monitors: DashMap::new(),
        }
    }

    pub fn instrument<F>(&self, task_id: TaskId, task: F) -> Instrumented<F> {
        if !self.task_monitors.contains_key(&task_id) {
            let monitor = TaskMonitor::new();
            self.task_monitors.insert(task_id.clone(), monitor);
        }
        let monitor = self.task_monitors
                          .get(&task_id)
                          .expect("Task existed or just inserted");
        monitor.instrument(task)
    }
}

pub fn instrument<F>(task_id: TaskId, task: F) -> Instrumented<F> {
    MONITOR.instrument(task_id, task)
}
