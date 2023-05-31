use dashmap::DashMap;
use tokio_metrics::{Instrumented, TaskMetrics, TaskMonitor};

pub type TaskId = String;

pub struct MultiTaskMonitor {
    task_monitors: DashMap<TaskId, TaskMonitor>,
}

impl MultiTaskMonitor {
    pub fn new() -> Self {
        Self {
            task_monitors: DashMap::new(),
        }
    }

    pub fn task_monitor(&self, task_id: TaskId) -> Monitor {
        Monitor {
            task_id,
            monitors: &self.task_monitors,
        }
        // Monitor::new(task_id, &self.task_monitors)
    }

    pub fn export_all(&self) -> Vec<(TaskId, TaskMetrics)> {
        self.task_monitors
            .iter()
            .filter_map(|item| {
                let task_id = item.key().to_owned();
                let metrics = item.value().intervals().next();
                metrics.map(|metrics| (task_id, metrics))
            })
            .collect()
    }
}

pub struct Monitor<'a> {
    task_id: TaskId,
    monitors: &'a DashMap<TaskId, TaskMonitor>,
}

impl<'a> Monitor<'a> {
    // Consuming `self` ensures a short life for the reference inside a DashMap,
    // avoiding potential deadlocks.
    pub fn instrument<F>(self, task: F) -> Instrumented<F> {
        if !self.monitors.contains_key(&self.task_id) {
            let monitor = TaskMonitor::new();
            self.monitors.insert(self.task_id.clone(), monitor);
        }
        let monitor = self
            .monitors
            .get(&self.task_id)
            .expect("Task existed or just inserted");

        monitor.instrument(task)
    }
}
