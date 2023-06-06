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
    }

    pub fn export_all(&self) -> impl Iterator<Item = (TaskId, TaskMetrics)> + '_ {
        self.task_monitors.iter().filter_map(|item| {
            let task_id = item.key().to_owned();
            let metrics = item.value().intervals().next();
            metrics.map(|metrics| (task_id, metrics))
        })
    }
}

pub struct Monitor<'a> {
    task_id: TaskId,
    monitors: &'a DashMap<TaskId, TaskMonitor>,
}

impl<'a> Monitor<'a> {
    // Consuming `self` ensures a short life for the reference inside a DashMap,
    // avoiding potential deadlocks. See DashMap docs for more info.
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
