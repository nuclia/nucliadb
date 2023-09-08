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

use tokio;

use crate::metrics::meters::{Meter, PrometheusMeter};

#[tokio::test(flavor = "current_thread")]
async fn test_export_metric_name() {
    let meter = PrometheusMeter::new();
    let export = meter.export().unwrap();
    assert!(export.contains("\n# TYPE nucliadb_node_workers_count gauge\n"));
    assert!(export.contains("\n# TYPE nucliadb_node_instrumented_count counter\n"))
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_tasks_instrumented_count() {
    let meter = PrometheusMeter::new();
    let task_id = "my-task".to_string();

    meter
        .task_monitor(task_id.clone())
        .unwrap()
        .instrument(async {})
        .await;

    let export = meter.export().unwrap();
    assert!(export.contains("nucliadb_node_instrumented_count_total{request=\"my-task\"} 1"));

    meter
        .task_monitor(task_id.clone())
        .unwrap()
        .instrument(async {})
        .await;
    meter
        .task_monitor(task_id)
        .unwrap()
        .instrument(async {})
        .await;

    let export = meter.export().unwrap();
    assert!(export.contains("nucliadb_node_instrumented_count_total{request=\"my-task\"} 3"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_export_runtime_metrics_with_prometheus_meter_workers_count() {
    let meter = PrometheusMeter::new();
    let export = meter.export().unwrap();
    assert!(export.contains("nucliadb_node_workers_count 2"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_runtime_metrics_with_prometheus_meter_polls_count() {
    let meter = PrometheusMeter::new();

    // do some async work
    const N: usize = 100;
    for _ in 0..N {
        let _ = tokio::spawn(async {}).await;
    }

    flush_metrics().await;

    let export = meter.export().unwrap();

    assert!(export.contains("nucliadb_node_workers_count 1"));
    assert!(export.contains("nucliadb_node_total_polls_count_total 100"));
    assert!(export.contains("nucliadb_node_min_polls_count 100"));
    assert!(export.contains("nucliadb_node_max_polls_count 100"));

    // do some more async work
    for _ in 0..N {
        let _ = tokio::spawn(async {}).await;
    }

    flush_metrics().await;

    let export = meter.export().unwrap();

    assert!(export.contains("nucliadb_node_workers_count 1"));
    // total is a global couter while min and max are gauges updated per
    // sampling interval
    assert!(export.contains("nucliadb_node_total_polls_count_total 200"));
    assert!(export.contains("nucliadb_node_min_polls_count 100"));
    assert!(export.contains("nucliadb_node_max_polls_count 100"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_export_runtime_metrics_blocking_threads_count() {
    let meter = PrometheusMeter::new();

    let export = meter.export().unwrap();
    assert!(export.contains("nucliadb_node_blocking_threads_count 0"));

    tokio::task::spawn_blocking(|| std::thread::sleep(std::time::Duration::from_millis(10)));

    let export = meter.export().unwrap();
    assert!(export.contains("nucliadb_node_blocking_threads_count 1"));
}

async fn flush_metrics() {
    let _ = tokio::task::yield_now().await;
}
