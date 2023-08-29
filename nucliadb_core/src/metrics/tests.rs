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
    assert!(export.contains("nucliadb_node_total_polls_count 100"));
    assert!(export.contains("nucliadb_node_min_polls_count 100"));
    assert!(export.contains("nucliadb_node_max_polls_count 100"));
}

async fn flush_metrics() {
    let _ = tokio::task::yield_now().await;
}
