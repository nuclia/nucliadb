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

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReplicatedBytesKey {}
pub type ReplicatedBytesValue = f64;

pub type ReplicatedBytesMetric = Family<ReplicatedBytesKey, Histogram>;

const BUCKETS: [f64; 8] = [
    0.0,
    1024.0,
    1024.0 * 1024.0,
    1024.0 * 1024.0 * 5.0,
    1024.0 * 1024.0 * 10.0,
    1024.0 * 1024.0 * 100.0,
    1024.0 * 1024.0 * 512.0,
    1024.0 * 1024.0 * 1024.0,
];

pub fn register_replicated_bytes_ops(registry: &mut Registry) -> ReplicatedBytesMetric {
    let constructor = || Histogram::new(BUCKETS.iter().copied());
    let metric = ReplicatedBytesMetric::new_with_constructor(constructor);
    registry.register(
        "nucliadb_replication_replicated_bytes",
        "Replicated bytes per second",
        metric.clone(),
    );
    metric
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ShardOpsKey {
    pub operation: String,
}

pub type ShardOpsMetric = Family<ShardOpsKey, Counter>;

pub fn register_replication_operations(registry: &mut Registry) -> ShardOpsMetric {
    let operation_count = ShardOpsMetric::default();
    registry.register(
        "total_park_count",
        "The number of times worker threads parked. [...]",
        operation_count.clone(),
    );
    operation_count
}
