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
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReplicatedBytesKey {}

pub type ReplicatedBytesMetric = Family<ReplicatedBytesKey, Counter>;

pub fn register_replicated_bytes_ops(registry: &mut Registry) -> ReplicatedBytesMetric {
    let bytes_count = ReplicatedBytesMetric::default();
    registry.register(
        "nucliadb_replication_replicated_bytes_processed",
        "Replicated bytes",
        bytes_count.clone(),
    );
    bytes_count
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReplicationOpsKey {
    pub operation: String,
}

pub type ReplicationOpsMetric = Family<ReplicationOpsKey, Counter>;

pub fn register_replication_operations(registry: &mut Registry) -> ReplicationOpsMetric {
    let operation_count = ReplicationOpsMetric::default();
    registry.register(
        "replication_operations",
        "Replication operations",
        operation_count.clone(),
    );
    operation_count
}
