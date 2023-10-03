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
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GrpcOpKey {
    pub method: String,
}
pub type GrpcOpValue = f64;

pub type GrpcOpMetric = Family<GrpcOpKey, Histogram>;

const BUCKETS: [f64; 14] = [
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    2.5 * 60.0,
    5.0 * 60.0,
];

pub fn register_grpc_ops(registry: &mut Registry) -> GrpcOpMetric {
    let constructor = || Histogram::new(BUCKETS.iter().copied());
    let metric = GrpcOpMetric::new_with_constructor(constructor);
    registry.register(
        "grpc_server_op_duration_seconds",
        "gRPC server operations duration in seconds",
        metric.clone(),
    );
    metric
}
