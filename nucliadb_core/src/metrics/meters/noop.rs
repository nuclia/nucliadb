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

use crate::metrics::meters::Meter;
use crate::metrics::metric::grpc_ops::{GrpcOpKey, GrpcOpValue};
use crate::metrics::metric::replication;
use crate::metrics::metric::request_time::{RequestTimeKey, RequestTimeValue};
use crate::NodeResult;

pub struct NoOpMeter;
impl Meter for NoOpMeter {
    fn record_request_time(&self, _metric: RequestTimeKey, _value: RequestTimeValue) {}

    fn record_grpc_op(&self, _method: GrpcOpKey, _value: GrpcOpValue) {}

    fn export(&self) -> NodeResult<String> {
        Ok(Default::default())
    }
    fn record_replicated_bytes(&self, _value: u64) {}
    fn record_replication_op(&self, _key: replication::ReplicationOpsKey) {}
}
