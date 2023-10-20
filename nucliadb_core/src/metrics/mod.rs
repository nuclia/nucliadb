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

mod meters;
mod metric;
mod task_monitor;

pub use metric::{grpc_ops, replication, request_time};

#[cfg(test)]
mod tests;

use std::sync::Arc;

use lazy_static::lazy_static;

use self::meters::Meter;

lazy_static! {
    static ref METRICS: Arc<dyn Meter> = create_metrics();
}

#[cfg(prometheus_metrics)]
fn create_metrics() -> Arc<dyn Meter> {
    Arc::new(meters::PrometheusMeter::new())
}

#[cfg(not(prometheus_metrics))]
fn create_metrics() -> Arc<dyn Meter> {
    Arc::new(meters::NoOpMeter)
}

pub fn get_metrics() -> Arc<dyn Meter> {
    Arc::clone(&METRICS)
}
