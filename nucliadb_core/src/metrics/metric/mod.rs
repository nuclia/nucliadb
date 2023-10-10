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

//! Every metric must be define in its own module, which must fulfill the following requirements:
//! - The name of the module must be the name of the name of the metric.
//! - If the metric is called SomeName, then there must be a type 'SomeNameMetric' describing such
//!   metric.
//! - If the metric is called SomeName, a struct 'SomeNameKey' must be defined.
//! - If the metric is called SomeName, a struct 'SomeNameValue' must be defined.

pub mod grpc_ops;
pub mod replication;
pub mod request_time;
pub mod tokio_runtime;
pub mod tokio_tasks;
