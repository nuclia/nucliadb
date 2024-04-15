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

use crate::telemetry::run_with_telemetry;
use nucliadb_core::protos::*;
use nucliadb_core::relations::{RelationConfig, RelationsWriter};
use nucliadb_core::write_rw_lock;
use nucliadb_core::{node_error, NodeResult};
use nucliadb_procs::measure;
use nucliadb_relations::service::RelationsWriterService as RelationsWriterServiceV1;
use nucliadb_relations2::writer::RelationsWriterService as RelationsWriterServiceV2;
use std::sync::RwLock;

pub type RelationWPointer = RwLock<Box<dyn RelationsWriter>>;

pub fn new(version: u32, config: &RelationConfig) -> NodeResult<RelationWPointer> {
    match version {
        1 => RelationsWriterServiceV1::start(config).map(|i| RwLock::new(Box::new(i) as Box<dyn RelationsWriter>)),
        2 => RelationsWriterServiceV2::start(config).map(|i| RwLock::new(Box::new(i) as Box<dyn RelationsWriter>)),
        v => Err(node_error!("Invalid relations version {v}")),
    }
}
