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
//

// #![warn(missing_docs)]

//! NucliaDB Index Node component
//!
//! This module provides the top level NucliaDB's indexing funcionality. It
//! allows indexing, searching and deleting indexed contents.
//!
//! As a high level interface, it provides a gRPC server to deploy the index in
//! a distributed fashion. The API allows building other interfaces, as the
//! already built PyO3 bindings.

pub mod analytics;
pub mod grpc;
pub mod http_server;
pub mod lifecycle;
pub mod node_metadata;
pub mod replication;
pub mod settings;
pub mod shards;
pub mod telemetry;
pub mod utils;

mod disk_structure;
