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

mod errors;
mod reader;
mod writer;

use std::env;

use nucliadb_core::tracing::{error, Level};
use nucliadb_node::utils::parse_log_levels;
use pyo3::prelude::*;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

type RawProtos = Vec<u8>;

#[pymodule]
pub fn nucliadb_node_binding(py: Python, m: &PyModule) -> PyResult<()> {
    let log_levels =
        parse_log_levels(&env::var("RUST_LOG").unwrap_or("nucliadb_node=WARN".to_string()));

    setup_tracing(log_levels);

    m.add_class::<reader::NodeReader>()?;
    m.add_class::<writer::NodeWriter>()?;

    m.add(
        "IndexNodeException",
        py.get_type::<errors::IndexNodeException>(),
    )?;
    m.add("LoadShardError", py.get_type::<errors::LoadShardError>())?;
    m.add("ShardNotFound", py.get_type::<errors::ShardNotFound>())?;

    Ok(())
}

fn setup_tracing(log_levels: Vec<(String, Level)>) {
    let mut layers = Vec::new();

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_filter(Targets::new().with_targets(log_levels))
        .boxed();

    layers.push(stdout_layer);

    // An error can occur when another global registry has been set up. In that
    // case, we'll use it instead of ours
    let _ = tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .map_err(|error| error!("Error initializing logs and traces: {error}"));
}
