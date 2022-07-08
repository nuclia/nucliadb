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

use opentelemetry::global;
use tracing::{debug, error};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use crate::config::Configuration;
use crate::result::{ServiceError, ServiceResult};

pub fn init_telemetry() -> ServiceResult<()> {
    let agent_endpoint = Configuration::jaeger_agent_endp();
    debug!("{agent_endpoint}");
    let _log_levels = Configuration::log_level();

    let mut layers = Vec::new();

    if Configuration::jaeger_enabled() {
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(agent_endpoint)
            .with_service_name("nucliadb_node")
            .with_auto_split_batch(true)
            .install_batch(opentelemetry::runtime::Tokio)
            .map_err(|e| ServiceError::GenericErr(Box::new(e)))?;

        // let filter = FilterFn::new(|metadata| {
        //    let target = metadata.target();
        //    match metadata.module_path() {
        //        Some(module_path) if module_path.contains("nucliadb_node") => {
        //            target.contains("nucliadb_node::writer")
        //                || target.contains("nucliadb_node::reader")
        //        }
        //        _ => false,
        //    }
        //});
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

        let jaeger_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter)
            .boxed();
        layers.push(jaeger_layer);
    }

    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let stdout_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_level(true)
        .with_filter(filter)
        //.with_filter(Targets::new().with_targets(log_levels))
        .boxed();

    layers.push(stdout_layer);

    tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .map_err(|e| {
            error!("Try init error: {e}");
            ServiceError::GenericErr(Box::new(e))
        })?;
    Ok(())
}
