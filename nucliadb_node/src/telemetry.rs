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
use sentry::ClientInitGuard;
use tracing::{debug, error};
use tracing_subscriber::filter::{FilterFn, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use crate::config::Configuration;
use crate::result::{ServiceError, ServiceResult};

pub fn init_telemetry() -> ServiceResult<ClientInitGuard> {
    let agent_endpoint = Configuration::jaeger_agent_endp();
    debug!("{agent_endpoint}");
    let log_levels = Configuration::log_level();

    let mut layers = Vec::new();

    if Configuration::jaeger_enabled() {
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(agent_endpoint)
            .with_service_name("nucliadb_node")
            .with_auto_split_batch(true)
            .install_batch(opentelemetry::runtime::Tokio)
            .map_err(|e| ServiceError::GenericErr(Box::new(e)))?;

        // This filter is needed because we want to keep logs in stdout and attach logs to jaeger
        // spans in really rare cases So, basically it checks the source of event (allowed
        // only from nucliadb_node crate) and filter out all events without special field
        // For attaching log to jaeger span use this:
        // tracing::event!(Level::INFO, trace_marker = true, "your logs for jaeger here: {}", foo =
        // bar);
        let filter = FilterFn::new(|metadata| {
            metadata
                .file()
                .filter(|file| file.contains("nucliadb_node"))
                .map(|_| metadata.is_event())
                .map(|state| state && metadata.fields().field("trace_marker").is_none())
                .map(|state| !state)
                .unwrap_or_default()
        });
        global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());

        let jaeger_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(Targets::new().with_targets(log_levels.clone()))
            .with_filter(filter)
            .boxed();
        layers.push(jaeger_layer);
    }

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_filter(Targets::new().with_targets(log_levels))
        .boxed();

    layers.push(stdout_layer);

    let sentry_env = Configuration::get_sentry_env();
    let guard = sentry::init((
        Configuration::sentry_url(),
        sentry::ClientOptions {
            release: sentry::release_name!(),
            environment: Some(sentry_env.into()),
            ..Default::default()
        },
    ));
    layers.push(sentry_tracing::layer().boxed());

    tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .map_err(|e| {
            error!("Try init error: {e}");
            ServiceError::GenericErr(Box::new(e))
        })?;
    Ok(guard)
}
