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

use nucliadb_core::tracing::{Level, Span};
use nucliadb_core::{Context, NodeResult};
use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;
use sentry::ClientInitGuard;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{FilterFn, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry};

use crate::env;

const TRACE_ID: &str = "trace-id";

pub fn init_telemetry() -> NodeResult<ClientInitGuard> {
    let log_levels = env::log_level();

    let mut layers = Vec::new();

    if env::jaeger_enabled() {
        layers.push(init_jaeger(log_levels.clone())?);
    }

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_filter(Targets::new().with_targets(log_levels))
        .boxed();

    layers.push(stdout_layer);

    let sentry_env = env::get_sentry_env();
    let guard = sentry::init((
        env::sentry_url(),
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
        .with_context(|| "trying to init tracing")?;
    Ok(guard)
}

pub(crate) fn run_with_telemetry<F, R>(current: Span, f: F) -> R
where F: FnOnce() -> R {
    let tid = current.context().span().span_context().trace_id();
    sentry::with_scope(|scope| scope.set_tag(TRACE_ID, tid), || current.in_scope(f))
}

fn init_jaeger(
    log_levels: Vec<(String, Level)>,
) -> NodeResult<Box<dyn Layer<Registry> + Send + Sync>> {
    let agent_endpoint = env::jaeger_agent_endp();
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(agent_endpoint)
        .with_service_name("nucliadb_node")
        .with_auto_split_batch(true)
        .install_batch(opentelemetry::runtime::Tokio)?;

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

    Ok(tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(Targets::new().with_targets(log_levels))
        .with_filter(filter)
        .boxed())
}
