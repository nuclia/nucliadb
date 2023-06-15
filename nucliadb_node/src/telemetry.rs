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
use tracing_subscriber::filter::{FilterFn, LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry};

use crate::env;

const TRACE_ID: &str = "trace-id";

pub fn init_telemetry() -> NodeResult<ClientInitGuard> {
    let mut layers = Vec::new();

    let log_levels = env::log_level();
    let stdout = stdout_layer(log_levels);
    layers.push(stdout);

    if env::jaeger_enabled() {
        let span_levels = env::span_levels();
        let jaeger = jaeger_layer(span_levels)?;
        layers.push(jaeger);
    }

    let sentry_guard = setup_sentry(env::get_sentry_env(), env::sentry_url());
    let sentry = sentry_layer();
    layers.push(sentry);

    tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .with_context(|| "trying to init tracing")?;

    Ok(sentry_guard)
}

fn stdout_layer(log_levels: Vec<(String, Level)>) -> Box<dyn Layer<Registry> + Send + Sync> {
    let format = tracing_subscriber::fmt::format().with_level(true).compact();

    tracing_subscriber::fmt::layer()
        .event_format(format)
        .with_filter(Targets::new().with_targets(log_levels))
        .boxed()
}

fn jaeger_layer(
    _span_levels: Vec<(String, Level)>,
) -> NodeResult<Box<dyn Layer<Registry> + Send + Sync>> {
    global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());

    let agent_endpoint = env::jaeger_agent_endp();
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(agent_endpoint)
        .with_service_name("nucliadb_node")
        .with_auto_split_batch(true)
        .install_batch(opentelemetry::runtime::Tokio)?;

    // To avoid sending too much information to Jaeger, we filter out all events
    // (as they are logged to stdout), spans from external instrumented crates
    // (like tantivy, hyper, tower, mio...) and spans below INFO level (default
    // span level).
    let level_filter = LevelFilter::from_level(Level::INFO);
    let span_filter = FilterFn::new(|metadata| {
        metadata.is_span()
            && metadata
                .file()
                .filter(|file| file.contains("nucliadb"))
                .is_some()
    });

    Ok(tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(level_filter)
        .with_filter(span_filter)
        .boxed())
}

fn setup_sentry(env: &'static str, sentry_url: String) -> ClientInitGuard {
    sentry::init((
        sentry_url,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            environment: Some(env.into()),
            ..Default::default()
        },
    ))
}

fn sentry_layer() -> Box<dyn Layer<Registry> + Send + Sync> {
    sentry_tracing::layer().boxed()
}

pub fn run_with_telemetry<F, R>(current: Span, f: F) -> R
where F: FnOnce() -> R {
    let tid = current.context().span().span_context().trace_id();
    sentry::with_scope(|scope| scope.set_tag(TRACE_ID, tid), || current.in_scope(f))
}
