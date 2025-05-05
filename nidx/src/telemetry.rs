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

mod duration_layer;
mod log_format;
pub mod middleware;

use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::settings::{LogFormat, TelemetrySettings};
use arc_swap::ArcSwap;
use duration_layer::DurationLayer;
use lazy_static::lazy_static;
use log_format::StructuredFormat;
use opentelemetry::global::get_text_map_propagator;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, trace::TracerProvider};
use tracing::{Level, Metadata};
use tracing_core::LevelFilter;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::Filter;
use tracing_subscriber::{EnvFilter, filter::FilterFn, fmt, prelude::*};

struct SwappableLogLevelFilter(ArcSwap<EnvFilter>);

impl<S> Filter<S> for &SwappableLogLevelFilter {
    fn enabled(&self, meta: &tracing::Metadata<'_>, cx: &tracing_subscriber::layer::Context<'_, S>) -> bool {
        self.0.load().enabled(meta, cx.clone())
    }
}

lazy_static! {
    static ref ENV_FILTER: SwappableLogLevelFilter =
        SwappableLogLevelFilter(ArcSwap::from_pointee(EnvFilter::from_default_env()));
}

pub fn set_log_level(log_level: &str) -> anyhow::Result<()> {
    let env_filter = EnvFilter::from_str(log_level)?;
    ENV_FILTER.0.store(Arc::new(env_filter));

    Ok(())
}

fn nidx_filter(meta: &Metadata) -> bool {
    meta.module_path().is_some_and(|module| module.starts_with("nidx"))
}

pub fn init(settings: &TelemetrySettings) -> anyhow::Result<()> {
    // Configure logging format
    let log_layer = match settings.log_format {
        LogFormat::Pretty => fmt::layer().with_filter(ENV_FILTER.deref()).boxed(),
        LogFormat::Structured => fmt::layer()
            .json()
            .event_format(StructuredFormat)
            .with_filter(ENV_FILTER.deref())
            .boxed(),
    };

    // Trace propagation is done in Zipkin format (b3-* headers)
    opentelemetry::global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());

    // Traces go to the collector in OTLP format
    let otel_layer = if let Some(otlp_collector_url) = &settings.otlp_collector_url {
        let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(otlp_collector_url)
            .with_timeout(Duration::from_secs(2))
            .build()?;
        let provider = TracerProvider::builder()
            .with_batch_exporter(otlp_exporter, opentelemetry_sdk::runtime::Tokio)
            .with_resource(Resource::new(vec![KeyValue::new("service.name", "nidx")]))
            .build();
        let tracer_otlp = provider.tracer("nidx");
        Some(
            tracing_opentelemetry::layer()
                .with_tracer(tracer_otlp)
                .with_filter(EnvFilter::from_default_env())
                .with_filter(FilterFn::new(nidx_filter)),
        )
    } else {
        None
    };

    let metrics_layer = DurationLayer
        .with_filter(LevelFilter::from_level(Level::INFO))
        .with_filter(FilterFn::new(nidx_filter));

    // Initialize all telemetry layers
    tracing_subscriber::registry()
        .with(log_layer)
        .with(sentry_tracing::layer())
        .with(metrics_layer)
        .with(otel_layer)
        .init();

    Ok(())
}

struct NatsHeaders<'a>(&'a async_nats::HeaderMap);

impl Extractor for NatsHeaders<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| v.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        // Not implemented since it's not needed by zipkin propagator
        Vec::new()
    }
}

pub fn set_trace_from_nats(span: &tracing::Span, headers: async_nats::HeaderMap) {
    let parent_context = get_text_map_propagator(|p| p.extract(&NatsHeaders(&headers)));
    span.add_link(parent_context.span().span_context().clone());

    // Create a link back from the original trace to us
    let link_span = tracing::span!(Level::INFO, "Link to nidx trace");
    link_span.set_parent(parent_context);
    link_span.add_link(span.context().span().span_context().clone());
}
