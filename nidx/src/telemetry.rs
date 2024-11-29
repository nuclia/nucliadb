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

mod log_format;
pub mod middleware;

use std::time::Duration;

use crate::settings::{LogFormat, TelemetrySettings};
use log_format::StructuredFormat;
use opentelemetry::global::get_text_map_propagator;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::TracerProvider, Resource};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::{filter::FilterFn, fmt, prelude::*, EnvFilter};

pub fn init(settings: &TelemetrySettings) -> anyhow::Result<()> {
    // Configure logging format
    let env_filter = EnvFilter::from_default_env();
    let log_layer = match settings.log_format {
        LogFormat::Pretty => fmt::layer().with_filter(env_filter).boxed(),
        LogFormat::Structured => fmt::layer().json().event_format(StructuredFormat).with_filter(env_filter).boxed(),
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

        let span_filter = FilterFn::new(|meta| meta.module_path().is_some_and(|module| module.starts_with("nidx")));
        Some(
            tracing_opentelemetry::layer()
                .with_tracer(tracer_otlp)
                .with_filter(EnvFilter::from_default_env())
                .with_filter(span_filter),
        )
    } else {
        None
    };

    // Initialize all telemetry layers
    tracing_subscriber::registry().with(log_layer).with(sentry_tracing::layer()).with(otel_layer).init();

    Ok(())
}

struct NatsHeaders<'a>(&'a async_nats::HeaderMap);

impl<'a> Extractor for NatsHeaders<'a> {
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
}
