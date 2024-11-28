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

use log_format::StructuredFormat;
use nidx::settings::TelemetrySettings;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::TracerProvider;
use tracing_subscriber::prelude::*;

pub fn init(settings: &TelemetrySettings) {
    // Configure logging format
    let log_layer = match settings.log_format {
        nidx::settings::LogFormat::Pretty => tracing_subscriber::fmt::layer().boxed(),
        nidx::settings::LogFormat::Structured => {
            tracing_subscriber::fmt::layer().json().event_format(StructuredFormat).boxed()
        }
    };

    // Traces go to OpenTelemetry
    let provider =
        TracerProvider::builder().with_simple_exporter(opentelemetry_stdout::SpanExporter::default()).build();
    let tracer = provider.tracer("readme_example");
    // Create a tracing layer with the configured tracer
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry().with(log_layer).with(sentry_tracing::layer()).with(otel_layer).init();
}
