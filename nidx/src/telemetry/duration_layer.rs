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

use std::time::Instant;

use tracing::{
    Subscriber,
    field::Visit,
    span::{Attributes, Id},
};
use tracing_core::Field;
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use crate::metrics;

/// tracing_subscriber Layer that export span durations as prometheus metrics
pub struct DurationLayer;

struct Duration(Instant, String);

struct ExtractNameVisitor(Option<String>);

impl Visit for ExtractNameVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "otel.name" {
            self.0 = Some(value.to_string())
        }
    }

    fn record_debug(&mut self, _field: &tracing_core::Field, _value: &dyn std::fmt::Debug) {}
}

impl<S> Layer<S> for DurationLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).unwrap();

        let mut extractor = ExtractNameVisitor(None);
        attrs.record(&mut extractor);
        let name = extractor.0.unwrap_or(span.name().to_string());

        span.extensions_mut().insert(Duration(Instant::now(), name));
    }

    fn on_close(&self, id: tracing_core::span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).unwrap();
        let extensions = span.extensions();

        if let Some(Duration(t, name)) = extensions.get() {
            metrics::common::SPAN_DURATION
                .get_or_create(&vec![("span".to_string(), name.clone())])
                .observe(t.elapsed().as_secs_f64());
        }
    }
}
