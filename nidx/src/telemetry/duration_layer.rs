// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
