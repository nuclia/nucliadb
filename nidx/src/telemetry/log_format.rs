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

// Copyright (c) 2019 Tokio Contributors
//
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use opentelemetry::trace::TraceContextExt as _;
use serde::ser::{SerializeMap, Serializer as _};
use serde_json::Serializer;
use std::fmt;
use tracing::Subscriber;
use tracing_core::Event;
use tracing_serde::AsSerde;
use tracing_subscriber::{
    fmt::{
        FmtContext, FormatEvent, FormatFields,
        format::Writer,
        time::{FormatTime, SystemTime},
    },
    registry::LookupSpan,
};

/// A custom log formatter that logs to JSON and adds a OTEL `trace_id` to the log lines.
/// Copied from tracing_subscriber's JSON log implementations but keeping only what we need,
/// and adding the `trace_id` extraction.
pub struct StructuredFormat;

impl<S, N> FormatEvent<S, N> for StructuredFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let mut timestamp = String::new();
        SystemTime.format_time(&mut Writer::new(&mut timestamp))?;

        let meta = event.metadata();

        let mut visit = || {
            let mut serializer = Serializer::new(WriteAdaptor::new(&mut writer));
            let mut serializer = serializer.serialize_map(None)?;

            // Common fields
            serializer.serialize_entry("timestamp", &timestamp)?;
            serializer.serialize_entry("level", &meta.level().as_serde())?;
            serializer.serialize_entry("target", &meta.target())?;

            // Event fields, flattened to the top-level
            let mut visitor = tracing_serde::SerdeMapVisitor::new(serializer);
            event.record(&mut visitor);
            serializer = visitor.take_serializer()?;

            // Extract and serialize `trace_id`
            let span = event
                .parent()
                .and_then(|id| ctx.span(id))
                .or_else(|| ctx.lookup_current());
            if let Some(span) = span {
                let span_id = span.id();
                let trace_id = tracing::dispatcher::get_default(|dispatch| {
                    tracing_opentelemetry::get_otel_context(&span_id, dispatch).and_then(|cx| {
                        let trace_id = cx.span().span_context().trace_id();
                        if trace_id != opentelemetry::trace::TraceId::INVALID {
                            Some(trace_id)
                        } else {
                            None
                        }
                    })
                });
                if let Some(trace_id) = trace_id {
                    serializer.serialize_entry("trace_id", &trace_id.to_string())?;
                }
            }

            serializer.end()
        };

        visit().map_err(|_| fmt::Error)?;
        writeln!(writer)
    }
}

pub struct WriteAdaptor<'a> {
    fmt_write: &'a mut dyn fmt::Write,
}

impl<'a> WriteAdaptor<'a> {
    pub fn new(fmt_write: &'a mut dyn fmt::Write) -> Self {
        Self { fmt_write }
    }
}

impl std::io::Write for WriteAdaptor<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s = std::str::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        self.fmt_write.write_str(s).map_err(std::io::Error::other)?;

        Ok(s.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
