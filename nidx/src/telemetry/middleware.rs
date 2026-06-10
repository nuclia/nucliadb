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

use std::task::{Context, Poll};

use axum::{body::Body, extract::Request, http, response::Response};
use futures::future::BoxFuture;
use opentelemetry::propagation::{Extractor, Injector};
use tonic::metadata::MetadataKey;
use tower::{Layer, Service};
use tracing::*;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

#[derive(Debug, Clone, Default)]
pub struct GrpcInstrumentorLayer;

impl<S> Layer<S> for GrpcInstrumentorLayer {
    type Service = GrpcInstrumentor<S>;

    fn layer(&self, service: S) -> Self::Service {
        GrpcInstrumentor { inner: service }
    }
}

/// Dynamically instrument gRPC server endpoints which continue traces injected
/// by clients.
#[derive(Debug, Clone)]
pub struct GrpcInstrumentor<S> {
    inner: S,
}

impl<S> Service<Request<Body>> for GrpcInstrumentor<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        // We need to swap the clone and the original to avoid a not ready
        // service. See
        // https://docs.rs/tower/0.4.13/tower/trait.Service.html#be-careful-when-cloning-inner-services
        // for more details
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let name = req.uri().path();
        let (service, method) = match name.strip_prefix('/').and_then(|s| s.split_once('/')) {
            Some((service, method)) => (service, method),
            None => {
                warn!("gRPC server called with unexpected format: {name:?}");
                ("Unknown", name)
            }
        };

        let span = info_span!(
            target: "nidx",
            "nidx::grpc", // placeholder that will be substituted by otel.name
            otel.name = name,
            rpc.system = "grpc",
            rpc.service = service,
            rpc.method = method
        );
        let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderMapWrapper { inner: req.headers() })
        });

        let _ = span.set_parent(parent_context);

        let fut = inner.call(req).instrument(span);
        Box::pin(fut)
    }
}

struct HeaderMapWrapper<'a> {
    inner: &'a http::header::HeaderMap,
}

impl Extractor for HeaderMapWrapper<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.inner
            .get(key)
            .map(|value| value.to_str())
            .transpose()
            .unwrap_or(None)
    }

    fn keys(&self) -> Vec<&str> {
        self.inner.keys().map(|key| key.as_str()).collect()
    }
}

#[allow(clippy::result_large_err)]
pub fn add_telemetry_headers(mut req: tonic::Request<()>) -> tonic::Result<tonic::Request<()>> {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject(&mut MetadataMapWrapper {
            inner: req.metadata_mut(),
        })
    });

    Ok(req)
}

struct MetadataMapWrapper<'a> {
    inner: &'a mut tonic::metadata::MetadataMap,
}

impl Injector for MetadataMapWrapper<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.inner.insert(
            MetadataKey::from_bytes(key.to_lowercase().as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
}
