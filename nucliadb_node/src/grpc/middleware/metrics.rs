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

use std::task::{Context, Poll};

use futures::future::BoxFuture;
use hyper::Body;
use nucliadb_core::metrics;
use tonic::body::BoxBody;
use tower::{Layer, Service};

#[derive(Debug, Clone, Default)]
pub struct GrpcTasksMetricsLayer;

impl<S> Layer<S> for GrpcTasksMetricsLayer {
    type Service = GrpcTasksMetrics<S>;

    fn layer(&self, service: S) -> Self::Service {
        GrpcTasksMetrics { inner: service }
    }
}

/// Dynamically instrument gRPC server endpoints to extract async tokio task
/// metrics
#[derive(Debug, Clone)]
pub struct GrpcTasksMetrics<S> {
    inner: S,
}

impl<S> Service<hyper::Request<Body>> for GrpcTasksMetrics<S>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        // We need to swap the clone and the original to avoid a not ready
        // service. See
        // https://docs.rs/tower/0.4.13/tower/trait.Service.html#be-careful-when-cloning-inner-services
        // for more details
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let task_id = req.uri().path().into();
            let call = inner.call(req);
            let response = match metrics::get_metrics().task_monitor(task_id) {
                Some(monitor) => {
                    let instrumented = monitor.instrument(call);
                    instrumented.await
                }
                None => call.await,
            };

            response
        })
    }
}
