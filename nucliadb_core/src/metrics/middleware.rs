use std::task::{Context, Poll};

use futures::future::BoxFuture;
use hyper::Body;
use tonic::body::BoxBody;
use tower::{Layer, Service};

use crate::metrics;

#[derive(Debug, Clone, Default)]
pub struct MetricsLayer;

impl<S> Layer<S> for MetricsLayer {
    type Service = EndpointMetrics<S>;

    fn layer(&self, service: S) -> Self::Service {
        EndpointMetrics { inner: service }
    }
}

/// Dynamically instrument service calls by HTTP path / gRPC method.
#[derive(Debug, Clone)]
pub struct EndpointMetrics<S> {
    inner: S,
}

impl<S> Service<hyper::Request<Body>> for EndpointMetrics<S>
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
