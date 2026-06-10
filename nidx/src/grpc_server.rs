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

use axum::serve::ListenerExt;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[cfg(feature = "telemetry")]
use crate::telemetry;

/// A tonic server that allows binding to and returning a random port.
pub struct GrpcServer(TcpListener);

impl GrpcServer {
    pub async fn new(address: impl ToSocketAddrs) -> anyhow::Result<Self> {
        Ok(GrpcServer(TcpListener::bind(address).await?))
    }

    pub fn port(&self) -> anyhow::Result<u16> {
        Ok(self.0.local_addr()?.port())
    }

    pub async fn serve(self, router: axum::Router, shutdown: CancellationToken) -> anyhow::Result<()> {
        #[cfg(feature = "telemetry")]
        let router = router.layer(telemetry::middleware::GrpcInstrumentorLayer);

        // Try to set TCP_NODELAY on all TCP connections.
        //
        // This is necessary for gRPC performance as istio's envoy proxy sometimes misbehaves with
        // gRPC calls. The combination of TCP delayed ACK and Nagle's algorithm result in envoy
        // proxy waiting Linux's TCP_DELACK_MIN (default to 40ms) before sending the last packets
        // and finishing the gRPC call.
        //
        // As many of nidx's gRPC calls take only ~1-10ms, this extra 40ms delay per nidx-searcher
        // is unacceptable. Thus, we prefer disabling the Nagle algorithm
        //
        let listener = self.0.tap_io(|tcp_stream| {
            if let Err(err) = tcp_stream.set_nodelay(true) {
                warn!("unable to set TCP_NODELAY on connection: {err:?}");
            }
        });

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await?;
        Ok(())
    }
}
