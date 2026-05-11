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
