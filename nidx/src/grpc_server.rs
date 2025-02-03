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

use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_util::sync::CancellationToken;

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

        axum::serve(self.0, router.into_make_service())
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await?;
        Ok(())
    }
}
