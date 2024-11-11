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

use http::Uri;
use tokio::net::{TcpListener, ToSocketAddrs};
use tonic::{
    service::Routes,
    transport::{server::TcpIncoming, Server},
};
use tower::util::MapRequestLayer;

pub struct RemappedGrpcService {
    pub routes: Routes,
    pub package: String,
}

/// A tonic server that allows binding to and returning a random port.
pub struct GrpcServer(TcpListener);

impl GrpcServer {
    pub async fn new(address: impl ToSocketAddrs) -> anyhow::Result<Self> {
        Ok(GrpcServer(TcpListener::bind(address).await?))
    }

    pub fn port(&self) -> anyhow::Result<u16> {
        Ok(self.0.local_addr()?.port())
    }

    pub async fn serve(self, service: RemappedGrpcService) -> anyhow::Result<()> {
        let server = Server::builder()
            .layer(MapRequestLayer::new(move |req| map_grpc_path_to(&service.package, req)))
            .add_routes(service.routes);
        Ok(server.serve_with_incoming(TcpIncoming::from_listener(self.0, true, None).unwrap()).await?)
    }
}

// TODO: Remove once we don't need backwards API compatibility
/// Sets the request path for Grpc services. This is useful to be able to serve the same
/// service with different names. e.g: We expose the same API for NodeWriter and NidxApi
fn map_grpc_path_to(to: &str, mut req: http::Request<tonic::body::BoxBody>) -> http::Request<tonic::body::BoxBody> {
    let mut parts = req.uri().clone().into_parts();
    let mut new_path = None;
    // Finds the first part of the URI, which in grpc, it's the service
    if let Some(path_and_query) = parts.path_and_query {
        let path = path_and_query.path();
        let mut parts = path[1..].split('/');
        if let Some(service) = parts.next() {
            new_path = Some(path.replace(service, to));
        }
    }
    if let Some(new_path) = new_path {
        parts.path_and_query = Some(new_path.try_into().unwrap());
        *req.uri_mut() = Uri::from_parts(parts).unwrap();
    }

    req
}
