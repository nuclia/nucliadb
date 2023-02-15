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

use nucliadb_core::protos::shutdown_handler_server::{ShutdownHandler, ShutdownHandlerServer};
use nucliadb_core::protos::*;
use nucliadb_core::tracing::*;
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status};

pub struct GrpcShutdown {
    shutdown: Sender<()>,
}

impl GrpcShutdown {
    pub fn new(shutdown: Sender<()>) -> ShutdownHandlerServer<GrpcShutdown> {
        ShutdownHandlerServer::new(GrpcShutdown { shutdown })
    }
}

#[tonic::async_trait]
impl ShutdownHandler for GrpcShutdown {
    async fn shutdown(&self, _: Request<EmptyQuery>) -> Result<Response<EmptyResponse>, Status> {
        info!("Sending a shutdown signal");
        let Ok(()) = self.shutdown.send(()).await else {
            unreachable!("The shutdown future was consumed before the server ends ")
        };
        info!("shutdown notified successfully");
        Ok(Response::new(EmptyResponse {}))
    }
}
