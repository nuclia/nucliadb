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
