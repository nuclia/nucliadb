use std::net::SocketAddr;

use nucliadb_protos::node_writer_client::NodeWriterClient as GrpcClient;
use nucliadb_protos::{AcceptShardRequest, MoveShardRequest, ShardId};
use tonic::Request;

use crate::Error;

pub struct ShardCutover {
    pub(crate) shard_id: String,
    pub(crate) source_address: SocketAddr,
    pub(crate) destination_address: SocketAddr,
    pub(crate) port: u16,
}

impl ShardCutover {
    pub async fn execute(self) -> Result<(), Error> {
        let mut source_client =
            GrpcClient::connect(format!("grpc://{}", self.source_address)).await?;

        let mut destination_client =
            GrpcClient::connect(format!("grpc://{}", self.destination_address)).await?;

        tokio::try_join!(
            // TODO: Create shadow shard
            destination_client.accept_shard(Request::new(AcceptShardRequest {
                shard_id: Some(ShardId {
                    id: self.shard_id.clone(),
                }),
                port: self.port as u32,
                override_shard: true,
            })),
            source_client.move_shard(Request::new(MoveShardRequest {
                shard_id: Some(ShardId {
                    id: self.shard_id.clone(),
                }),
                address: format!("{}:{}", self.destination_address.ip(), self.port),
            })),
        )?;

        Ok(())
    }
}
