use std::net::SocketAddr;

use itertools::Itertools;
use nucliadb_protos::node_writer_client::NodeWriterClient as GrpcClient;
use nucliadb_protos::{AcceptShardRequest, MoveShardRequest, ShardId};
use reqwest::Client as HttpClient;
use serde::Deserialize;
use tonic::Request;

use crate::index::{Indexable, ReversedIndex};
use crate::node::Node;
use crate::views::KnowledgeBox as KnowledgeBoxView;
use crate::Error;

/// A reversed index storing information about logic shard (and so knowledge box and shard
/// replicas).
pub type ShardIndex = ReversedIndex<LogicShard>;

impl ShardIndex {
    /// Get all logic shards from Nuclia's API for the given list of nodes.
    ///
    /// # Errors
    /// This associated function will return an error if:
    /// - The Nuclia's API is not reachable
    /// - The JSON response is malformed
    pub async fn from_api(url: &str, nodes: &[Node]) -> Result<Self, Error> {
        let mut data = Vec::default();
        let http_client = HttpClient::new();

        for kb_id in nodes
            .iter()
            .flat_map(Node::shards)
            .map(Shard::kb_id)
            .unique()
        {
            let url = format!("http://search.{url}/api/v1/kb/{kb_id}/shards");

            let kb = http_client
                .get(url)
                .header("X-NUCLIADB-ROLES", "MANAGER")
                .send()
                .await?
                .json::<KnowledgeBoxView>()
                .await?;

            data.append(
                &mut kb
                    .shards
                    .into_iter()
                    .map(|shard| LogicShard {
                        id: shard.id,
                        replicas: shard
                            .replicas
                            .into_iter()
                            .map(|replica| replica.shard.id)
                            .collect::<Vec<_>>(),
                    })
                    .collect(),
            );
        }

        Ok(Self::new(data))
    }
}

/// The internal logic shard representation.
#[derive(Debug)]
pub struct LogicShard {
    #[allow(dead_code)]
    /// The shard identifier.
    id: String,
    /// The shard replicas.
    replicas: Vec<String>,
}

impl Indexable for LogicShard {
    type Key = String;

    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &Self::Key> + 'a> {
        Box::new(self.replicas.iter())
    }
}

/// The internal shard representation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Shard {
    /// The shard identifier.
    id: String,
    /// The shard load score.
    load_score: u64,
    /// The knowledge box the shard belongs to.
    kb_id: String,
}

impl Shard {
    /// Creates a new shard.
    pub fn new(id: String, load_score: u64, kb_id: String) -> Self {
        Self {
            id,
            load_score,
            kb_id,
        }
    }

    /// Returns the knowledge box the shard belongs to.
    #[inline]
    pub fn kb_id(&self) -> &str {
        self.kb_id.as_str()
    }

    /// Returns the shard identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Indicates if the shard is active or not.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.load_score > 0
    }

    /// Returns the shard load score.
    #[inline]
    pub fn load_score(&self) -> u64 {
        self.load_score
    }
}

/// The shard cutover representation
///
/// Note that to actually perform the shard cutover, [`ShardCutover::execute`] must be called.
#[derive(Debug, PartialEq, Eq)]
pub struct ShardCutover {
    /// The shard identifier.
    id: String,
    /// The gRPC address of the source node of the shard.
    source_address: SocketAddr,
    /// The gRPC address of the destination node of the shard.
    destination_address: SocketAddr,
    /// The TCP/IP port to use for transferring the shard.
    port: u16,
}

impl ShardCutover {
    /// Creates a new shard cutover
    pub fn new(
        id: String,
        source_address: SocketAddr,
        destination_address: SocketAddr,
        port: u16,
    ) -> Self {
        Self {
            id,
            source_address,
            destination_address,
            port,
        }
    }
    /// Perform the shard cutover.
    ///
    /// # Errors
    /// This method may fails if:
    /// - The source/destination nodes are not reachable.
    /// - The shard transfer fails somehow.
    /// - The creation/deletion of the shadow shard fails.
    pub async fn execute(self) -> Result<(), Error> {
        let mut source_client =
            GrpcClient::connect(format!("http://{}", self.source_address)).await?;

        let mut destination_client =
            GrpcClient::connect(format!("http://{}", self.destination_address)).await?;

        // TODO
        // destination_client.create_shadow_shard().await?;

        tokio::try_join!(
            destination_client.accept_shard(Request::new(AcceptShardRequest {
                shard_id: Some(ShardId {
                    id: self.id.clone(),
                }),
                port: self.port.into(),
                override_shard: true,
            })),
            source_client.move_shard(Request::new(MoveShardRequest {
                shard_id: Some(ShardId {
                    id: self.id.clone()
                }),
                address: format!("{}:{}", self.destination_address.ip(), self.port),
            })),
        )?;

        // TODO
        // destination_client.delete_shadow_shard().await?;
        // source_client_delete_shard(Request::new(ShardId { id: self.id.clone() })).await?

        Ok(())
    }
}
